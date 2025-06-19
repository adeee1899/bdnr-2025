#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script usado para benchmark Redis con sistema de cola de tickets de soporte al cliente.
Se llama desde run_tests.sh y utiliza LIST y HASH de Redis.
"""
import redis
import pandas as pd
import time
import os
import sys
import csv
from datetime import datetime

def cargar_tickets(path_csv, max_tickets):
    """Carga el CSV en memoria hasta max_tickets registros"""
    df = pd.read_csv(path_csv)
    total = len(df)
    print(f"Filas totales en CSV: {total}")
    df = df.fillna("")
    tickets = df.to_dict(orient="records")
    if max_tickets < total:
        tickets = tickets[:max_tickets]
    print(f"Tickets a usar: {len(tickets)}")
    return tickets


def ejecutar_operaciones(r, tickets):
    """
    Mide latencias de inserción (LPUSH+HSET) y consumo (RPOP+HGETALL).
    Devuelve p50_insert, p99_insert, thr_insert, p50_consume, p99_consume, thr_consume
    """
    insert_lat = []
    consume_lat = []
    n = len(tickets)

    # Inserción de tickets
    print("=== Fase de inserción ===")
    t0 = time.time()
    for t in tickets:
        start = time.time()
        # HSET detalles
        key = f"ticket:{t['Ticket ID']}"
        r.hset(key, mapping={
            "customer_name": t.get("Customer Name", ""),
            "customer_email": t.get("Customer Email", ""),
            "ticket_subject": t.get("Ticket Subject", ""),
            "ticket_priority": t.get("Ticket Priority", ""),
            "ticket_status": t.get("Ticket Status", ""),
            # agregar otros campos si se desea
        })
        # LPUSH a la cola
        r.lpush("tickets_queue", t['Ticket ID'])
        insert_lat.append(time.time() - start)
    dt_ins = time.time() - t0
    ops_ins = n * 2  # HSET + LPUSH
    thr_ins = ops_ins / dt_ins if dt_ins>0 else 0
    ms_ins = sorted([l*1000 for l in insert_lat])
    p50_ins = ms_ins[len(ms_ins)//2] if ms_ins else 0
    p99_ins = ms_ins[int(len(ms_ins)*0.99)] if len(ms_ins)>1 else p50_ins

    # Consumo de tickets
    print("=== Fase de consumo ===")
    t0 = time.time()
    while True:
        start = time.time()
        tid = r.rpop("tickets_queue")
        if tid is None:
            break
        _ = r.hgetall(f"ticket:{tid}")
        consume_lat.append(time.time() - start)
    dt_cons = time.time() - t0
    ops_cons = len(consume_lat) * 2  # RPOP + HGETALL
    thr_cons = ops_cons / dt_cons if dt_cons>0 else 0
    ms_cons = sorted([l*1000 for l in consume_lat])
    p50_cons = ms_cons[len(ms_cons)//2] if ms_cons else 0
    p99_cons = ms_cons[int(len(ms_cons)*0.99)] if len(ms_cons)>1 else p50_cons

    return p50_ins, p99_ins, thr_ins, p50_cons, p99_cons, thr_cons


def esperar_bgsave(r, timeout=60.0, interval=0.5):
    start = time.time()
    while True:
        info = r.info("persistence")
        if info.get("rdb_bgsave_in_progress") == 0:
            break
        if time.time() - start > timeout:
            print("WARNING: timeout esperando que termine BGSAVE previo")
            return False
        time.sleep(interval)
    r.bgsave()
    while True:
        info = r.info("persistence")
        if info.get("rdb_bgsave_in_progress") == 0:
            break
        if time.time() - start > timeout:
            print("WARNING: timeout esperando BGSAVE")
            return False
        time.sleep(interval)
    return True


def medir_metricas(r, redis_dir, modo):
    # Persistencia
    if modo.startswith("rdb") or modo == "mixto":
        esperar_bgsave(r)
    if modo.startswith("aof") or modo == "mixto":
        start = time.time()
        while r.info("persistence").get("aof_rewrite_in_progress", 0) == 1:
            time.sleep(0.1)
            if time.time() - start > 30:
                print("WARNING: timeout esperando fin de AOF rewrite previo")
                break
        r.bgrewriteaof()
        start = time.time()
        while r.info("persistence").get("aof_rewrite_in_progress", 0) == 1:
            time.sleep(0.1)
            if time.time() - start > 30:
                print("WARNING: timeout esperando nuevo AOF rewrite")
                break

    mem = r.info("memory")
    stats = r.info("stats")
    memory_used  = mem["used_memory"]
    evicted_keys = stats.get("evicted_keys", 0)

    rdb_path = os.path.join(redis_dir, "dump.rdb")
    rdb_size = os.path.getsize(rdb_path) if os.path.exists(rdb_path) else 0

    aof_size = 0
    main_aof = os.path.join(redis_dir, "appendonly.aof")
    if os.path.exists(main_aof):
        aof_size += os.path.getsize(main_aof)
    append_dir = os.path.join(redis_dir, "appendonlydir")
    if os.path.isdir(append_dir):
        for fname in os.listdir(append_dir):
            if fname.startswith("appendonly.aof"):
                aof_size += os.path.getsize(os.path.join(append_dir, fname))

    return {"memory_used": memory_used,
            "evicted_keys": evicted_keys,
            "rdb_size": rdb_size,
            "aof_size": aof_size}


def guardar_csv(fname, datos, modo, politica, dataset,
                p50_ins, p99_ins, thr_ins, p50_con, p99_con, thr_con):
    header = [
        "fecha","modo","politica","dataset",
        "lat50_ins_ms","lat99_ins_ms","thr_ins_ops_s",
        "lat50_cons_ms","lat99_cons_ms","thr_cons_ops_s",
        "rdb_bytes","aof_bytes","evicted_keys"
    ]
    existe = os.path.isfile(fname)
    with open(fname, "a", newline="") as f:
        w = csv.writer(f)
        if not existe:
            w.writerow(header)
        w.writerow([
            datetime.now().isoformat(),
            modo, politica, dataset,
            f"{p50_ins:.3f}", f"{p99_ins:.3f}", f"{thr_ins:.1f}",
            f"{p50_con:.3f}", f"{p99_con:.3f}", f"{thr_con:.1f}",
            datos["rdb_size"], datos["aof_size"], datos["evicted_keys"]
        ])


def main():
    if len(sys.argv) != 7:
        print("Uso: test_redis_queue.py <csv> <modo> <politica> <dataset> <redis_dir> <out_csv>")
        sys.exit(1)

    path_csv, modo, politica, dataset, redis_dir, out_csv = sys.argv[1:]
    if not os.path.isfile(path_csv):
        print("ERROR: CSV no existe:", path_csv); sys.exit(1)
    if not os.path.isdir(redis_dir):
        print("ERROR: redis_dir no es dir:", redis_dir); sys.exit(1)

    r = redis.Redis(host="localhost", port=6380, decode_responses=True)
    print("=== Ejecutando benchmark de cola de tickets ===")
    print(f"Modo={modo}, Pol={politica}, Dataset={dataset}")

    tickets = cargar_tickets(path_csv, int(dataset))
    # Inicializar cola y datos previos
    r.flushdb()

    p50_i, p99_i, thr_i, p50_c, p99_c, thr_c = ejecutar_operaciones(r, tickets)
    mets = medir_metricas(r, redis_dir, modo)
    guardar_csv(out_csv, mets, modo, politica, dataset,
                p50_i, p99_i, thr_i, p50_c, p99_c, thr_c)

    print("Benchmark completado. Resultados en", out_csv)

if __name__ == "__main__":
    main()
