#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark Redis con sistema de cola de tickets de soporte.
Usa LIST y HASH de Redis.
Se llama desde run_tests.sh
"""
import redis
import pandas as pd
import random
import time
import os
import sys
import csv
from datetime import datetime

def cargar_tickets(path_csv, max_tickets, expandir_tickets=True):
    """Carga el CSV en memoria, expandiendo si es necesario para llegar a max_tickets"""
    df = pd.read_csv(path_csv)
    total = len(df)
    print(f"Filas totales en CSV: {total}")
    
    # Rellenar nulos correctamente según tipo
    for col in df.select_dtypes(include=["object"]).columns:
        df[col].fillna("", inplace=True)
    for col in df.select_dtypes(include=["number"]).columns:
        df[col].fillna(0, inplace=True)

    tickets = []
    contador = 0
    id_global = 0

    while contador < max_tickets:
        for _, row in df.iterrows():
            if contador >= max_tickets:
                break

            versiones = [0] if not expandir_tickets else range(4)
            for v in versiones:
                if contador >= max_tickets:
                    break

                ticket_id = f"tkt:{id_global}"
                id_global += 1

                subject = row["Ticket Subject"] if v == 0 else f"{row['Ticket Subject']} - versión {v}"

                ticket = {
                    "Ticket ID": ticket_id,
                    "Customer Name": row.get("Customer Name", ""),
                    "Customer Email": row.get("Customer Email", ""),
                    "Ticket Subject": subject,
                    "Ticket Priority": row.get("Ticket Priority", ""),
                    "Ticket Status": row.get("Ticket Status", ""),
                }
                tickets.append(ticket)
                contador += 1

                if contador % 100000 == 0:
                    print(f"Cargados {contador} tickets...")

            if contador >= max_tickets:
                break

    print(f"Tickets cargados en memoria: {len(tickets)}")
    return tickets


def ejecutar_operaciones(r, tickets, rondas=3):
    """Mide latencias de inserción y consumo en Redis"""
    consume_lat = []
    n = len(tickets)

    # Inserción de todos los tickets antes de consumo
    print("=== Fase de inserción ===")
    for t in tickets:
        r.hset(f"ticket:{t['Ticket ID']}", mapping={
            "customer_name": t["Customer Name"],
            "customer_email": t["Customer Email"],
            "ticket_subject": t["Ticket Subject"],
            "ticket_priority": t["Ticket Priority"],
            "ticket_status": t["Ticket Status"],
        })
        r.lpush("tickets_queue", t["Ticket ID"])

    # Consumo de tickets simulando procesamiento por rondas
    print("=== Fase de consumo ===")
    ops = 0
    t0 = time.time()
    for i in range(rondas):
        while True:
            start = time.time()
            tid = r.rpop("tickets_queue")
            if tid is None:
                break
            _ = r.hgetall(f"ticket:{tid}")
            consume_lat.append(time.time() - start)
            ops += 2  # RPOP + HGETALL
        print(f"Ronda {i+1}/{rondas} completada.")
    dt = time.time() - t0

    ms = sorted([l * 1000 for l in consume_lat])
    p50 = ms[len(ms) // 2] if ms else 0
    p99 = ms[int(len(ms) * 0.99)] if len(ms) > 1 else p50
    thr = ops / dt if dt > 0 else 0

    return p50, p99, thr


def esperar_bgsave(r, timeout=60.0, interval=0.5):
    start = time.time()
    while True:
        info = r.info("persistence")
        if info.get("rdb_bgsave_in_progress") == 0:
            break
        if time.time() - start > timeout:
            print("WARNING: timeout esperando fin de BGSAVE previo")
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
    memory_used = mem["used_memory"]
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

    return {
        "memory_used": memory_used,
        "evicted_keys": evicted_keys,
        "rdb_size": rdb_size,
        "aof_size": aof_size
    }


def guardar_csv(fname, datos, modo, politica, dataset, p50, p99, thr):
    header = ["fecha","modo","politica","dataset",
              "lat_p50_ms","lat_p99_ms","throughput",
              "rdb_bytes","aof_bytes","evicted_keys"]
    existe = os.path.isfile(fname)
    with open(fname, "a", newline="") as f:
        w = csv.writer(f)
        if not existe:
            w.writerow(header)
        w.writerow([
            datetime.now().isoformat(),
            modo, politica, dataset,
            f"{p50:.3f}", f"{p99:.3f}", f"{thr:.1f}",
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
    p50, p99, thr = ejecutar_operaciones(r, tickets)
    mets = medir_metricas(r, redis_dir, modo)
    guardar_csv(out_csv, mets, modo, politica, dataset, p50, p99, thr)

    print("Benchmark completado. Resultados en", out_csv)

if __name__ == "__main__":
    main()
