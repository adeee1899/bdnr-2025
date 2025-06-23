# -*- coding: utf-8 -*-
"""
Script para benchmarkear inserciones en un Stream de Redis y registrar las métricas.
"""

import gzip
import json
import time
import sys
import csv
import os
from datetime import datetime
from redis import Redis, RedisError

def esperar_bgsave(r, timeout=60.0, interval=0.5):
    """Espera a que finalice un BGSAVE previo y lanza otro para asegurar
    que haya un snapshot RDB actualizado."""
    start = time.time()
    while True:
        if r.info("persistence").get("rdb_bgsave_in_progress", 0) == 0:
            break
        if time.time() - start > timeout:
            print("WARNING: timeout esperando fin de BGSAVE previo")
            return False
        time.sleep(interval)

    r.bgsave()

    while True:
        if r.info("persistence").get("rdb_bgsave_in_progress", 0) == 0:
            break
        if time.time() - start > timeout:
            print("WARNING: timeout esperando BGSAVE")
            return False
        time.sleep(interval)
    return True


def medir_metricas(r: Redis, redis_dir: str, modo: str):
    """Fuerza persistencia y devuelve estadísticas de uso de disco/RAM."""
    if modo.startswith("rdb") or modo == "mixto":
        esperar_bgsave(r)
    if modo.startswith("aof") or modo == "mixto":
        # Iniciar un BGREWRITEAOF para consolidar ficheros.
        start = time.time()
        while r.info("persistence").get("aof_rewrite_in_progress", 0) == 1:
            if time.time() - start > 30:
                print("WARNING: timeout esperando fin de AOF rewrite previo")
                break
            time.sleep(0.1)
        r.bgrewriteaof()
        start = time.time()
        while r.info("persistence").get("aof_rewrite_in_progress", 0) == 1:
            if time.time() - start > 30:
                print("WARNING: timeout esperando nuevo AOF rewrite")
                break
            time.sleep(0.1)

    mem = r.info("memory")
    stats = r.info("stats")
    evicted_keys = stats.get("evicted_keys", 0)

    # Tamaño RDB
    rdb_path = os.path.join(redis_dir, "dump.rdb")
    rdb_bytes = os.path.getsize(rdb_path) if os.path.exists(rdb_path) else 0

    # Tamaño AOF (archivos modulares incluidos)
    aof_bytes = 0
    main_aof = os.path.join(redis_dir, "appendonly.aof")
    if os.path.exists(main_aof):
        aof_bytes += os.path.getsize(main_aof)
    append_dir = os.path.join(redis_dir, "appendonlydir")
    if os.path.isdir(append_dir):
        for fname in os.listdir(append_dir):
            if fname.startswith("appendonly.aof"):
                aof_bytes += os.path.getsize(os.path.join(append_dir, fname))

    return {
        "evicted_keys": evicted_keys,
        "rdb_bytes": rdb_bytes,
        "aof_bytes": aof_bytes,
    }


def guardar_csv(path: str, datos: dict, modo: str, politica: str, dataset: int,
                p50: float, p99: float, thr: float):
    header = [
        "fecha", "modo", "politica", "dataset", "lat_p50_ms", "lat_p99_ms",
        "throughput", "rdb_bytes", "aof_bytes", "evicted_keys",
    ]
    existe = os.path.isfile(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if not existe:
            w.writerow(header)
        w.writerow([
            datetime.now().isoformat(), modo, politica, dataset,
            f"{p50:.3f}", f"{p99:.3f}", f"{thr:.1f}",
            datos["rdb_bytes"], datos["aof_bytes"], datos["evicted_keys"],
        ])

def main():
    if len(sys.argv) != 7:
        print("Uso: python3 test_stream_metrics.py <archivo_gz> <modo> "
              "<politica> <cantidad_eventos> <redis_dir> <out_csv>")
        sys.exit(1)

    archivo_gz, modo, politica, cantidad, redis_dir, out_csv = sys.argv[1:]
    cantidad = int(cantidad)

    if not os.path.isfile(archivo_gz):
        print("ERROR: archivo .gz inexistente:", archivo_gz)
        sys.exit(1)
    if not os.path.isdir(redis_dir):
        print("ERROR: redis_dir no es un directorio válido:", redis_dir)
        sys.exit(1)

    r = Redis(host="localhost", port=6380, decode_responses=True)

    # Limpiar stream existente.
    r.delete("user_activity_stream")

    latencias = []
    cargados = 0
    inicio = time.time()

    with gzip.open(archivo_gz, "rt") as f:
        for i, linea in enumerate(f):
            if i >= cantidad:
                break
            try:
                evt = json.loads(linea)
                user = evt.get("actor", {}).get("login")
                tipo = evt.get("type")
                ts   = evt.get("created_at")
                if not (user and tipo and ts):
                    continue

                t0 = time.time()
                r.xadd("user_activity_stream", {
                    "user": user,
                    "type": tipo,
                    "timestamp": ts,
                })
                latencias.append(time.time() - t0)
                cargados += 1
                if cargados % 100000 == 0:
                    elapsed = time.time() - inicio
                    print(f"Cargados {cargados} eventos... ({elapsed:.1f}s)")
            except (ValueError, RedisError):
                continue

    duracion = time.time() - inicio
    if cargados == 0:
        print("No se cargó ningún evento.")
        sys.exit(1)

    # Cálculo de métricas de latencia y throughput
    ms = sorted(l * 1000 for l in latencias)
    p50 = ms[len(ms)//2]
    p99 = ms[int(len(ms)*0.99)] if len(ms) > 1 else p50
    throughput = cargados / duracion

    print(f"\nInserción terminada: {cargados} eventos en {duracion:.2f}s")
    print(f"p50 = {p50:.3f} ms | p99 = {p99:.3f} ms | thr = {throughput:.1f} op/s")

    # Métricas de persistencia / memoria
    mets = medir_metricas(r, redis_dir, modo)

    # Persistir resultados
    guardar_csv(out_csv, mets, modo, politica, cantidad, p50, p99, throughput)
    print("Resultados añadidos en", out_csv)


if __name__ == "__main__":
    main()
