#Script usado para benchmark Redis con datos de Hacker News, se llama desde run_tests.sh
import redis
import pandas as pd
import random
import time
import os
import sys
import csv
from datetime import datetime

def cargar_datos(r, path_csv, max_articulos=1_000_000, expandir_articulos=True):
    r.flushdb()
    print("Base de datos Redis vaciada.")

    df = pd.read_csv(path_csv)
    total_filas = len(df)
    print(f"Filas totales en CSV: {total_filas}")
    print(f"Max artículos (incluyendo versiones): {max_articulos}")

    # Rellenar nulos
    for col, val in [("title","sin título"),
                     ("author","desconocido"),
                     ("url","sin url"),
                     ("created_at","unknown")]:
        df[col].fillna(val, inplace=True)
    df["num_points"].fillna(0, inplace=True)
    df["num_comments"].fillna(0, inplace=True)

    contador = 0
    id_global = 0

    while contador < max_articulos:
        for idx, row in df.iterrows():
            if contador >= max_articulos:
                break

            versiones = [0] if not expandir_articulos else range(4)
            for v in versiones:
                if contador >= max_articulos:
                    break

                article_id = f"hn:{id_global}"
                id_global += 1

                title    = row["title"] if v == 0 else f"{row['title']} - versión {v}"
                url      = row["url"]   if v == 0 else f"{row['url']}?v={v}"
                pts      = int(row["num_points"])   + v*3
                comments = int(row["num_comments"]) + v*2

                try:
                    r.zadd("ranking_articles", {article_id: pts})
                    r.hset(f"article:{article_id}", mapping={
                        "title": title,
                        "author": row["author"],
                        "url": url,
                        "created_at": row["created_at"],
                        "num_points": pts,
                        "num_comments": comments,
                    })
                except redis.RedisError as e:
                    print("ERROR al insertar en Redis:", e)

                contador += 1
                if contador % 100000 == 0:
                    print(f"Cargados {contador} artículos...")

            if contador >= max_articulos:
                break

    print("Carga finalizada.")
    print(f"Total artículos insertados: {contador}")
    print(f"Total en ZSET: {r.zcard('ranking_articles')}")
    return contador

def ejecutar_operaciones(r, rondas=3):
    total = r.zcard("ranking_articles")
    if total == 0:
        print("No hay artículos cargados.")
        return 0, 0, 0

    latencias = []
    ops = 0
    t0 = time.time()

    for i in range(rondas):
        ids = r.zrange("ranking_articles", 0, -1)
        sample = random.sample(ids, max(1, len(ids)//10))
        for aid in sample:
            t_start = time.time()
            r.zincrby("ranking_articles", random.randint(1,5), aid)
            latencias.append(time.time() - t_start)
            ops += 1
        for aid,_ in r.zrevrange("ranking_articles", 0, 9, withscores=True):
            _ = r.hgetall(f"article:{aid}")
        print(f"Ronda {i+1}/{rondas} completada.")

    dt = time.time() - t0
    ms = sorted([l*1000 for l in latencias])
    p50 = ms[len(ms)//2] if ms else 0
    p99 = ms[int(len(ms)*0.99)] if len(ms)>1 else p50
    thr = ops / dt if dt>0 else 0
    return p50, p99, thr

def esperar_bgsave(r, timeout=60.0, interval=0.5):
    start = time.time()

    # Esperar a que no haya un BGSAVE en progreso
    while True:
        info = r.info("persistence")
        if info.get("rdb_bgsave_in_progress") == 0:
            break
        if time.time() - start > timeout:
            print("WARNING: timeout esperando que termine BGSAVE previo")
            return False
        time.sleep(interval)

    # Lanzar nuevo BGSAVE
    r.bgsave()

    # Esperar a que termine este BGSAVE
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
    # — Forzar persistencia —
    if modo.startswith("rdb") or modo=="mixto":
        esperar_bgsave(r)
    if modo.startswith("aof") or modo=="mixto":
        # Esperar y lanzar AOF rewrite
        start = time.time()
        # esperar que no haya rewrite en curso
        while r.info("persistence").get("aof_rewrite_in_progress", 0) == 1:
            time.sleep(0.1)
            if time.time() - start > 30:
                print("WARNING: timeout esperando fin de AOF rewrite previo")
                break
        # lanzar
        r.bgrewriteaof()
        # esperar que termine
        start = time.time()
        while r.info("persistence").get("aof_rewrite_in_progress", 0) == 1:
            time.sleep(0.1)
            if time.time() - start > 30:
                print("WARNING: timeout esperando nuevo AOF rewrite")
                break

    mem = r.info("memory")
    stats = r.info("stats")
    memory_used  = mem["used_memory"]
    evicted_keys = stats["evicted_keys"]

    # Tamaño RDB
    rdb_path = os.path.join(redis_dir, "dump.rdb")
    rdb_size = os.path.getsize(rdb_path) if os.path.exists(rdb_path) else 0

    # Tamaño AOF (busca en $REDIS_PERSISTENCE_DIR y en appendonlydir/)
    aof_size = 0

    # 1) Archivo principal si existe
    main_aof = os.path.join(redis_dir, "appendonly.aof")
    if os.path.exists(main_aof):
        aof_size += os.path.getsize(main_aof)

    # 2) Cualquier AOF dentro de appendonlydir/
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

def guardar_csv(fname, datos, modo, politica, dataset, lat_p50, lat_p99, thr):
    header = ["fecha","modo","politica","dataset","lat_p50_ms",
              "lat_p99_ms","throughput","rdb_bytes","aof_bytes",
              "evicted_keys"]
    existe = os.path.isfile(fname)
    with open(fname, "a", newline="") as f:
        w = csv.writer(f)
        if not existe:
            w.writerow(header)
        w.writerow([
            datetime.now().isoformat(),
            modo, politica, dataset,
            f"{lat_p50:.3f}", f"{lat_p99:.3f}", f"{thr:.1f}",
            datos["rdb_size"], datos["aof_size"], datos["evicted_keys"]
        ])

def main():
    if len(sys.argv)!=7:
        print("Uso: test_redis.py <csv> <modo> <politica> <dataset> <redis_dir> <out_csv>")
        sys.exit(1)

    path_csv, modo, politica, dataset, redis_dir, out_csv = sys.argv[1:]
    if not os.path.isfile(path_csv):
        print("ERROR: CSV no existe:", path_csv); sys.exit(1)
    if not os.path.isdir(redis_dir):
        print("ERROR: redis_dir no es dir:", redis_dir); sys.exit(1)

    r = redis.Redis(host="localhost", port=6380, decode_responses=True)

    print("=== Ejecutando benchmark Redis ===")
    print(f"Modo={modo}, Pol={politica}, Dataset={dataset}")

    cargar_datos(r, path_csv, max_articulos=int(dataset))
    p50, p99, thr = ejecutar_operaciones(r)
    mets = medir_metricas(r, redis_dir, modo)
    guardar_csv(out_csv, mets, modo, politica, dataset, p50, p99, thr)

    print("Benchmark completado. Resultados en", out_csv)

if __name__=="__main__":
    main()
