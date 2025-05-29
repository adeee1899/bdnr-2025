import redis
import pandas as pd
import random
import time
import os
from datetime import datetime
import sys
import csv
import subprocess

def cargar_datos(r, path_csv, max_articulos=1000000, expandir_articulos=True):
    r.flushdb()
    print("Base de datos Redis vaciada.")

    df = pd.read_csv(path_csv)
    df = df.dropna(subset=["title", "url", "author"])

    contador = 0
    for idx, row in df.iterrows():
        if contador >= max_articulos:
            break

        base_article_id = f"hn:{idx}"
        versiones = [0] if not expandir_articulos else range(4)

        for v in versiones:
            if contador >= max_articulos:
                break

            article_id = f"{base_article_id}_{v}" if expandir_articulos else base_article_id
            title = row["title"] if v == 0 else f"{row['title']} - versión {v}"
            url = row["url"] if v == 0 else f"{row['url']}?v={v}"
            num_points = int(row["num_points"]) + v * 3
            num_comments = int(row["num_comments"]) + v * 2

            r.zadd("ranking_articles", {article_id: num_points})
            r.hset(f"article:{article_id}", mapping={
                "title": title,
                "author": row["author"],
                "url": url,
                "created_at": row["created_at"],
                "num_points": num_points,
                "num_comments": num_comments,
            })

            contador += 1
            if contador % 100000 == 0:
                print(f"Cargados {contador} artículos...")

    print(f"Carga inicial completada. Total artículos cargados: {r.zcard('ranking_articles')}")
    return contador

def ejecutar_operaciones(r, rondas=3):
    total = r.zcard("ranking_articles")
    if total == 0:
        print("No hay artículos cargados para operar.")
        return 0, 0, 0  # lat_p50, lat_p99, throughput

    latencias = []
    total_ops = 0
    start_time = time.time()

    for ronda in range(rondas):
        sample_size = max(1, total // 10)
        artículos_random = random.sample(range(total), sample_size)

        article_ids = r.zrange("ranking_articles", 0, -1)
        seleccionados = [article_ids[i] for i in artículos_random]

        for aid in seleccionados:
            op_start = time.time()
            inc = random.randint(1, 5)
            r.zincrby("ranking_articles", inc, aid)
            op_end = time.time()
            latencias.append(op_end - op_start)
            total_ops += 1

        top10 = r.zrevrange("ranking_articles", 0, 9, withscores=True)
        for aid, score in top10:
            _ = r.hgetall(f"article:{aid}")

        print(f"Ronda {ronda+1}/{rondas} completada.")

    total_time = time.time() - start_time
    latencias_ms = [l * 1000 for l in latencias]
    lat_p50 = sorted(latencias_ms)[len(latencias_ms)//2]
    lat_p99 = sorted(latencias_ms)[int(len(latencias_ms)*0.99) if len(latencias_ms) > 1 else 0]
    throughput = total_ops / total_time if total_time > 0 else 0

    return lat_p50, lat_p99, throughput

def medir_metricas(r, redis_dir):
    info = r.info("memory")
    memory_used = info.get("used_memory", 0)
    evicted_keys = info.get("evicted_keys", 0)
    
    rdb_size = 0
    aof_size = 0
    rdb_path = os.path.join(redis_dir, "dump.rdb")
    aof_path = os.path.join(redis_dir, "appendonly.aof")

    if os.path.exists(rdb_path):
        rdb_size = os.path.getsize(rdb_path)
    if os.path.exists(aof_path):
        aof_size = os.path.getsize(aof_path)

    return {
        "memory_used": memory_used,
        "evicted_keys": evicted_keys,
        "rdb_size": rdb_size,
        "aof_size": aof_size
    }

def guardar_resultados_csv(filename, datos, modo_persistencia, politica_memoria, tam_dataset, lat_p50, lat_p99, ops_por_seg, tiempo_reinicio, errores):
    existe = os.path.isfile(filename)
    with open(filename, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not existe:
            writer.writerow(["fecha", "modo_persistencia", "politica_memoria", "tam_dataset", "lat_p50_ms", "lat_p99_ms", "ops_por_seg", "rdb_size_bytes", "aof_size_bytes", "tiempo_reinicio_ms", "evicted_keys", "errores"])
        writer.writerow([
            datetime.now().isoformat(),
            modo_persistencia,
            politica_memoria,
            tam_dataset,
            lat_p50,
            lat_p99,
            ops_por_seg,
            datos["rdb_size"],
            datos["aof_size"],
            tiempo_reinicio,
            datos["evicted_keys"],
            errores
        ])

def medir_tiempo_reinicio():
    start = time.time()
    result = subprocess.run(["docker", "restart", "redis-bdnr-ranking"], capture_output=True)
    if result.returncode != 0:
        print("Error reiniciando Redis:", result.stderr.decode())
        return -1
    time.sleep(10)
    end = time.time()
    return (end - start) * 1000

def main():
    if len(sys.argv) < 7:
        print("Uso: python test_redis.py <modo_persistencia> <politica_memoria> <tam_dataset> <max_articulos> <ruta_persistencia_redis> <archivo_resultados_csv>")
        sys.exit(1)
    print("Iniciando script", flush=True)
    modo_persistencia = sys.argv[1]
    politica_memoria = sys.argv[2]
    tam_dataset = sys.argv[3]
    max_articulos = int(sys.argv[4])
    redis_dir = sys.argv[5]  # ahora viene como parámetro
    archivo_resultados = sys.argv[6]

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    path_csv = "/mnt/c/Users/PC/Escritorio/bdnr 2025/bdnr-2025/ranking/hacker_news.csv"  # path Linux para WSL

    print(f"Modo: {modo_persistencia}, Política: {politica_memoria}, Dataset: {tam_dataset}, Max artículos: {max_articulos}")

    cargar_datos(r, path_csv, max_articulos=max_articulos)
    lat_p50, lat_p99, ops_por_seg = ejecutar_operaciones(r)
    metricas = medir_metricas(r, redis_dir)
    tiempo_reinicio = medir_tiempo_reinicio()
    errores = 0

    guardar_resultados_csv(archivo_resultados, metricas, modo_persistencia, politica_memoria, tam_dataset, lat_p50, lat_p99, ops_por_seg, tiempo_reinicio, errores)
    print("Resultados guardados.")

if __name__ == "__main__":
    main()
