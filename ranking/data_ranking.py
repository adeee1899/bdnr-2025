import redis
import pandas as pd
import random
import time
import os
from datetime import datetime

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
            if contador % 10000 == 0:
                print(f"Cargados {contador} artículos...")

    print(f"Carga inicial completada. Total artículos cargados: {r.zcard('ranking_articles')}")
    return contador

def ejecutar_operaciones(r, rondas=3):
    total = r.zcard("ranking_articles")
    if total == 0:
        print("No hay artículos cargados para operar.")
        return

    for ronda in range(rondas):
        # Elegir 10% artículos aleatorios para incrementar puntaje
        sample_size = max(1, total // 10)
        artículos_random = random.sample(range(total), sample_size)

        # Obtener IDs de artículos para esos índices con ZRANGE
        article_ids = r.zrange("ranking_articles", 0, -1)
        seleccionados = [article_ids[i] for i in artículos_random]

        # Incrementar puntajes
        for aid in seleccionados:
            inc = random.randint(1, 5)
            r.zincrby("ranking_articles", inc, aid)

        # Consultar top 10 artículos
        top10 = r.zrevrange("ranking_articles", 0, 9, withscores=True)

        # Consultar detalles
        for aid, score in top10:
            _ = r.hgetall(f"article:{aid}")

        print(f"Ronda {ronda+1}/{rondas} completada.")

def medir_metricas(r, redis_dir):
    info = r.info("memory")
    memory_used = info.get("used_memory", 0)
    evicted_keys = info.get("evicted_keys", 0)
    
    # Tamaño archivos persistencia
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
    import csv
    existe = os.path.isfile(filename)
    with open(filename, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not existe:
            writer.writerow(["fecha", "modo_persistencia", "politica_memoria", "tam_dataset", "lat_p50", "lat_p99", "ops_por_seg", "rdb_size", "aof_size", "tiempo_reinicio_ms", "evicted_keys", "errores"])
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

if __name__ == "__main__":
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    path = "C:/Users/PC/Escritorio/bdnr 2025/bdnr-2025/ranking/hacker_news.csv"

    max_art = 100000
    cargar_datos(r, path, max_articulos=max_art)
    ejecutar_operaciones(r, rondas=3)
    metrics = medir_metricas(r, "/var/lib/redis")  
    print(metrics)
