import redis
import pandas as pd
import time
import json
from datetime import datetime

def cargar_dataset(path, nrows=10000):
    df = pd.read_csv(path)
    df = df.dropna(subset=["title", "url", "author", "created_at"])
    return df.head(nrows)

def benchmark_detalles(r, df):
    r.flushdb()
    resultados = []

    # Inserción con HASH
    start_insert_hash = time.time()
    for idx, row in df.iterrows():
        article_id = f"article:{idx}"
        r.hset(article_id, mapping={
            "title": row["title"],
            "author": row["author"],
            "url": row["url"],
            "created_at": row["created_at"]
        })
    insert_hash_time = (time.time() - start_insert_hash) * 1000
    print(f"Tiempo inserción HASH (ms): {insert_hash_time:.2f}")

    # Inserción con STRING (JSON)
    start_insert_str = time.time()
    for idx, row in df.iterrows():
        article_id = f"article_str:{idx}"
        json_str = json.dumps({
            "title": row["title"],
            "author": row["author"],
            "url": row["url"],
            "created_at": row["created_at"]
        })
        r.set(article_id, json_str)
    insert_str_time = (time.time() - start_insert_str) * 1000
    print(f"Tiempo inserción STRING JSON (ms): {insert_str_time:.2f}")

    # Consulta HASH
    start_read_hash = time.time()
    for idx in range(len(df)):
        article_id = f"article:{idx}"
        _ = r.hgetall(article_id)
    read_hash_time = (time.time() - start_read_hash) * 1000
    print(f"Tiempo consulta HASH (ms): {read_hash_time:.2f}")

    # Consulta STRING (parse JSON)
    start_read_str = time.time()
    for idx in range(len(df)):
        article_id = f"article_str:{idx}"
        val = r.get(article_id)
        _ = json.loads(val) if val else None
    read_str_time = (time.time() - start_read_str) * 1000
    print(f"Tiempo consulta STRING JSON (ms): {read_str_time:.2f}")

    mem_used = r.info("memory")["used_memory"]

    resultados.append({
        "estructura": "HASH",
        "insercion_ms": round(insert_hash_time, 2),
        "lectura_ms": round(read_hash_time, 2),
        "memoria_bytes": mem_used,
        "timestamp": datetime.now().isoformat()
    })

    resultados.append({
        "estructura": "STRING_JSON",
        "insercion_ms": round(insert_str_time, 2),
        "lectura_ms": round(read_str_time, 2),
        "memoria_bytes": mem_used,
        "timestamp": datetime.now().isoformat()
    })

    return resultados

def main():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    df = cargar_dataset("hacker_news.csv", nrows=10000)
    resultados = benchmark_detalles(r, df)

    # Guardar resultados a CSV
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv("benchmark_detalles_resultados.csv", index=False)
    print("\nResultados guardados en benchmark_detalles_resultados.csv")

if __name__ == "__main__":
    main()
