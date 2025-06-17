import redis
import pandas as pd
import time
from datetime import datetime

def cargar_dataset(path, nrows=10000):
    df = pd.read_csv(path)
    df = df.dropna(subset=["title", "url", "author", "num_points"])
    return df.head(nrows)

def benchmark_ranking(r, df):
    resultados = []
    estructuras = ["zset", "list", "set"]

    for estructura in estructuras:
        r.flushdb()
        print(f"\nProbando estructura: {estructura}")

        # Inserción
        start_insert = time.time()
        for idx, row in df.iterrows():
            article_id = f"article:{idx}"
            score = int(row["num_points"])

            if estructura == "zset":
                r.zadd("ranking_articles", {article_id: score})
            elif estructura == "list":
                r.rpush("ranking_articles_list", f"{article_id}:{score}")
            elif estructura == "set":
                r.sadd("ranking_articles_set", f"{article_id}:{score}")
        insert_time = (time.time() - start_insert) * 1000
        print(f"Tiempo inserción (ms): {insert_time:.2f}")

        # Actualización / Incremento puntajes
        start_update = time.time()
        for i in range(1000):
            idx_rand = i % len(df)
            article_id = f"article:{idx_rand}"
            incr = 1

            if estructura == "zset":
                r.zincrby("ranking_articles", incr, article_id)
            elif estructura == "list":
                items = r.lrange("ranking_articles_list", 0, -1)
                for pos, item in enumerate(items):
                    aid, score_str = item.split(":")
                    if aid == article_id:
                        new_score = int(score_str) + incr
                        r.lset("ranking_articles_list", pos, f"{aid}:{new_score}")
                        break
            elif estructura == "set":
                # Set no soporta score, no hacemos nada aquí
                pass
        update_time = (time.time() - start_update) * 1000
        print(f"Tiempo actualización (ms): {update_time:.2f}")

        # Lectura top 10 artículos más populares
        start_read = time.time()
        top10 = []
        if estructura == "zset":
            top10 = r.zrevrange("ranking_articles", 0, 9, withscores=True)
        elif estructura == "list":
            items = r.lrange("ranking_articles_list", 0, -1)
            items_sorted = sorted(items, key=lambda x: int(x.split(":")[1]), reverse=True)[:10]
            top10 = [(item.split(":")[0], int(item.split(":")[1])) for item in items_sorted]
        elif estructura == "set":
            items = r.smembers("ranking_articles_set")
            items_sorted = sorted(items, key=lambda x: int(x.split(":")[1]), reverse=True)[:10]
            top10 = [(item.split(":")[0], int(item.split(":")[1])) for item in items_sorted]
        read_time = (time.time() - start_read) * 1000
        print(f"Tiempo lectura top 10 (ms): {read_time:.2f}")

        mem_used = r.info("memory")["used_memory"]

        resultados.append({
            "estructura": estructura,
            "insercion_ms": round(insert_time, 2),
            "actualizacion_ms": round(update_time, 2),
            "lectura_top10_ms": round(read_time, 2),
            "memoria_bytes": mem_used,
            "timestamp": datetime.now().isoformat()
        })

        print(f"Top 10 artículos (id, score): {top10}")

    return resultados

def main():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    df = cargar_dataset("hacker_news.csv", nrows=10000)
    resultados = benchmark_ranking(r, df)

    # Guardar resultados a CSV
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv("benchmark_ranking_resultados.csv", index=False)
    print("\nResultados guardados en benchmark_ranking_resultados.csv")

if __name__ == "__main__":
    main()
