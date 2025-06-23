# Script usado para verificar la carga de datos en Redis
import redis
import json

# Conexión a Redis
r = redis.Redis(host='localhost', port=6380, decode_responses=True)

print("=== Verificación de carga en Redis ===\n")

# 1. Total de artículos en el ranking
total = r.zcard("ranking_articles")
print(f"Total de artículos en el ZSET 'ranking_articles': {total}\n")

# 2. Top 5 artículos más populares
print("Top 5 artículos más populares (mayor puntaje):")
top5 = r.zrevrange("ranking_articles", 0, 4, withscores=True)
for aid, score in top5:
    title = r.hget(f"article:{aid}", "title")
    print(f"- {aid}: {title} ({int(score)} puntos)")
print()

# 3. Bottom 5 artículos menos populares
print("Bottom 5 artículos menos populares (menor puntaje):")
bottom5 = r.zrange("ranking_articles", 0, 4, withscores=True)
for aid, score in bottom5:
    title = r.hget(f"article:{aid}", "title")
    print(f"- {aid}: {title} ({int(score)} puntos)")
print()

# 4. Datos completos de un artículo específico (por ejemplo, hn:0_0)
ejemplo_id = "hn:0_0"
datos = r.hgetall(f"article:{ejemplo_id}")
print(f"Datos completos de '{ejemplo_id}':")
print(json.dumps(datos, indent=2, ensure_ascii=False))
print()

# 5. Contar cuántas versiones hay de un artículo base (ej: hn:0)
base = "hn:0"
versiones = [f"{base}_{v}" for v in range(4)]
existentes = [vid for vid in versiones if r.exists(f"article:{vid}")]
print(f"Versiones encontradas para {base}: {len(existentes)} -> {existentes}")
