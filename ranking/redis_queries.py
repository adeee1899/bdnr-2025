import redis

r = redis.Redis(host='localhost', port=6379, db=0)

# Top 5 artículos por score (de mayor a menor)
top5 = r.zrevrange("ranking_articles", 0, 4, withscores=True)
print("Top 5 artículos:")
for aid, score in top5:
    print(f"Artículo {aid.decode()}: score {score}")

# Datos del artículo 1
art1 = r.hgetall("article:1")
print("\nDatos del artículo 1:")
for k, v in art1.items():
    print(f"{k.decode()}: {v.decode()}")

# Cantidad de votos del artículo 1
votes_count = r.scard("votes:1")
print(f"\nVotos del artículo 1: {votes_count}")

# Primeros 3 comentarios del artículo 1
comments = r.lrange("comments:1", 0, 2)
print("\nComentarios del artículo 1:")
for c in comments:
    print(c.decode())


# Cantidad total de artículos
total_articles = r.zcard("ranking_articles")
print(f"Cantidad total de artículos: {total_articles}")

# Visitas totales
visits = r.get("visits_total")

print(f"\nVisitas totales: {visits.decode()}")
