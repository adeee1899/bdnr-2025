import redis
import random
from faker import Faker
import time

# Configuración de Redis
r = redis.Redis(host='localhost', port=6379, db=0)

fake = Faker()

# Ajustar parametros
NUM_ARTICLES = 500
NUM_USERS = 100
MAX_VOTES_PER_ARTICLE = 2000
MAX_COMMENTS_PER_ARTICLE = 30
TOTAL_VISITS = random.randint(10, 100)

# Reset de base de datos
r.flushdb()

ranking_key = "ranking_articles"

pipe = r.pipeline()

start_time = time.time()

for article_id in range(1, NUM_ARTICLES + 1):
    score = random.randint(0, 100_000)
    pipe.zadd(ranking_key, {article_id: score})

    # Generar datos del artículo
    article_key = f"article:{article_id}"
    pipe.hset(article_key, mapping={
        "title": fake.sentence(),
        "author": fake.name(),
        "url": fake.url()
    })

    # Generar visitas aleatorias
    votes_key = f"votes:{article_id}"
    num_votes = random.randint(0, MAX_VOTES_PER_ARTICLE)
    if num_votes > 0:
        voted_users = random.sample(range(1, NUM_USERS + 1), min(num_votes, NUM_USERS))
        pipe.sadd(votes_key, *voted_users)

    # Generar comentarios aleatorios"
    comments_key = f"comments:{article_id}"
    num_comments = random.randint(0, MAX_COMMENTS_PER_ARTICLE)
    for _ in range(num_comments):
        pipe.rpush(comments_key, fake.sentence())

    # Cada 50 artículos, ejecutamos el pipeline y mostramos progreso
    if article_id % 50 == 0:
        pipe.execute()
        elapsed = time.time() - start_time
        start_time = time.time()  # Reset tiempo para siguiente batch
        pipe = r.pipeline()  # Reiniciar pipeline para el próximo batch

# Insertar visitas totales y ejecutar lo que queda en el pipeline
pipe.set("visits_total", TOTAL_VISITS)
pipe.execute()

print(f"Dataset completo cargado: {NUM_ARTICLES} artículos, {NUM_USERS} usuarios, {TOTAL_VISITS} visitas totales")
