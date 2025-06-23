#!/usr/bin/env bash
#script para ejecutar pruebas de persistencia y memoria en Redis, llama a test_redis.py
set -euo pipefail

echo "Empezando el script :)"

# ------------------------------
# 1) Modos de persistencia y sus comandos
# ------------------------------
declare -A PERSISTENCE_MODES_CMDS
PERSISTENCE_MODES_CMDS["rdb"]="cmd1 cmd2"
PERSISTENCE_MODES_CMDS["aof_everysec"]="cmd3 cmd4 cmd5"
PERSISTENCE_MODES_CMDS["aof_always"]="cmd6 cmd7 cmd8"
PERSISTENCE_MODES_CMDS["mixto"]="cmd9 cmd10 cmd11"

declare -a cmd1=("CONFIG" "SET" "save" "60 1")
declare -a cmd2=("CONFIG" "SET" "appendonly" "no")
declare -a cmd3=("CONFIG" "SET" "save" "")
declare -a cmd4=("CONFIG" "SET" "appendonly" "yes")
declare -a cmd5=("CONFIG" "SET" "appendfsync" "everysec")
declare -a cmd6=("CONFIG" "SET" "save" "")
declare -a cmd7=("CONFIG" "SET" "appendonly" "yes")
declare -a cmd8=("CONFIG" "SET" "appendfsync" "always")
declare -a cmd9=("CONFIG" "SET" "save" "900 1 300 10 60 10000")
declare -a cmd10=("CONFIG" "SET" "appendonly" "yes")
declare -a cmd11=("CONFIG" "SET" "appendfsync" "everysec")

# ------------------------------
# 2) Políticas de memoria
# ------------------------------
declare -A MEMORY_POLICIES_CMDS
MEMORY_POLICIES_CMDS["noeviction"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy noeviction"
MEMORY_POLICIES_CMDS["allkeys_lru"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-lru"
MEMORY_POLICIES_CMDS["allkeys_lfu"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-lfu"
MEMORY_POLICIES_CMDS["allkeys_random"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-random"

# ------------------------------
# 3) Tamaños de dataset (labels y valores)
# ------------------------------
declare -A DATASET_SIZES
DATASET_SIZES["100k"]=100000
DATASET_SIZES["500k"]=500000
DATASET_SIZES["1M"]=1000000

# ------------------------------
# 4) Variables globales
# ------------------------------
RESULTS_FILE="/mnt/c/Users/PC/Escritorio/bdnr 2025/bdnr-2025/queue/resultados.csv"
REDIS_PERSISTENCE_DIR="/home/sofi/redis-data"
PYTHON_SCRIPT="/mnt/c/Users/PC/Escritorio/bdnr 2025/bdnr-2025/queue/test_redis.py"
CSV_PATH="/mnt/c/Users/PC/Escritorio/bdnr 2025/bdnr-2025/queue/customer_support_tickets.csv"

mkdir -p "$REDIS_PERSISTENCE_DIR"

echo "Iniciando pruebas…"

for mode in "${!PERSISTENCE_MODES_CMDS[@]}"; do
  for policy in "${!MEMORY_POLICIES_CMDS[@]}"; do
    for label in "${!DATASET_SIZES[@]}"; do
      size=${DATASET_SIZES[$label]}

      echo "----------------------------------------------"
      echo "Modo persistencia: $mode | Política memoria: $policy | Dataset: $label ($size)"

      docker rm -f redis-bdnr-ranking >/dev/null 2>&1 || true

      docker run -d \
        --name redis-bdnr-ranking \
        -v "$REDIS_PERSISTENCE_DIR":/data \
        -p 6380:6379 \
        redis:7.4 \
        redis-server --dir /data --dbfilename dump.rdb

      for cmdVar in ${PERSISTENCE_MODES_CMDS[$mode]}; do
        declare -n arr="$cmdVar"
        echo "  -> CONFIG SET (persistencia): ${arr[*]}"
        docker exec redis-bdnr-ranking redis-cli "${arr[@]}"
      done

      IFS=';' read -ra memcmds <<< "${MEMORY_POLICIES_CMDS[$policy]}"
      for mcmd in "${memcmds[@]}"; do
        read -ra parts <<< "$mcmd"
        echo "  -> ${parts[*]}"
        docker exec redis-bdnr-ranking redis-cli "${parts[@]}"
      done

      echo "  -> Reseteando estadísticas de Redis…"
      docker exec redis-bdnr-ranking redis-cli CONFIG RESETSTAT

      echo "  -> Esperando a que Redis responda PONG…"
      until docker exec redis-bdnr-ranking redis-cli PING 2>/dev/null | grep -q PONG; do
        sleep 0.2
      done
      echo "  -> Redis listo."

      docker exec redis-bdnr-ranking redis-cli FLUSHALL
      docker exec redis-bdnr-ranking redis-cli SET test_write_$RANDOM "$RANDOM"

      echo "  -> Ejecutando benchmark Python…"
      python3 "$PYTHON_SCRIPT" \
        "$CSV_PATH" \
        "$mode" \
        "$policy" \
        "$size" \
        "$REDIS_PERSISTENCE_DIR" \
        "$RESULTS_FILE"

      # Forzar snapshot SAVE después del benchmark
      echo "  -> Forzando snapshot SAVE"
      docker exec redis-bdnr-ranking redis-cli SAVE

      # Mostrar el dump generado
      echo "  -> dump.rdb generado en $REDIS_PERSISTENCE_DIR:"
      ls -lh "$REDIS_PERSISTENCE_DIR/dump.rdb"

      echo "  -> Contenido del dir de persistencia:"
      ls -lh "$REDIS_PERSISTENCE_DIR"
    done
  done
done

echo "Pruebas completadas. Resultados en $RESULTS_FILE"
