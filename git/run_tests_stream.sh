#!/usr/bin/env bash
set -euo pipefail

echo "Iniciando pruebas automatizadas para STREAM"

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

declare -A MEMORY_POLICIES_CMDS
MEMORY_POLICIES_CMDS["noeviction"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy noeviction"
MEMORY_POLICIES_CMDS["allkeys_lru"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-lru"
MEMORY_POLICIES_CMDS["allkeys_lfu"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-lfu"
MEMORY_POLICIES_CMDS["allkeys_random"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-random"

declare -A DATASET_SIZES
DATASET_SIZES["100k"]=100000
DATASET_SIZES["500k"]=500000
DATASET_SIZES["1M"]=1000000

RESULTS_FILE="/Users/adelinacurbelo/Documents/bdnr-2025/bdnr-pruebas/resultados.csv"
REDIS_PERSISTENCE_DIR="/Users/adelinacurbelo/Documents/bdnr-2025/bdnr-pruebas/persistencia"
PYTHON_SCRIPT_STREAM="/Users/adelinacurbelo/Documents/bdnr-2025/bdnr-pruebas/test_stream.py"
GZ_PATH="/Users/adelinacurbelo/Documents/bdnr-2025/bdnr-pruebas/2025-06-01-15.json.gz"

mkdir -p "$REDIS_PERSISTENCE_DIR"

for mode in "${!PERSISTENCE_MODES_CMDS[@]}"; do
  for policy in "${!MEMORY_POLICIES_CMDS[@]}"; do
    for label in "${!DATASET_SIZES[@]}"; do
      size=${DATASET_SIZES[$label]}
      echo "🧪 STREAM → $mode | $policy | $label ($size)"

      docker rm -f redis-bdnr-ranking >/dev/null 2>&1 || true

      docker run -d         --name redis-bdnr-ranking         -v "$REDIS_PERSISTENCE_DIR":/data         -p 6380:6379         redis:7.4         redis-server --dir /data --dbfilename dump.rdb

      for cmdVar in ${PERSISTENCE_MODES_CMDS[$mode]}; do
        declare -n arr="$cmdVar"
        docker exec redis-bdnr-ranking redis-cli "${arr[@]}"
      done

      IFS=';' read -ra memcmds <<< "${MEMORY_POLICIES_CMDS[$policy]}"
      for mcmd in "${memcmds[@]}"; do
        read -ra parts <<< "$mcmd"
        docker exec redis-bdnr-ranking redis-cli "${parts[@]}"
      done

      docker exec redis-bdnr-ranking redis-cli CONFIG RESETSTAT
      until docker exec redis-bdnr-ranking redis-cli PING 2>/dev/null | grep -q PONG; do sleep 0.2; done
      docker exec redis-bdnr-ranking redis-cli FLUSHALL

      echo "  -> Ejecutando benchmark con test_stream.py"
      python3 "$PYTHON_SCRIPT_STREAM"         "$GZ_PATH"         "$mode"         "$policy"         "$size"         "$REDIS_PERSISTENCE_DIR"         "$RESULTS_FILE"

      docker exec redis-bdnr-ranking redis-cli SAVE
    done
  done
done

echo "✅ Pruebas STREAM finalizadas. Resultados en $RESULTS_FILE"
