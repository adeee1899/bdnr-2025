#!/bin/bash
echo "Empezando el script :)"

# Defino arrays de comandos para cada modo de persistencia
declare -A PERSISTENCE_MODES_CMDS
PERSISTENCE_MODES_CMDS["rdb"]="cmd1 cmd2"
PERSISTENCE_MODES_CMDS["aof_everysec"]="cmd3 cmd4 cmd5"
PERSISTENCE_MODES_CMDS["aof_always"]="cmd6 cmd7 cmd8"
# PERSISTENCE_MODES_CMDS["mixto"]="cmd9 cmd10 cmd11"

# Comandos de persistencia como arrays
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

# Políticas de memoria
declare -A MEMORY_POLICIES_CMDS
MEMORY_POLICIES_CMDS["noeviction"]="CONFIG SET maxmemory 100gb; CONFIG SET maxmemory-policy noeviction"
# MEMORY_POLICIES_CMDS["allkeys_lru"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-lru"
# MEMORY_POLICIES_CMDS["allkeys_lfu"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-lfu"
# MEMORY_POLICIES_CMDS["allkeys_random"]="CONFIG SET maxmemory 100mb; CONFIG SET maxmemory-policy allkeys-random"

# Dataset sizes
declare -A DATASET_SIZES
DATASET_SIZES["500k"]=500000
DATASET_SIZES["1M"]=1000000

RESULTS_FILE="/mnt/c/Users/PC/Escritorio/bdnr 2025/bdnr-2025/ranking/resultados.csv"
REDIS_PERSISTENCE_DIR="/mnt/c/Users/PC/redis-data"
PYTHON_SCRIPT="/mnt/c/Users/PC/Escritorio/bdnr 2025/bdnr-2025/ranking/test_redis.py"

echo "Iniciando pruebas..."

for mode in "${!PERSISTENCE_MODES_CMDS[@]}"; do
  for policy in "${!MEMORY_POLICIES_CMDS[@]}"; do
    for size in "${!DATASET_SIZES[@]}"; do
      echo "=============================================="
      echo "Modo persistencia: $mode | Política memoria: $policy | Tamaño dataset: $size"

      # Ejecutar comandos de persistencia
      for cmd_name in ${PERSISTENCE_MODES_CMDS[$mode]}; do
        declare -n args_ref="$cmd_name"
        echo "Ejecutando: redis-cli ${args_ref[*]}"
        redis-cli "${args_ref[@]}"
      done

      # Ejecutar comandos de política de memoria
      IFS=';' read -ra cmds <<< "${MEMORY_POLICIES_CMDS[$policy]}"
      for cmd in "${cmds[@]}"; do
        read -ra args <<< "$cmd"
        echo "Ejecutando: redis-cli ${args[*]}"
        redis-cli "${args[@]}"
      done

      # Esperar a que Redis termine de cargar antes de continuar
      echo "Esperando a que Redis esté listo..."
      while true; do
        pong=$(redis-cli PING 2>/dev/null)
        if [ "$pong" == "PONG" ]; then
          break
        fi
        sleep 0.5
      done

      echo "Redis está listo."

      redis-cli FLUSHALL

      python3 "$PYTHON_SCRIPT" "$mode" "$policy" "$size" "${DATASET_SIZES[$size]}" "$REDIS_PERSISTENCE_DIR" "$RESULTS_FILE"
    done
  done
done

echo "Pruebas finalizadas. Resultados en $RESULTS_FILE"
