#!/bin/bash
# Скрипт проверки пайплайна Firebird -> Iceberg
# Требует: кластер k8s с firebird, minio, iceberg-rest, flink
# Или локальный запуск с портами/хостами, доступными из pod'а

set -e

echo "=== Проверка пайплайна Firebird -> Iceberg ==="
echo ""

# 1. Сборка JAR
echo "1. Сборка firebird-job JAR..."
cd "$(dirname "$0")"
mvn -q package -DskipTests
JAR=$(ls -1 target/firebird-job-*-with-dependencies.jar 2>/dev/null | head -1)
if [ -z "$JAR" ]; then
    JAR="target/firebird-job-1.0.0.jar"
fi
echo "   JAR: $JAR"
echo ""

# 2. Параметры (настраиваются под окружение)
FB_URL="${FB_URL:-jdbc:firebirdsql://firebird:3050//firebird/data/testdb.fdb}"
FB_USER="${FB_USER:-SYSDBA}"
FB_PASS="${FB_PASS:-Q1w2e3r+}"
ICEBERG_DB="${ICEBERG_DB:-rzdm__mis}"
TABLE="${TABLE:-TEST}"

echo "2. Параметры:"
echo "   Firebird URL: $FB_URL"
echo "   Firebird user: $FB_USER"
echo "   Iceberg DB: $ICEBERG_DB"
echo "   Таблица: $TABLE"
echo ""

# 3. Варианты запуска
echo "3. Варианты запуска:"
echo ""
echo "   A) В Kubernetes (через flink run):"
echo "      kubectl exec -it deploy/flink-jobmanager -n <namespace> -- \\"
echo "        /opt/flink/bin/flink run -d /path/to/firebird-job-1.0.0.jar \\"
echo "        --table $TABLE \\"
echo "        --firebird-url '$FB_URL' \\"
echo "        --firebird-user $FB_USER \\"
echo "        --firebird-pass '$FB_PASS' \\"
echo "        --iceberg-db $ICEBERG_DB"
echo ""
echo "   B) Локально (если Firebird/MinIO/Iceberg доступны):"
echo "      java -cp \"$JAR:$HOME/.m2/repository/org/apache/flink/*/flink-streaming-java-*.jar:...\" \\"
echo "        com.rzdmed.flink.FirebirdToIcebergJob --table $TABLE"
echo ""
echo "   C) Через Flink REST API (submit job):"
echo "      curl -X POST 'http://flink-jobmanager:8081/v1/jars/upload' \\"
echo "        -F 'jarfile=@$JAR'"
echo ""

# 4. Быстрая проверка доступности сервисов (если в k8s)
if command -v kubectl &>/dev/null; then
    echo "4. Проверка сервисов в кластере..."
    for svc in firebird minio-svc iceberg-rest flink-jobmanager; do
        if kubectl get svc "$svc" -A &>/dev/null 2>&1; then
            echo "   [OK] $svc найден"
        else
            echo "   [--] $svc не найден (проверьте namespace)"
        fi
    done
else
    echo "4. kubectl не найден, проверка сервисов пропущена"
fi

echo ""
echo "5. Перед первым запуском убедитесь, что в Firebird есть таблица TEST:"
echo "   isql -user SYSDBA -password Q1w2e3r+ firebird_host:/firebird/data/testdb.fdb <<EOF"
echo "   CREATE TABLE TEST (ID INT, NAME VARCHAR(100));"
echo "   INSERT INTO TEST VALUES (1, 'row1');"
echo "   COMMIT;"
echo "   EOF"
echo ""
echo "=== Готово. Запустите job одним из способов выше. ==="
