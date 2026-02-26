package com.rzdmed.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.table.api.StatementSet;

import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

/**
 * Универсальная Flink job: читает одну или несколько таблиц из Firebird
 * и создаёт/заполняет соответствующие таблицы в Iceberg.
 *
 * Параметры запуска:
 *   --table           TABLE_NAME           одна таблица (обратная совместимость)
 *   --tables          T1,T2,T3             несколько таблиц через запятую
 *                                          поддерживается маппинг: FB_TABLE:ice_table
 *   --iceberg-db      DB_NAME              (по умолчанию: rzdm) имя базы в Iceberg каталоге
 *   --firebird-url    JDBC_URL             (по умолчанию: jdbc:firebirdsql://firebird:3050//firebird/data/testdb.fdb)
 *   --firebird-user   USER                 (по умолчанию: SYSDBA)
 *   --firebird-pass   PASSWORD             (по умолчанию: Q1w2e3r+)
 *   --mode            append|replace       (по умолчанию: append) режим записи
 *   --order-by        COLUMN               (по умолчанию: первый столбец каждой таблицы) столбец ORDER BY
 *   --parallelism     N                    (по умолчанию: 8) уровень параллелизма для записи
 *   --fetch-size      N                    (по умолчанию: 50000) размер батча для чтения из Firebird
 *   --batch-size      N                    (по умолчанию: 50) количество таблиц в одном Flink job
 *
 * Примеры:
 *   # Одна таблица
 *   flink run firebird-job-1.0.0.jar --table TEST
 *
 *   # Несколько таблиц (имена Iceberg = lowercase от Firebird)
 *   flink run firebird-job-1.0.0.jar --tables TEST,EMPLOYEES,DEPARTMENTS
 *
 *   # Явный маппинг Firebird → Iceberg
 *   flink run firebird-job-1.0.0.jar --tables FB_USERS:users,FB_ORDERS:orders
 *
 *   # Комбинация: таблицы + общий order-by
 *   flink run firebird-job-1.0.0.jar --tables TEST,BIG_TEST --order-by ID --mode replace
 */
public class FirebirdToIcebergJob {

    // === Defaults ===
    private static final String DEFAULT_FB_URL = "jdbc:firebirdsql://firebird:3050//firebird/data/testdb.fdb";
    private static final String DEFAULT_FB_USER = "SYSDBA";
    private static final String DEFAULT_FB_PASS = "Q1w2e3r+";
    private static final String DEFAULT_ICEBERG_DB = "rzdm__mis";
    private static final String DEFAULT_MODE = "append";
    private static final int DEFAULT_PARALLELISM = 8;
    private static final int DEFAULT_FETCH_SIZE = 50000;
    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final int TECH_COLS_COUNT = 10;
    private static final String[] TECH_COL_BASE_NAMES = {
        "load_dttm", "load_dttm_tz", "load_id", "op", "ts_ms",
        "source_ts_ms", "src_system_code", "extract_dttm", "src_chng_dttm", "row_hash"
    };

    // === Iceberg catalog settings ===
    private static final String ICEBERG_CATALOG_URI = "http://iceberg-rest:8181";
    private static final String ICEBERG_WAREHOUSE = "s3://iceberg/";
    private static final String S3_ENDPOINT = "http://minio-svc:9000";
    private static final String S3_REGION = "ru-central1";
    private static final String S3_ACCESS_KEY = "minioadmin";
    private static final String S3_SECRET_KEY = "Q1w2e3r+";
    private static final String FLINK_CHECKPOINTS_PATH = "s3://rzdm-test-technical-area/flink/checkpoints";

    // ======================================================================

    public static void main(String[] args) throws Exception {

        // 1. Парсим аргументы
        String singleTable = getArg(args, "--table", null);
        String tablesArg   = getArg(args, "--tables", null);
        String icebergDb   = getArg(args, "--iceberg-db", DEFAULT_ICEBERG_DB);
        String fbUrl       = getArg(args, "--firebird-url", DEFAULT_FB_URL);
        String fbUser      = getArg(args, "--firebird-user", DEFAULT_FB_USER);
        String fbPass      = getArg(args, "--firebird-pass", DEFAULT_FB_PASS);
        String mode        = getArg(args, "--mode", DEFAULT_MODE);
        String orderBy     = getArg(args, "--order-by", null); // null = auto (first column)
        int parallelism    = Integer.parseInt(getArg(args, "--parallelism", String.valueOf(DEFAULT_PARALLELISM)));
        int fetchSize      = Integer.parseInt(getArg(args, "--fetch-size", String.valueOf(DEFAULT_FETCH_SIZE)));
        int batchSize      = Integer.parseInt(getArg(args, "--batch-size", String.valueOf(DEFAULT_BATCH_SIZE)));
        boolean failOnConsistencyError = Boolean.parseBoolean(
            getArg(args, "--fail-on-consistency-error", "false")
        );

        // 2. Строим список пар таблиц Firebird → Iceberg
        List<TableMapping> tableMappings = parseTableMappings(singleTable, tablesArg);
        if (tableMappings.isEmpty()) {
            System.err.println("ERROR: --table or --tables parameter is required!");
            System.err.println("Usage:");
            System.err.println("  --table TABLE_NAME                     одна таблица");
            System.err.println("  --tables T1,T2,T3                      несколько таблиц");
            System.err.println("  --tables FB_TABLE1:ice1,FB_TABLE2:ice2  явный маппинг");
            System.exit(1);
        }

        System.out.println("=== Firebird → Iceberg Transfer ===");
        System.out.println("Tables         : " + tableMappings.size());
        for (TableMapping tm : tableMappings) {
            System.out.println("  " + tm.fbTable + " → iceberg." + icebergDb + "." + tm.icebergTable);
        }
        System.out.println("Firebird URL   : " + fbUrl);
        System.out.println("Mode           : " + mode);
        System.out.println("Global order-by: " + (orderBy != null ? orderBy : "auto (first column)"));
        System.out.println("Parallelism    : " + parallelism);
        System.out.println("Fetch size     : " + fetchSize);
        System.out.println("Batch size     : " + batchSize + " tables per job");
        System.out.println("Fail on consistency error: " + failOnConsistencyError);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // Настройка чекпоинтов → S3 (MinIO)
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setCheckpointStorage(FLINK_CHECKPOINTS_PATH);
        cpConfig.setMinPauseBetweenCheckpoints(10000); // Уменьшено для более частых checkpoint
        cpConfig.setCheckpointTimeout(600000);
        cpConfig.setMaxConcurrentCheckpoints(1);
        cpConfig.setTolerableCheckpointFailureNumber(3); // Разрешить несколько неудачных checkpoint
        cpConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        System.out.println("Checkpointing: EXACTLY_ONCE, interval=60s, storage=" + FLINK_CHECKPOINTS_PATH);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. Создаём Iceberg каталог
        tableEnv.executeSql(
            "CREATE CATALOG iceberg WITH (" +
            "  'type' = 'iceberg'," +
            "  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog'," +
            "  'uri' = '" + ICEBERG_CATALOG_URI + "'," +
            "  'warehouse' = '" + ICEBERG_WAREHOUSE + "'," +
            "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'," +
            "  's3.endpoint' = '" + S3_ENDPOINT + "'," +
            "  's3.path-style-access' = 'true'," +
            "  'client.region' = '" + S3_REGION + "'," +
            "  's3.access-key-id' = '" + S3_ACCESS_KEY + "'," +
            "  's3.secret-access-key' = '" + S3_SECRET_KEY + "'" +
            ")"
        );

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg." + icebergDb);

        // 5. Обрабатываем таблицы батчами для избежания проблем с большим количеством операторов
        int totalTables = tableMappings.size();
        int processedTables = 0;
        int batchNumber = 1;

        while (processedTables < totalTables) {
            int batchStart = processedTables;
            int batchEnd = Math.min(processedTables + batchSize, totalTables);
            List<TableMapping> currentBatch = tableMappings.subList(batchStart, batchEnd);
            
            System.out.println();
            System.out.println("=== Processing batch " + batchNumber + ": tables " + (batchStart + 1) + "-" + batchEnd + " of " + totalTables + " ===");
            
            // Создаём новый StatementSet для каждого батча
            StatementSet stmtSet = tableEnv.createStatementSet();
            int tablesAdded = 0;
            List<TableLoadContext> loadedTables = new ArrayList<>();

            for (TableMapping tm : currentBatch) {
            System.out.println();
            System.out.println("--- Configuring table: " + tm.fbTable + " → " + tm.icebergTable + " ---");

            // 5a. Проверяем доступ к таблице
            if (!checkTableAccess(fbUrl, fbUser, fbPass, tm.fbTable)) {
                System.err.println("WARNING: No SELECT access to table '" + tm.fbTable + "'. Skipping.");
                continue;
            }

            // 5b. Читаем метаданные
            List<ColumnInfo> columns;
            try {
                columns = readTableMetadata(fbUrl, fbUser, fbPass, tm.fbTable);
            } catch (Exception e) {
                System.err.println("ERROR: Failed to read metadata for table '" + tm.fbTable + "': " + e.getMessage() + ". Skipping.");
                continue;
            }
            
            if (columns.isEmpty()) {
                System.err.println("ERROR: Table '" + tm.fbTable + "' not found or has no columns! Skipping.");
                continue;
            }

            System.out.println("  Columns: " + columns.size());
            for (ColumnInfo col : columns) {
                System.out.println("    " + col.name + " : " + col.icebergType + " (JDBC type: " + col.jdbcType + ")");
            }

            // 5c. Определяем ORDER BY (глобальный или первый столбец таблицы)
            String orderByColumn = (orderBy != null && !orderBy.isEmpty())
                    ? orderBy.toUpperCase()
                    : columns.get(0).name.toUpperCase();
            System.out.println("  Order by: " + orderByColumn);

            // 5d. Полный путь к таблице Iceberg
            String fullIcebergPath = "iceberg." + icebergDb + ".`" + tm.icebergTable + "`";

            // 5e. Создаём/пересоздаём Iceberg таблицу
            if ("replace".equalsIgnoreCase(mode)) {
                tableEnv.executeSql("DROP TABLE IF EXISTS " + fullIcebergPath);
            }
            String createSql = buildCreateTableSql(icebergDb, tm.icebergTable, columns);
            System.out.println("  Creating Iceberg table: " + createSql);
            tableEnv.executeSql(createSql);

            // 5f. Создаём Source DataStream с уникальным uid для checkpoint state
            RowTypeInfo rowTypeInfo = buildRowTypeInfo(columns);
            String safeTableName = tm.fbTable.toLowerCase().replace("$", "_");
            String sourceUid = "source-" + safeTableName;
            DataStream<Row> data = env
                .addSource(
                    new FirebirdDynamicSource(fbUrl, fbUser, fbPass, tm.fbTable, columns, orderByColumn, fetchSize),
                    "firebird-" + safeTableName,
                    rowTypeInfo
                )
                .uid(sourceUid);

            System.out.println("  Source operator uid: " + sourceUid);

            // 5g. Регистрируем как временное представление
            String viewName = "source_" + safeTableName;
            Schema schema = buildSchema(columns);
            tableEnv.createTemporaryView(viewName, data, schema);

            // 5h. Добавляем INSERT в StatementSet
            String insertSql = "INSERT INTO " + fullIcebergPath
                              + " SELECT * FROM " + viewName;
            System.out.println("  Insert SQL: " + insertSql);
                stmtSet.addInsertSql(insertSql);
                tablesAdded++;
                loadedTables.add(new TableLoadContext(tm, columns, orderByColumn));
            }

            // 6. Выполняем текущий батч как отдельный Flink job
            System.out.println();
            if (tablesAdded == 0) {
                System.out.println("=== Batch " + batchNumber + ": all tables skipped, nothing to execute ===");
            } else {
                System.out.println("=== Executing batch " + batchNumber + ": " + tablesAdded + " table(s) ===");
                try {
                    TableResult result = stmtSet.execute();
                    result.await();
                    System.out.println("=== Batch " + batchNumber + " completed successfully ===");
                } catch (Exception e) {
                    System.err.println("ERROR: Batch " + batchNumber + " failed: " + e.getMessage());
                    System.err.println("Continuing with next batch...");
                    processedTables = batchEnd;
                    batchNumber++;
                    if (processedTables < totalTables) {
                        System.out.println();
                        System.out.println("Preparing next batch...");
                        env = StreamExecutionEnvironment.getExecutionEnvironment();
                        env.setParallelism(parallelism);
                        
                        // Настройка S3 файловой системы для checkpoint storage
                        System.setProperty("fs.s3a.access.key", S3_ACCESS_KEY);
                        System.setProperty("fs.s3a.secret.key", S3_SECRET_KEY);
                        System.setProperty("fs.s3a.endpoint", S3_ENDPOINT);
                        System.setProperty("fs.s3a.path.style.access", "true");
                        System.setProperty("fs.s3a.connection.ssl.enabled", "false");
                        System.setProperty("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                        
                        // Настройка чекпоинтов для нового окружения
                        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
                        CheckpointConfig cpConfigBatch = env.getCheckpointConfig();
                        cpConfigBatch.setCheckpointStorage(FLINK_CHECKPOINTS_PATH);
                        cpConfigBatch.setMinPauseBetweenCheckpoints(10000);
                        cpConfigBatch.setCheckpointTimeout(600000);
                        cpConfigBatch.setMaxConcurrentCheckpoints(1);
                        cpConfigBatch.setTolerableCheckpointFailureNumber(3);
                        cpConfigBatch.setExternalizedCheckpointCleanup(
                            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
                        );
                        
                        tableEnv = StreamTableEnvironment.create(env);
                        
                        // Пересоздаём Iceberg каталог для нового окружения
                        tableEnv.executeSql(
                            "CREATE CATALOG iceberg WITH (" +
                            "  'type' = 'iceberg'," +
                            "  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog'," +
                            "  'uri' = '" + ICEBERG_CATALOG_URI + "'," +
                            "  'warehouse' = '" + ICEBERG_WAREHOUSE + "'," +
                            "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'," +
                            "  's3.endpoint' = '" + S3_ENDPOINT + "'," +
                            "  's3.path-style-access' = 'true'," +
                            "  'client.region' = '" + S3_REGION + "'," +
                            "  's3.access-key-id' = '" + S3_ACCESS_KEY + "'," +
                            "  's3.secret-access-key' = '" + S3_SECRET_KEY + "'" +
                            ")"
                        );
                    }
                    continue;
                }

                try {
                    runConsistencyChecks(fbUrl, fbUser, fbPass, icebergDb, loadedTables);
                } catch (ConsistencyCheckException e) {
                    System.err.println("ERROR: Consistency check failed for batch " + batchNumber + ": " + e.getMessage());
                    if (failOnConsistencyError) {
                        throw e;
                    }
                    System.err.println("Continuing with next batch...");
                }
            }

            processedTables = batchEnd;
            batchNumber++;
            
            // Если есть ещё таблицы, создаём новое окружение для следующего батча
            if (processedTables < totalTables) {
                System.out.println();
                System.out.println("Preparing next batch...");
                env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(parallelism);
                
                // Настройка S3 файловой системы для checkpoint storage
                System.setProperty("fs.s3a.access.key", S3_ACCESS_KEY);
                System.setProperty("fs.s3a.secret.key", S3_SECRET_KEY);
                System.setProperty("fs.s3a.endpoint", S3_ENDPOINT);
                System.setProperty("fs.s3a.path.style.access", "true");
                System.setProperty("fs.s3a.connection.ssl.enabled", "false");
                System.setProperty("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                
                // Настройка чекпоинтов для нового окружения
                env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
                CheckpointConfig cpConfigBatch = env.getCheckpointConfig();
                cpConfigBatch.setCheckpointStorage(FLINK_CHECKPOINTS_PATH);
                cpConfigBatch.setMinPauseBetweenCheckpoints(10000);
                cpConfigBatch.setCheckpointTimeout(600000);
                cpConfigBatch.setMaxConcurrentCheckpoints(1);
                cpConfigBatch.setTolerableCheckpointFailureNumber(3);
                cpConfigBatch.setExternalizedCheckpointCleanup(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
                );
                
                tableEnv = StreamTableEnvironment.create(env);
                
                // Пересоздаём Iceberg каталог для нового окружения
                tableEnv.executeSql(
                    "CREATE CATALOG iceberg WITH (" +
                    "  'type' = 'iceberg'," +
                    "  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog'," +
                    "  'uri' = '" + ICEBERG_CATALOG_URI + "'," +
                    "  'warehouse' = '" + ICEBERG_WAREHOUSE + "'," +
                    "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'," +
                    "  's3.endpoint' = '" + S3_ENDPOINT + "'," +
                    "  's3.path-style-access' = 'true'," +
                    "  'client.region' = '" + S3_REGION + "'," +
                    "  's3.access-key-id' = '" + S3_ACCESS_KEY + "'," +
                    "  's3.secret-access-key' = '" + S3_SECRET_KEY + "'" +
                    ")"
                );
            }
        }

        System.out.println();
        System.out.println("=== Transfer complete! Processed " + processedTables + " table(s) in " + (batchNumber - 1) + " batch(es) ===");
    }

    // ======================================================================
    // Маппинг таблиц
    // ======================================================================

    /**
     * Заменяет недопустимые символы в имени таблицы Iceberg.
     * Iceberg не поддерживает $ в именах таблиц.
     */
    static String sanitizeIcebergName(String name) {
        return name.replace("$", "_");
    }

    /**
     * Парсит список пар таблиц из аргументов --table / --tables.
     *
     * Форматы --tables:
     *   TABLE1,TABLE2             → Firebird UPPER → Iceberg lower
     *   FB_TABLE1:ice1,FB_TABLE2  → явный маппинг (или auto lowercase)
     */
    static List<TableMapping> parseTableMappings(String singleTable, String tablesArg) {
        List<TableMapping> mappings = new ArrayList<>();

        if (tablesArg != null && !tablesArg.isEmpty()) {
            for (String entry : tablesArg.split(",")) {
                entry = entry.trim();
                if (entry.isEmpty()) continue;
                if (entry.contains(":")) {
                    String[] parts = entry.split(":", 2);
                    mappings.add(new TableMapping(
                        parts[0].trim().toUpperCase(),
                        parts[1].trim().toLowerCase()
                    ));
                } else {
                    mappings.add(new TableMapping(
                        entry.toUpperCase(),
                        sanitizeIcebergName(entry.toLowerCase())
                    ));
                }
            }
        } else if (singleTable != null && !singleTable.isEmpty()) {
            mappings.add(new TableMapping(
                singleTable.toUpperCase(),
                sanitizeIcebergName(singleTable.toLowerCase())
            ));
        }

        return mappings;
    }

    /**
     * Пара: имя таблицы в Firebird → имя таблицы в Iceberg.
     */
    static class TableMapping implements java.io.Serializable {
        final String fbTable;      // UPPERCASE (Firebird)'format-version' = '2',
        final String icebergTable; // lowercase (Iceberg)

        TableMapping(String fbTable, String icebergTable) {
            this.fbTable = fbTable;
            this.icebergTable = icebergTable;
        }

        @Override
        public String toString() {
            return fbTable + " → " + icebergTable;
        }
    }

    // ======================================================================
    // Чтение метаданных
    // ======================================================================

    /**
     * Проверяет доступ к таблице, выполняя простой SELECT запрос.
     * @return true если доступ есть, false если нет доступа
     */
    static boolean checkTableAccess(String url, String user, String pass, String tableName) {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pass);
        props.setProperty("encoding", "UTF8");
        props.setProperty("authPlugins", "Srp256,Srp,Legacy_Auth");

        try {
            Class.forName("org.firebirdsql.jdbc.FBDriver");
            try (Connection conn = DriverManager.getConnection(url, props);
                 Statement stmt = conn.createStatement()) {
                // Пробуем выполнить простой SELECT с LIMIT 1
                String testQuery = "SELECT 1 FROM " + tableName + " ROWS 1";
                stmt.executeQuery(testQuery);
                return true;
            }
        } catch (SQLException e) {
            // Проверяем, является ли это ошибкой доступа
            String errorMsg = e.getMessage().toLowerCase();
            if (errorMsg.contains("no permission") || 
                errorMsg.contains("access denied") ||
                errorMsg.contains("permission") ||
                (e.getSQLState() != null && e.getSQLState().equals("28000"))) {
                return false;
            }
            // Другие ошибки (таблица не существует и т.д.) тоже считаем отсутствием доступа
            return false;
        } catch (Exception e) {
            // Любые другие исключения считаем отсутствием доступа
            return false;
        }
    }

    /**
     * Читает метаданные столбцов таблицы из Firebird через JDBC DatabaseMetaData.
     */
    static List<ColumnInfo> readTableMetadata(String url, String user, String pass, String tableName) throws Exception {
        List<ColumnInfo> columns = new ArrayList<>();
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pass);
        props.setProperty("encoding", "UTF8");
        props.setProperty("authPlugins", "Srp256,Srp,Legacy_Auth");

        Class.forName("org.firebirdsql.jdbc.FBDriver");

        try (Connection conn = DriverManager.getConnection(url, props)) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, null, tableName, null)) {
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME").trim();
                    int jdbcType = rs.getInt("DATA_TYPE");
                    String typeName = rs.getString("TYPE_NAME").trim().toUpperCase();
                    int precision = rs.getInt("COLUMN_SIZE");
                    int scale = rs.getInt("DECIMAL_DIGITS");
                    boolean nullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;

                    // Пропускаем ТОЛЬКО столбцы типа BLOB
                    if (jdbcType == java.sql.Types.BLOB || typeName.contains("BLOB")) {
                        System.out.println("  SKIPPING column '" + colName + "' (BLOB type: " + typeName + ")");
                        continue;
                    }

                    ColumnInfo col = new ColumnInfo();
                    col.name = colName.toLowerCase();
                    col.jdbcType = jdbcType;
                    col.typeName = typeName;
                    col.precision = precision;
                    col.scale = scale;
                    col.nullable = nullable;
                    col.flinkType = mapToFlinkType(jdbcType, precision, scale);
                    col.icebergType = mapToIcebergSqlType(jdbcType, precision, scale);
                    col.flinkDataType = mapToFlinkDataType(jdbcType, precision, scale, nullable);

                    // Предупреждение если тип не распознан → сохраняем как STRING
                    if ("STRING".equals(col.icebergType)
                            && jdbcType != java.sql.Types.CHAR
                            && jdbcType != java.sql.Types.VARCHAR
                            && jdbcType != java.sql.Types.LONGVARCHAR
                            && jdbcType != java.sql.Types.NCHAR
                            && jdbcType != java.sql.Types.NVARCHAR
                            && jdbcType != java.sql.Types.LONGNVARCHAR
                            && jdbcType != java.sql.Types.CLOB) {
                        System.out.println("  WARNING: Unrecognized JDBC type " + jdbcType +
                            " (" + typeName + ") for column '" + colName + "' → saved as STRING");
                    }

                    columns.add(col);
                }
            }
        }
        return columns;
    }

    // ======================================================================
    // Маппинг типов
    // ======================================================================

    /**
     * JDBC type → Flink TypeInformation (для DataStream API)
     */
    static TypeInformation<?> mapToFlinkType(int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                return Types.BOOLEAN;

            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
                return Types.SHORT;

            case java.sql.Types.INTEGER:
                return Types.INT;

            case java.sql.Types.BIGINT:
                return Types.LONG;

            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                return Types.FLOAT;

            case java.sql.Types.DOUBLE:
                return Types.DOUBLE;

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                return Types.BIG_DEC;

            case java.sql.Types.DATE:
                return Types.LOCAL_DATE;

            case java.sql.Types.TIME:
            case java.sql.Types.TIME_WITH_TIMEZONE:
                return Types.LOCAL_TIME;

            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                return Types.LOCAL_DATE_TIME;

            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB:
                return Types.PRIMITIVE_ARRAY(Types.BYTE);

            default:
                return Types.STRING;
        }
    }

    /**
     * JDBC type → Iceberg CREATE TABLE SQL тип
     */
    static String mapToIcebergSqlType(int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                return "BOOLEAN";

            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
                return "SMALLINT";

            case java.sql.Types.INTEGER:
                return "INT";

            case java.sql.Types.BIGINT:
                return "BIGINT";

            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                return "FLOAT";

            case java.sql.Types.DOUBLE:
                return "DOUBLE";

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                int p = precision > 0 ? precision : 38;
                int s = scale >= 0 ? scale : 0;
                return "DECIMAL(" + p + ", " + s + ")";

            case java.sql.Types.DATE:
                return "DATE";

            case java.sql.Types.TIME:
            case java.sql.Types.TIME_WITH_TIMEZONE:
                return "TIME";

            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP";

            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB:
                return "BYTES";

            default:
                return "STRING";
        }
    }

    /**
     * JDBC type → Flink DataType (для Schema builder)
     */
    static DataType mapToFlinkDataType(int jdbcType, int precision, int scale, boolean nullable) {
        DataType base;
        switch (jdbcType) {
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                base = DataTypes.BOOLEAN(); break;
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
                base = DataTypes.SMALLINT(); break;
            case java.sql.Types.INTEGER:
                base = DataTypes.INT(); break;
            case java.sql.Types.BIGINT:
                base = DataTypes.BIGINT(); break;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                base = DataTypes.FLOAT(); break;
            case java.sql.Types.DOUBLE:
                base = DataTypes.DOUBLE(); break;
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                int p = precision > 0 ? precision : 38;
                int s = scale >= 0 ? scale : 0;
                base = DataTypes.DECIMAL(p, s); break;
            case java.sql.Types.DATE:
                base = DataTypes.DATE(); break;
            case java.sql.Types.TIME:
            case java.sql.Types.TIME_WITH_TIMEZONE:
                base = DataTypes.TIME(); break;
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                base = DataTypes.TIMESTAMP(6); break;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB:
                base = DataTypes.BYTES(); break;
            default:
                base = DataTypes.STRING(); break;
        }
        return nullable ? base.nullable() : base.notNull();
    }

    // ======================================================================
    // Генерация SQL и схемы
    // ======================================================================

    /**
     * Экранирует имя столбца обратными кавычками для безопасного использования в Flink SQL.
     */
    static String escapeColumnName(String columnName) {
        return "`" + columnName + "`";
    }

    /**
     * Формирует имена технических столбцов с учётом конфликтов с исходными столбцами таблицы.
     * Если имя технического столбца совпадает с именем столбца из Firebird,
     * добавляется префикс "__".
     */
    static String[] resolveTechColumnNames(List<ColumnInfo> columns) {
        java.util.Set<String> sourceNames = new java.util.HashSet<>();
        for (ColumnInfo col : columns) {
            sourceNames.add(col.name.toLowerCase());
        }

        String[] resolved = new String[TECH_COL_BASE_NAMES.length];
        for (int i = 0; i < TECH_COL_BASE_NAMES.length; i++) {
            String name = TECH_COL_BASE_NAMES[i];
            if (sourceNames.contains(name)) {
                resolved[i] = "__" + name;
                System.out.println("  CONFLICT: column '" + name + "' exists in source, tech column renamed to '__" + name + "'");
            } else {
                resolved[i] = name;
            }
        }
        return resolved;
    }

    /**
     * Генерирует CREATE TABLE SQL для Iceberg.
     */
    static String buildCreateTableSql(String db, String table, List<ColumnInfo> columns) {
        String[] techNames = resolveTechColumnNames(columns);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS iceberg.").append(db).append(".`").append(table).append("` (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(escapeColumnName(columns.get(i).name)).append(" ").append(columns.get(i).icebergType);
        }
        // Технические поля (имена зависят от наличия конфликтов)
        String[] techTypes = {"TIMESTAMP NOT NULL", "TIMESTAMP", "BIGINT", "STRING", "BIGINT", "BIGINT", "STRING", "TIMESTAMP", "TIMESTAMP", "STRING"};
        for (int i = 0; i < techNames.length; i++) {
            sb.append(", ").append(escapeColumnName(techNames[i])).append(" ").append(techTypes[i]);
        }
        sb.append(") WITH (");
        sb.append("  'format-version' = '2',");
        sb.append("  'partitioning' = 'days(").append(techNames[0]).append(")'");
        sb.append(")");
        return sb.toString();
    }

    /**
     * Строит RowTypeInfo для DataStream.
     */
    static RowTypeInfo buildRowTypeInfo(List<ColumnInfo> columns) {
        String[] techNames = resolveTechColumnNames(columns);

        int total = columns.size() + TECH_COLS_COUNT;
        TypeInformation<?>[] types = new TypeInformation[total];
        String[] names = new String[total];
        for (int i = 0; i < columns.size(); i++) {
            types[i] = columns.get(i).flinkType;
            names[i] = columns.get(i).name;
        }
        // Технические поля (имена зависят от наличия конфликтов)
        TypeInformation<?>[] techTypes = {
            Types.LOCAL_DATE_TIME, Types.LOCAL_DATE_TIME, Types.LONG,
            Types.STRING, Types.LONG, Types.LONG,
            Types.STRING, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE_TIME, Types.STRING
        };
        int o = columns.size();
        for (int i = 0; i < techNames.length; i++) {
            types[o + i] = techTypes[i];
            names[o + i] = techNames[i];
        }
        return new RowTypeInfo(types, names);
    }

    /**
     * Строит Schema для Table API.
     */
    static Schema buildSchema(List<ColumnInfo> columns) {
        String[] techNames = resolveTechColumnNames(columns);

        Schema.Builder builder = Schema.newBuilder();
        for (ColumnInfo col : columns) {
            builder.column(col.name, col.flinkDataType);
        }
        // Технические поля (имена зависят от наличия конфликтов)
        org.apache.flink.table.types.DataType[] techDataTypes = {
            DataTypes.TIMESTAMP(6).notNull(), DataTypes.TIMESTAMP(6).nullable(),
            DataTypes.BIGINT().nullable(), DataTypes.STRING().nullable(),
            DataTypes.BIGINT().nullable(), DataTypes.BIGINT().nullable(),
            DataTypes.STRING().nullable(), DataTypes.TIMESTAMP(6).nullable(),
            DataTypes.TIMESTAMP(6).nullable(), DataTypes.STRING().nullable()
        };
        for (int i = 0; i < techNames.length; i++) {
            builder.column(techNames[i], techDataTypes[i]);
        }
        return builder.build();
    }

    // ======================================================================
    // Dynamic Firebird Source
    // ======================================================================

    /**
     * Универсальный Flink Source с checkpointed offset: читает любую таблицу
     * из Firebird на основе метаданных столбцов.
     *
     * При checkpoint сохраняет количество обработанных строк (offset).
     * При восстановлении из checkpoint/savepoint продолжает чтение с сохранённой
     * позиции, используя Firebird SKIP N синтаксис и ORDER BY для
     * детерминированного порядка строк.
     */
    public static class FirebirdDynamicSource extends RichSourceFunction<Row>
            implements CheckpointedFunction {

        private final String url;
        private final String user;
        private final String password;
        private final String tableName;
        private final List<ColumnInfo> columns;
        private final String orderByColumn;
        private final int fetchSize;
        private volatile boolean running = true;

        // === Checkpoint state ===
        private transient ListState<Long> offsetState;
        private volatile long rowsProcessed = 0;

        public FirebirdDynamicSource(String url, String user, String password,
                                     String tableName, List<ColumnInfo> columns,
                                     String orderByColumn, int fetchSize) {
            this.url = url;
            this.user = user;
            this.password = password;
            this.tableName = tableName;
            this.columns = columns;
            this.orderByColumn = orderByColumn;
            this.fetchSize = fetchSize;
        }

        // ---------- CheckpointedFunction ----------

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            offsetState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("source-offset", Long.class)
            );

            if (context.isRestored()) {
                for (Long offset : offsetState.get()) {
                    rowsProcessed = offset;
                }
                System.out.println("★ RESTORED offset from checkpoint: rowsProcessed = " + rowsProcessed);
            } else {
                System.out.println("★ Fresh start: rowsProcessed = 0");
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            offsetState.clear();
            offsetState.add(rowsProcessed);
            System.out.println("★ Checkpoint " + context.getCheckpointId()
                + ": saved offset = " + rowsProcessed);
        }

        // ---------- SourceFunction ----------

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("encoding", "UTF8");
            props.setProperty("authPlugins", "Srp256,Srp,Legacy_Auth");

            Class.forName("org.firebirdsql.jdbc.FBDriver");

            // Строим SELECT с SKIP и ORDER BY для checkpoint offset
            long skipRows = rowsProcessed;

            StringBuilder query = new StringBuilder("SELECT ");
            if (skipRows > 0) {
                // Firebird синтаксис: SELECT SKIP <n> col1, col2, ... FROM ...
                query.append("SKIP ").append(skipRows).append(" ");
            }
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) query.append(", ");
                query.append(columns.get(i).name.toUpperCase());
            }
            query.append(" FROM ").append(tableName);
            query.append(" ORDER BY ").append(orderByColumn);

            System.out.println("Executing query (offset=" + skipRows + "): " + query);

            // Checkpoint lock для синхронизации emit + offset
            final Object lock = ctx.getCheckpointLock();

            try (Connection conn = DriverManager.getConnection(url, props);
                 Statement stmt = conn.createStatement()) {
                stmt.setFetchSize(fetchSize);
                try (ResultSet rs = stmt.executeQuery(query.toString())) {
                    long emittedInThisRun = 0;
                    int srcCols = columns.size();
                    while (rs.next() && running) {
                        Row row = new Row(srcCols + TECH_COLS_COUNT);
                        Object[] sourceValues = new Object[srcCols];
                        // Исходные столбцы из Firebird
                        for (int i = 0; i < srcCols; i++) {
                            Object value = readColumn(rs, i + 1, columns.get(i).jdbcType);
                            sourceValues[i] = value;
                            row.setField(i, value);
                        }
                        // Технические поля
                        int o = srcCols;
                        row.setField(o,     LocalDateTime.now());  // load_dttm
                        row.setField(o + 1, null);                 // load_dttm_tz
                        row.setField(o + 2, null);                 // load_id
                        row.setField(o + 3, "INITIAL");            // op
                        row.setField(o + 4, null);                 // ts_ms
                        row.setField(o + 5, null);                 // source_ts_ms
                        row.setField(o + 6, "mis");                // src_system_code
                        row.setField(o + 7, null);                 // extract_dttm
                        row.setField(o + 8, null);                 // src_chng_dttm
                        row.setField(o + 9, computeRowHash(sourceValues)); // row_hash

                        // Атомарно: emit + increment offset (под checkpoint lock)
                        synchronized (lock) {
                            ctx.collect(row);
                            rowsProcessed++;
                        }

                        emittedInThisRun++;
                        if (emittedInThisRun % 10000 == 0) {
                            System.out.println("Progress: " + emittedInThisRun
                                + " emitted (total offset: " + rowsProcessed + ") from " + tableName);
                        }
                    }
                    System.out.println("Total emitted in this run: " + emittedInThisRun
                        + " (total offset: " + rowsProcessed + ") from " + tableName);
                }
            } catch (SQLException e) {
                // Проверяем, является ли это ошибкой доступа
                String errorMsg = e.getMessage().toLowerCase();
                if (errorMsg.contains("no permission") || 
                    errorMsg.contains("access denied") ||
                    errorMsg.contains("permission") ||
                    (e.getSQLState() != null && e.getSQLState().equals("28000"))) {
                    System.err.println("ERROR: No SELECT access to table '" + tableName + "'. " +
                                     "Source will terminate gracefully. Error: " + e.getMessage());
                    // Завершаем источник корректно вместо падения
                    running = false;
                    return;
                }
                // Другие SQL ошибки пробрасываем дальше
                throw e;
            }
        }

        /**
         * Читает значение столбца из ResultSet с учётом JDBC типа.
         */
        private Object readColumn(ResultSet rs, int colIndex, int jdbcType) throws SQLException {
            Object value = null;
            switch (jdbcType) {
                case java.sql.Types.BIT:
                case java.sql.Types.BOOLEAN:
                    value = rs.getBoolean(colIndex);
                    break;
                case java.sql.Types.TINYINT:
                case java.sql.Types.SMALLINT:
                    value = rs.getShort(colIndex);
                    break;
                case java.sql.Types.INTEGER:
                    value = rs.getInt(colIndex);
                    break;
                case java.sql.Types.BIGINT:
                    value = rs.getLong(colIndex);
                    break;
                case java.sql.Types.FLOAT:
                case java.sql.Types.REAL:
                    value = rs.getFloat(colIndex);
                    break;
                case java.sql.Types.DOUBLE:
                    value = rs.getDouble(colIndex);
                    break;
                case java.sql.Types.NUMERIC:
                case java.sql.Types.DECIMAL:
                    value = rs.getBigDecimal(colIndex);
                    break;
                case java.sql.Types.DATE:
                    Date d = rs.getDate(colIndex);
                    value = d != null ? d.toLocalDate() : null;
                    break;
                case java.sql.Types.TIME:
                case java.sql.Types.TIME_WITH_TIMEZONE:
                    Time t = rs.getTime(colIndex);
                    value = t != null ? t.toLocalTime() : null;
                    break;
                case java.sql.Types.TIMESTAMP:
                case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                    Timestamp ts = rs.getTimestamp(colIndex);
                    value = ts != null ? ts.toLocalDateTime() : null;
                    break;
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.LONGVARBINARY:
                case java.sql.Types.BLOB:
                    value = rs.getBytes(colIndex);
                    break;
                default:
                    value = rs.getString(colIndex);
                    break;
            }
            if (rs.wasNull()) {
                value = null;
            }
            return value;
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // ======================================================================
    // Контроль консистентности
    // ======================================================================

    static class TableLoadContext {
        final TableMapping mapping;
        final List<ColumnInfo> columns;
        final String orderByColumn;

        TableLoadContext(TableMapping mapping, List<ColumnInfo> columns, String orderByColumn) {
            this.mapping = mapping;
            this.columns = columns;
            this.orderByColumn = orderByColumn;
        }
    }

    static class TableHashResult {
        final long rowCount;
        final String hash;

        TableHashResult(long rowCount, String hash) {
            this.rowCount = rowCount;
            this.hash = hash;
        }
    }

    static class ConsistencyCheckException extends RuntimeException {
        ConsistencyCheckException(String message) {
            super(message);
        }
    }

    /**
     * Коммутативный агрегатор хэшей строк (устойчив к порядку строк).
     * Нужен, чтобы в стриминговом режиме не делать ORDER BY всей таблицы.
    */
    static class RowHashAccumulator {
        long sumA = 0L;
        long sumB = 0L;
        long xorA = 0L;
        long xorB = 0L;

        void addRowHash(String rowHashHex64) {
            if (rowHashHex64 == null || rowHashHex64.length() < 32) {
                throw new IllegalArgumentException("Invalid row hash: " + rowHashHex64);
            }
            long partA = Long.parseUnsignedLong(rowHashHex64.substring(0, 16), 16);
            long partB = Long.parseUnsignedLong(rowHashHex64.substring(16, 32), 16);

            sumA += partA;
            sumB += partB;
            xorA ^= partA;
            xorB ^= partB;
        }

        String digest() {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(Long.toHexString(sumA).getBytes(StandardCharsets.UTF_8));
                md.update((byte) '|');
                md.update(Long.toHexString(sumB).getBytes(StandardCharsets.UTF_8));
                md.update((byte) '|');
                md.update(Long.toHexString(xorA).getBytes(StandardCharsets.UTF_8));
                md.update((byte) '|');
                md.update(Long.toHexString(xorB).getBytes(StandardCharsets.UTF_8));
                return bytesToHex(md.digest());
            } catch (Exception e) {
                throw new RuntimeException("Failed to finalize aggregated hash", e);
            }
        }
    }

    static void runConsistencyChecks(String fbUrl,
                                     String fbUser,
                                     String fbPass,
                                     String icebergDb,
                                     List<TableLoadContext> loadedTables) throws Exception {
        if (loadedTables.isEmpty()) {
            return;
        }

        // Для проверки консистентности используем отдельный batch TableEnvironment,
        // чтобы SELECT из Iceberg завершался конечным job и не "висел" как streaming query.
        TableEnvironment batchTableEnv = createBatchTableEnvironmentForConsistency(icebergDb);

        System.out.println();
        System.out.println("=== Consistency check (Firebird vs Iceberg) ===");
        for (TableLoadContext ctx : loadedTables) {
            TableHashResult fb = computeFirebirdTableHash(fbUrl, fbUser, fbPass, ctx.mapping.fbTable, ctx.columns);
            TableHashResult ib = computeIcebergTableHash(batchTableEnv, icebergDb, ctx.mapping.icebergTable, ctx.columns);

            boolean countMatch = fb.rowCount == ib.rowCount;
            boolean hashMatch = fb.hash.equals(ib.hash);
            if (countMatch && hashMatch) {
                System.out.println("  OK: " + ctx.mapping.fbTable + " -> " + ctx.mapping.icebergTable
                    + " (rows=" + fb.rowCount + ", hash=" + fb.hash + ")");
            } else {
                String error = "CONSISTENCY CHECK FAILED for table " + ctx.mapping.fbTable
                    + " -> " + ctx.mapping.icebergTable
                    + " | Firebird(rows=" + fb.rowCount + ", hash=" + fb.hash + ")"
                    + " vs Iceberg(rows=" + ib.rowCount + ", hash=" + ib.hash + ")";
                System.err.println("  ERROR: " + error);
                throw new ConsistencyCheckException(error);
            }
        }
    }

    static TableEnvironment createBatchTableEnvironmentForConsistency(String icebergDb) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql(
            "CREATE CATALOG iceberg WITH (" +
            "  'type' = 'iceberg'," +
            "  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog'," +
            "  'uri' = '" + ICEBERG_CATALOG_URI + "'," +
            "  'warehouse' = '" + ICEBERG_WAREHOUSE + "'," +
            "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'," +
            "  's3.endpoint' = '" + S3_ENDPOINT + "'," +
            "  's3.path-style-access' = 'true'," +
            "  'client.region' = '" + S3_REGION + "'," +
            "  's3.access-key-id' = '" + S3_ACCESS_KEY + "'," +
            "  's3.secret-access-key' = '" + S3_SECRET_KEY + "'" +
            ")"
        );
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg." + icebergDb);
        return tEnv;
    }

    static TableHashResult computeFirebirdTableHash(String url, String user, String pass,
                                                    String tableName, List<ColumnInfo> columns) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pass);
        props.setProperty("encoding", "UTF8");
        props.setProperty("authPlugins", "Srp256,Srp,Legacy_Auth");

        Class.forName("org.firebirdsql.jdbc.FBDriver");

        StringBuilder selectList = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                selectList.append(", ");
            }
            String colUpper = columns.get(i).name.toUpperCase();
            selectList.append(colUpper);
        }

        String query = "SELECT " + selectList + " FROM " + tableName;

        long rows = 0;
        RowHashAccumulator accumulator = new RowHashAccumulator();

        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                Object[] values = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    values[i] = readColumnValue(rs, i + 1, columns.get(i).jdbcType);
                }
                String rowHash = computeRowHash(values);
                accumulator.addRowHash(rowHash);
                rows++;
            }
        }

        return new TableHashResult(rows, accumulator.digest());
    }

    static TableHashResult computeIcebergTableHash(TableEnvironment tableEnv,
                                                   String icebergDb,
                                                   String icebergTable,
                                                   List<ColumnInfo> columns) throws Exception {
        String[] techNames = resolveTechColumnNames(columns);
        String techRowHashName = techNames[techNames.length - 1];

        String sql = "SELECT " + escapeColumnName(techRowHashName)
            + " FROM iceberg." + icebergDb + ".`" + icebergTable + "`";

        long rows = 0;
        RowHashAccumulator accumulator = new RowHashAccumulator();
        TableResult result = tableEnv.executeSql(sql);
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                Object rowHashObj = row.getField(0);
                if (rowHashObj == null) {
                    throw new IllegalStateException("Found NULL row_hash in Iceberg table " + icebergTable);
                }
                accumulator.addRowHash(rowHashObj.toString());
                rows++;
            }
        }

        return new TableHashResult(rows, accumulator.digest());
    }

    static Object readColumnValue(ResultSet rs, int colIndex, int jdbcType) throws SQLException {
        Object value;
        switch (jdbcType) {
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                value = rs.getBoolean(colIndex);
                break;
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
                value = rs.getShort(colIndex);
                break;
            case java.sql.Types.INTEGER:
                value = rs.getInt(colIndex);
                break;
            case java.sql.Types.BIGINT:
                value = rs.getLong(colIndex);
                break;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                value = rs.getFloat(colIndex);
                break;
            case java.sql.Types.DOUBLE:
                value = rs.getDouble(colIndex);
                break;
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                value = rs.getBigDecimal(colIndex);
                break;
            case java.sql.Types.DATE:
                Date d = rs.getDate(colIndex);
                value = d != null ? d.toLocalDate() : null;
                break;
            case java.sql.Types.TIME:
            case java.sql.Types.TIME_WITH_TIMEZONE:
                Time t = rs.getTime(colIndex);
                value = t != null ? t.toLocalTime() : null;
                break;
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                Timestamp ts = rs.getTimestamp(colIndex);
                value = ts != null ? ts.toLocalDateTime() : null;
                break;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB:
                value = rs.getBytes(colIndex);
                break;
            default:
                value = rs.getString(colIndex);
                break;
        }
        if (rs.wasNull()) {
            value = null;
        }
        return value;
    }

    static String computeRowHash(Object[] values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (Object value : values) {
                String serialized;
                if (value == null) {
                    serialized = "<NULL>";
                } else if (value instanceof byte[]) {
                    serialized = "BYTES:" + Base64.getEncoder().encodeToString((byte[]) value);
                } else {
                    serialized = value.getClass().getName() + ":" + value;
                }
                byte[] bytes = serialized.getBytes(StandardCharsets.UTF_8);
                digest.update(Integer.toString(bytes.length).getBytes(StandardCharsets.UTF_8));
                digest.update((byte) ':');
                digest.update(bytes);
                digest.update((byte) '|');
            }
            return bytesToHex(digest.digest());
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute row hash", e);
        }
    }

    static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // ======================================================================
    // Утилиты
    // ======================================================================

    /**
     * Информация о столбце.
     */
    static class ColumnInfo implements java.io.Serializable {
        String name;
        int jdbcType;
        String typeName;
        int precision;
        int scale;
        boolean nullable;
        TypeInformation<?> flinkType;
        String icebergType;
        DataType flinkDataType;

        @Override
        public String toString() {
            return name + " " + icebergType + " (jdbc=" + jdbcType + ", fb=" + typeName + ")";
        }
    }

    /**
     * Парсит аргумент командной строки.
     */
    static String getArg(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
