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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.flink.table.api.StatementSet;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
    private static final String DEFAULT_ICEBERG_DB = "rzdm";
    private static final String DEFAULT_MODE = "append";
    private static final int DEFAULT_FETCH_SIZE = 10000;
    private static final int TECH_COLS_COUNT = 9;

    // === Iceberg catalog settings ===
    private static final String ICEBERG_CATALOG_URI = "http://iceberg-rest:8181";
    private static final String ICEBERG_WAREHOUSE = "s3://iceberg/";
    private static final String S3_ENDPOINT = "http://minio-svc:9000";
    private static final String S3_REGION = "ru-central1";
    private static final String S3_ACCESS_KEY = "minioadmin";
    private static final String S3_SECRET_KEY = "Q1w2e3r+";

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

        // 3. Настройка S3 файловой системы для checkpoint storage
        System.setProperty("fs.s3a.access.key", S3_ACCESS_KEY);
        System.setProperty("fs.s3a.secret.key", S3_SECRET_KEY);
        System.setProperty("fs.s3a.endpoint", S3_ENDPOINT);
        System.setProperty("fs.s3a.path.style.access", "true");
        System.setProperty("fs.s3a.connection.ssl.enabled", "false");
        System.setProperty("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // 4. Создаём Flink окружение
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Настройка чекпоинтов → S3 (MinIO)
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setCheckpointStorage("s3://flink/checkpoints");
        cpConfig.setMinPauseBetweenCheckpoints(30000);
        cpConfig.setCheckpointTimeout(600000);
        cpConfig.setMaxConcurrentCheckpoints(1);
        cpConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        System.out.println("Checkpointing: EXACTLY_ONCE, interval=60s, storage=s3://flink/checkpoints");

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

        // 5. Для каждой таблицы: читаем метаданные, создаём Iceberg таблицу,
        //    создаём Source DataStream и регистрируем как view
        StatementSet stmtSet = tableEnv.createStatementSet();

        for (TableMapping tm : tableMappings) {
            System.out.println();
            System.out.println("--- Configuring table: " + tm.fbTable + " → " + tm.icebergTable + " ---");

            // 5a. Читаем метаданные
            List<ColumnInfo> columns = readTableMetadata(fbUrl, fbUser, fbPass, tm.fbTable);
            if (columns.isEmpty()) {
                System.err.println("ERROR: Table '" + tm.fbTable + "' not found or has no columns! Skipping.");
                continue;
            }

            System.out.println("  Columns: " + columns.size());
            for (ColumnInfo col : columns) {
                System.out.println("    " + col.name + " : " + col.icebergType + " (JDBC type: " + col.jdbcType + ")");
            }

            // 5b. Определяем ORDER BY (глобальный или первый столбец таблицы)
            String orderByColumn = (orderBy != null && !orderBy.isEmpty())
                    ? orderBy.toUpperCase()
                    : columns.get(0).name.toUpperCase();
            System.out.println("  Order by: " + orderByColumn);

            // 5c. Создаём/пересоздаём Iceberg таблицу
            if ("replace".equalsIgnoreCase(mode)) {
                tableEnv.executeSql("DROP TABLE IF EXISTS iceberg." + icebergDb + "." + tm.icebergTable);
            }
            String createSql = buildCreateTableSql(icebergDb, tm.icebergTable, columns);
            System.out.println("  Creating Iceberg table: " + createSql);
            tableEnv.executeSql(createSql);

            // 5d. Создаём Source DataStream с уникальным uid для checkpoint state
            RowTypeInfo rowTypeInfo = buildRowTypeInfo(columns);
            String sourceUid = "source-" + tm.fbTable.toLowerCase();
            DataStream<Row> data = env
                .addSource(
                    new FirebirdDynamicSource(fbUrl, fbUser, fbPass, tm.fbTable, columns, orderByColumn),
                    "firebird-" + tm.fbTable.toLowerCase(),
                    rowTypeInfo
                )
                .uid(sourceUid);

            System.out.println("  Source operator uid: " + sourceUid);

            // 5e. Регистрируем как временное представление
            String viewName = "source_" + tm.fbTable.toLowerCase();
            Schema schema = buildSchema(columns);
            tableEnv.createTemporaryView(viewName, data, schema);

            // 5f. Добавляем INSERT в StatementSet
            String insertSql = "INSERT INTO iceberg." + icebergDb + "." + tm.icebergTable
                              + " SELECT * FROM " + viewName;
            System.out.println("  Insert SQL: " + insertSql);
            stmtSet.addInsertSql(insertSql);
        }

        // 6. Выполняем все INSERT как единый Flink job
        System.out.println();
        System.out.println("=== Executing " + tableMappings.size() + " table(s) as a single Flink job ===");

        TableResult result = stmtSet.execute();
        try {
            result.await();
        } catch (Exception e) {
            System.out.println("Job submitted (await not supported in this mode): " + e.getMessage());
        }

        System.out.println("=== Transfer complete! ===");
    }

    // ======================================================================
    // Маппинг таблиц
    // ======================================================================

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
                        entry.toLowerCase()
                    ));
                }
            }
        } else if (singleTable != null && !singleTable.isEmpty()) {
            mappings.add(new TableMapping(
                singleTable.toUpperCase(),
                singleTable.toLowerCase()
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
                    String typeName = rs.getString("TYPE_NAME").trim();
                    int precision = rs.getInt("COLUMN_SIZE");
                    int scale = rs.getInt("DECIMAL_DIGITS");
                    boolean nullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;

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
                return "BINARY";

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
     * Генерирует CREATE TABLE SQL для Iceberg.
     */
    static String buildCreateTableSql(String db, String table, List<ColumnInfo> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS iceberg.").append(db).append(".").append(table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns.get(i).name).append(" ").append(columns.get(i).icebergType);
        }
        // Технические поля
        sb.append(", load_dttm TIMESTAMP NOT NULL");
        sb.append(", load_dttm_tz TIMESTAMP");
        sb.append(", load_id BIGINT");
        sb.append(", op STRING");
        sb.append(", ts_ms BIGINT");
        sb.append(", source_ts_ms BIGINT");
        sb.append(", src_system_code STRING");
        sb.append(", extract_dttm TIMESTAMP");
        sb.append(", src_chng_dttm TIMESTAMP");
        sb.append(") WITH (");
        sb.append("  'format-version' = '2',");
        sb.append("  'partitioning' = 'days(load_dttm)'");
        sb.append(")");
        return sb.toString();
    }

    /**
     * Строит RowTypeInfo для DataStream.
     */
    static RowTypeInfo buildRowTypeInfo(List<ColumnInfo> columns) {
        int total = columns.size() + TECH_COLS_COUNT;
        TypeInformation<?>[] types = new TypeInformation[total];
        String[] names = new String[total];
        for (int i = 0; i < columns.size(); i++) {
            types[i] = columns.get(i).flinkType;
            names[i] = columns.get(i).name;
        }
        // Технические поля
        int o = columns.size();
        types[o]     = Types.LOCAL_DATE_TIME; names[o]     = "load_dttm";
        types[o + 1] = Types.LOCAL_DATE_TIME; names[o + 1] = "load_dttm_tz";
        types[o + 2] = Types.LONG;            names[o + 2] = "load_id";
        types[o + 3] = Types.STRING;          names[o + 3] = "op";
        types[o + 4] = Types.LONG;            names[o + 4] = "ts_ms";
        types[o + 5] = Types.LONG;            names[o + 5] = "source_ts_ms";
        types[o + 6] = Types.STRING;          names[o + 6] = "src_system_code";
        types[o + 7] = Types.LOCAL_DATE_TIME; names[o + 7] = "extract_dttm";
        types[o + 8] = Types.LOCAL_DATE_TIME; names[o + 8] = "src_chng_dttm";
        return new RowTypeInfo(types, names);
    }

    /**
     * Строит Schema для Table API.
     */
    static Schema buildSchema(List<ColumnInfo> columns) {
        Schema.Builder builder = Schema.newBuilder();
        for (ColumnInfo col : columns) {
            builder.column(col.name, col.flinkDataType);
        }
        // Технические поля
        builder.column("load_dttm",        DataTypes.TIMESTAMP(6).notNull());
        builder.column("load_dttm_tz",     DataTypes.TIMESTAMP(6).nullable());
        builder.column("load_id",          DataTypes.BIGINT().nullable());
        builder.column("op",               DataTypes.STRING().nullable());
        builder.column("ts_ms",            DataTypes.BIGINT().nullable());
        builder.column("source_ts_ms",     DataTypes.BIGINT().nullable());
        builder.column("src_system_code",  DataTypes.STRING().nullable());
        builder.column("extract_dttm",     DataTypes.TIMESTAMP(6).nullable());
        builder.column("src_chng_dttm",    DataTypes.TIMESTAMP(6).nullable());
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
        private volatile boolean running = true;

        // === Checkpoint state ===
        private transient ListState<Long> offsetState;
        private volatile long rowsProcessed = 0;

        public FirebirdDynamicSource(String url, String user, String password,
                                     String tableName, List<ColumnInfo> columns,
                                     String orderByColumn) {
            this.url = url;
            this.user = user;
            this.password = password;
            this.tableName = tableName;
            this.columns = columns;
            this.orderByColumn = orderByColumn;
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
                stmt.setFetchSize(DEFAULT_FETCH_SIZE);
                try (ResultSet rs = stmt.executeQuery(query.toString())) {
                    long emittedInThisRun = 0;
                    int srcCols = columns.size();
                    while (rs.next() && running) {
                        Row row = new Row(srcCols + TECH_COLS_COUNT);
                        // Исходные столбцы из Firebird
                        for (int i = 0; i < srcCols; i++) {
                            row.setField(i, readColumn(rs, i + 1, columns.get(i).jdbcType));
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
