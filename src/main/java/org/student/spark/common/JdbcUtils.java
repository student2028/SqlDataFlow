package org.student.spark.common;

import com.typesafe.config.Config;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Map;
import scala.collection.Seq;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.student.spark.common.CommonUtils.*;
import static org.student.spark.common.Constants.REGEXP_LENGTH;
import static org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType;
import static org.apache.spark.sql.functions.col;

public class JdbcUtils {
    private static final String JDBC_DB2 = "jdbc:db2";

    final static Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    public static void executeSql(Config config, String tag, String sql) {
        int maxRetry = 5;
        int times = 1;
        boolean hasException;
        do {
            logger.info("{} time to Execute sql  :\n {}", times, sql);
            try (Connection conn = JdbcConnectionPool.getPool(config, tag).getConnection(tag);
                 Statement statement = conn.createStatement()
            ) {
                statement.execute(sql);
                hasException = false;
            } catch (Exception e) {
                logger.error(e.getMessage());
                hasException = true;
                if (times == maxRetry - 1) {
                    throw new DataFlowException(e);
                }
                if (times < maxRetry - 2) {
                    try {
                        Thread.sleep((long) (1000 * Math.pow(2, times)));
                    } catch (InterruptedException ex) {
                        logger.info(ex.getMessage());
                    }
                }
            }
            times++;
        } while (hasException && times < maxRetry);
    }


    public static void truncateTable(Config config, String tag, JdbcOptionsInWrite options) {
        JdbcDialect dialect = JdbcDialects.get(options.url());
        try (Connection conn = JdbcConnectionPool.getPool(config, tag).getConnection(tag);
             Statement statement = conn.createStatement()) {
            statement.setQueryTimeout(options.queryTimeout());
            String truncateQuery;
            if (options.isCascadeTruncate().isDefined()) {
                truncateQuery = dialect.getTruncateQuery(options.table(), options.isCascadeTruncate());
            } else {
                truncateQuery = dialect.getTruncateQuery(options.table());
            }
            if (!truncateQuery.contains("IMMEDIATE") && options.url().startsWith(JDBC_DB2)) {
                truncateQuery += " IMMEDIATE";
            }
            logger.info("Execute truncate query:{}", truncateQuery);
            statement.executeUpdate(truncateQuery);
        } catch (SQLException ex) {
            printSQLException(ex);
            throw new DataFlowException(ex);
        }
    }

    public static String getInsertStatement(String table, StructType rddSchema, JdbcDialect dialect) {
        String columns = Arrays.stream(rddSchema.fields())
                .map(x ->
                        dialect.toString().contains(".DB2Dialect") ?
                                dialect.quoteIdentifier(x.name())
                                : x.name()
                ).collect(Collectors.joining(","));

        String placeholders = Arrays.stream(rddSchema.fields()).map(x -> "?").collect(Collectors.joining(","));
        String insertStmt = String.format("INSERT INTO %s (%s) VALUES (%s)", table.toUpperCase(),
                columns.toUpperCase(), placeholders.toUpperCase());
        logger.info("insert query sql is\n {}", insertStmt);
        return insertStmt;
    }

    public static String getDeleteStatement(String table, StructType rddSchema, JdbcDialect dialect) {
        String columns = Arrays.stream(rddSchema.fields()).map(x ->
                (dialect.toString().contains(".DB2Dialect") ?
                        dialect.quoteIdentifier(x.name()) :
                        x.name())
                        + "=?").collect(Collectors.joining(" AND "));
        String deleteStmt = String.format("DELETE FROM %s WHERE %s", table.toUpperCase(), columns.toUpperCase());
        logger.info("update query sql is\n {}", deleteStmt);
        return deleteStmt;
    }


    public static String getMergeStatement(
            String table,
            StructType rddSchema,
            Seq<String> priKeys,
            JdbcDialect dialect) {
        List<String> fullColsList = Arrays.stream(rddSchema.fields()).map(x -> dialect.quoteIdentifier(x.name())).collect(Collectors.toList());
        Seq<String> fullCols = scala.collection.JavaConverters.asScalaBuffer(fullColsList).seq();
        Seq<String> priCols = scala.collection.JavaConverters.asScalaBufferConverter(
                scala.collection.JavaConverters.seqAsJavaListConverter(priKeys).asJava().stream().map(dialect::quoteIdentifier).collect(Collectors.toList())
        ).asScala().seq();
        Seq<String> nrmCols = fullCols.diff(priCols);
        String fullPart = scala.collection.JavaConverters.seqAsJavaListConverter(fullCols).asJava().stream()
                .map(c -> dialect.quoteIdentifier("SRC")
                        + "." + c.toUpperCase()
                ).collect(Collectors.joining(","));
        String priPart = scala.collection.JavaConverters.seqAsJavaListConverter(priCols).asJava().stream()
                .map(c -> dialect.quoteIdentifier("TGT")
                        + "." + c.toUpperCase()
                        + "=" + dialect.quoteIdentifier("SRC")
                        + "." + c.toUpperCase()
                ).collect(Collectors.joining(" AND "));
        String nrmPart = scala.collection.JavaConverters.seqAsJavaListConverter(nrmCols).asJava().stream()
                .map(c -> c.toUpperCase() + "=" + dialect.quoteIdentifier("SRC")
                        + "." + c.toUpperCase()
                ).collect(Collectors.joining(","));
        String columns = String.join(",", scala.collection.JavaConverters.seqAsJavaListConverter(fullCols).asJava()).toUpperCase();
        String placeholders = scala.collection.JavaConverters.seqAsJavaListConverter(fullCols).asJava().stream().map(x -> "?").collect(Collectors.joining(","));
        String mergeStmtDB2 = String.format("MERGE INTO %s AS %s USING TABLE(VALUES(%s)) AS SRC(%s) "
                        + "on %s WHEN NOT MATCHED THEN INSERT (%s) VALUES(%s)"
                        + " WHEN MATCHED THEN UPDATE SET %s", table.toUpperCase(),
                dialect.quoteIdentifier("TGT"),
                placeholders, columns, priPart, columns, fullPart, nrmPart);
        String mergeStmtOthers = String.format("INSERT INTO %s (%s) " +
                        "        VALUES(%s)\n" +
                        "        ON CONFLICT (%s) DO UPDATE\n" +
                        "        SET %s ", table.toUpperCase(),
                columns.replaceAll("\"", ""),
                placeholders,
                String.join(",", scala.collection.JavaConverters.seqAsJavaListConverter(priKeys).asJava()),
                nrmPart.replaceAll("\"", "").replaceAll("SRC[.]", "excluded.")
        );
        String mergeStmt = dialect.toString().contains(".DB2Dialect") ? mergeStmtDB2 : mergeStmtOthers;
        logger.info("merge query sql is\n {}", mergeStmt);
        return mergeStmt;
    }


    public static void insertTable(Config config, String tag, JdbcOptionsInWrite options, Dataset<Row> df) {
        String url = options.url();
        String table = options.table();
        JdbcDialect dialect = JdbcDialects.get(url);
        int batchSize = options.batchSize();
        int isolationLevel = options.isolationLevel();
        df.printSchema();
        java.util.Map<String, String> map = new HashMap<>();
        Dataset<Row> df2 = df;
        if (url.startsWith(JDBC_DB2)) {
            map = queryTableColumns(config, tag, table);
            df = castDataset(df, map);
            StructType rddSchema = df.schema();
            java.util.Map<String, String> finalMap = map;
            List<Column> colList = Arrays.stream(rddSchema.fields())
                    .filter(x -> finalMap.containsKey(x.name().toUpperCase()))
                    .map(x -> col(x.name()))
                    .collect(Collectors.toList());
            Column[] columns = new Column[colList.size()];
            columns = colList.toArray(columns);
            df2 = df.select(columns);
            df2.printSchema();
        }
        StructType rddSchema2 = df2.schema();
        String insertStmt = getInsertStatement(table, rddSchema2, dialect);
        Dataset<Row> repartitionedDF;
        if (!options.numPartitions().isEmpty()) {
            int n = Integer.parseInt(options.numPartitions().get().toString());
            if (n < df2.rdd().getNumPartitions()) {
                repartitionedDF = df2.coalesce(n);
            } else {
                repartitionedDF = df2;
            }
        } else {
            repartitionedDF = df2;
        }
        java.util.Map<String, String> finalMap = map;
        repartitionedDF.javaRDD().foreachPartition(iterator -> runPartition(config, tag, iterator, rddSchema2, insertStmt, batchSize, dialect, isolationLevel, options,
                scala.collection.JavaConverters.mapAsScalaMapConverter(finalMap).asScala()));

    }

    @FunctionalInterface
    interface JDBCValueSetter {
        void apply(PreparedStatement stmt, Row row, int pos) throws SQLException;
    }

    private static JDBCValueSetter makeSetter(Map<String, String> dbSchema, StructField f) throws SQLException {
        DataType dataType = f.dataType();
        if (dataType instanceof IntegerType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setInt(pos + 1, row.getInt(pos));
        } else if (dataType instanceof LongType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setLong(pos + 1, row.getLong(pos));
        } else if (dataType instanceof DoubleType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setDouble(pos + 1, row.getDouble(pos));
        } else if (dataType instanceof FloatType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setFloat(pos + 1, row.getFloat(pos));
        } else if (dataType instanceof ShortType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setShort(pos + 1, row.getShort(pos));
        } else if (dataType instanceof ByteType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setInt(pos + 1, row.getByte(pos));
        } else if (dataType instanceof BooleanType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setBoolean(pos + 1, row.getBoolean(pos));
        } else if (dataType instanceof StringType) {
            return (PreparedStatement stmt, Row row, int pos) -> {
                String value = row.getString(pos).trim();
                if (dbSchema != null && dbSchema.get(f.name()).isDefined()) {
                    String len = regexExtractFirst(dbSchema.get(f.name()).get(), REGEXP_LENGTH);
                    if (len.equalsIgnoreCase("0")) {
                        logger.info("Not extract length from datatype info");
                        stmt.setString(pos + 1, value);
                        return;
                    }
                    int length = Integer.parseInt(len);
                    int actLength = getStringOctetsLen(value);
                    if (actLength > length) {
                        String newValue = value.replaceAll("[^\\p{ASCII}]", "");
                        if (getStringOctetsLen(newValue) <= length) {
                            stmt.setString(pos + 1, newValue);
                        } else {
                            stmt.setString(pos + 1, newValue.substring(0, length));
                        }
                    } else {
                        stmt.setString(pos + 1, value);
                    }
                } else {
                    stmt.setString(pos + 1, value);
                }
            };
        } else if (dataType instanceof DateType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setDate(pos + 1, row.getDate(pos));
        } else if (dataType instanceof DecimalType) {
            return (PreparedStatement stmt, Row row, int pos) ->
                    stmt.setBigDecimal(pos + 1, row.getDecimal(pos));
        } else if (dataType instanceof TimestampType) {
            return (PreparedStatement stmt, Row row, int pos) -> stmt.setTimestamp(pos + 1, row.getTimestamp(pos));
        } else {
            throw new SQLException("not found this type:" + dataType.toString());
        }
    }

    private static JdbcType getJdbcType(DataType dt, JdbcDialect dialect) {
        if (dialect.getJDBCType(dt).isDefined()) {
            return dialect.getJDBCType(dt).get();
        } else {
            return getCommonJDBCType(dt).get();
        }
    }

    private static void runPartition(
            Config config,
            String tag,
            Iterator<Row> iterator,
            StructType rddSchema,
            String insertStmt,
            int batchSize,
            JdbcDialect dialect,
            int isolationLevel,
            JDBCOptions options,
            Map<String, String> dbSchema
    ) throws SQLException {
        Connection conn = JdbcConnectionPool.getPool(config, tag).getConnection(tag);
        boolean committed = false;
        int finalIsolationLevel = getIsolationLevel(isolationLevel, conn);
        if (isolationLevel != Connection.TRANSACTION_NONE) {
            boolean supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE;
            try {
                if (supportsTransactions) {
                    conn.setAutoCommit(false);
                    conn.setTransactionIsolation(finalIsolationLevel);
                }
                try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
                    List<JDBCValueSetter> setters = getSetters(rddSchema, dbSchema);
                    List<Integer> nullTypes = getNullTypes(rddSchema, dialect);
                    int numFields = rddSchema.fields().length;
                    int rowCount = 0;
                    stmt.setQueryTimeout(options.queryTimeout());
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        int i = 0;
                        while (i < numFields) {
                            if (row.isNullAt(i)) {
                                stmt.setNull(i + 1, nullTypes.get(i));
                            } else {
                                setters.get(i).apply(stmt, row, i);
                            }
                            i = i + 1;
                        }
                        stmt.addBatch();
                        rowCount += 1;
                        if (rowCount % batchSize == 0) {
                            stmt.executeBatch();
                            rowCount = 0;
                        }
                    }
                    if (rowCount > 0) {
                        stmt.executeBatch();
                    }
                } catch (SQLException ex) {
                    printSQLException(ex);
                    throw ex;
                }
                if (supportsTransactions) {
                    conn.commit();
                }
                committed = true;
            } finally {
                if (!committed && supportsTransactions) {
                    conn.rollback();
                }
                conn.close();
            }
        }
    }

    public static void mergeTable(Config config, String tag, JdbcOptionsInWrite options, Dataset<Row> df, Seq<String> priKeys) {
        String url = options.url();
        String table = options.table();
        JdbcDialect dialect = JdbcDialects.get(url);
        int batchSize = options.batchSize();
        int isolationLevel = options.isolationLevel();
        java.util.Map<String, String> map = new HashMap<>();
        Dataset<Row> df2 = df;
        if (url.startsWith(JDBC_DB2)) {
            map = queryTableColumns(config, tag, table);
            df = castDataset(df, map);
            StructType rddSchema = df.schema();
            java.util.Map<String, String> finalMap = map;
            List<Column> colList = Arrays.stream(rddSchema.fields())
                    .filter(x -> finalMap.containsKey(x.name().toUpperCase()))
                    .map(x -> col(x.name()))
                    .collect(Collectors.toList());
            Column[] columns = new Column[colList.size()];
            columns = colList.toArray(columns);
            df2 = df.select(columns);
        }
        StructType rddSchema2 = df2.schema();
        String mergeStmt = getMergeStatement(table, rddSchema2, priKeys, dialect);
        Dataset<Row> repartitionedDF;
        if (!options.numPartitions().isEmpty()) {
            int n = Integer.parseInt(options.numPartitions().get().toString());
            repartitionedDF = n < df2.rdd().getNumPartitions() ? df2.coalesce(n) : df2;
        } else {
            repartitionedDF = df2;
        }
        java.util.Map<String, String> finalMap = map;
        repartitionedDF.javaRDD().foreachPartition(iterator -> runPartition(config, tag, iterator, rddSchema2, mergeStmt, batchSize, dialect, isolationLevel, options,
                scala.collection.JavaConverters.mapAsScalaMapConverter(finalMap).asScala()));
    }

    public static void mergeJdbcTable(Config config, String tag, JdbcOptionsInWrite options, Dataset<Row> df, Seq<String> priKeys) {
        String url = options.url();
        String table = options.table();
        JdbcDialect dialect = JdbcDialects.get(url);
        int batchSize = options.batchSize();
        int isolationLevel = options.isolationLevel();
        StructType rddSchema = df.schema();
        List<Column> colList = Arrays.stream(rddSchema.fields())
                .map(x -> col(x.name()))
                .collect(Collectors.toList());
        Column[] columns = new Column[colList.size()];
        columns = colList.toArray(columns);
        Dataset<Row> df2 = df.select(columns);
        StructType rddSchema2 = df2.schema();
        String mergeStmt = getMergeStatement(table, rddSchema2, priKeys, dialect)
                .replace("TABLE(VALUES", "(VALUES");
        Dataset<Row> repartitionedDF;
        if (!options.numPartitions().isEmpty()) {
            int n = Integer.parseInt(options.numPartitions().get().toString());
            repartitionedDF = n < df2.rdd().getNumPartitions() ? df2.coalesce(n) : df2;
        } else {
            repartitionedDF = df2;
        }
        repartitionedDF.javaRDD().foreachPartition(iterator -> runPartition(config, tag, iterator, rddSchema2, mergeStmt, batchSize, dialect, isolationLevel, options, null));
    }

    public static void deleteTable(Config config, String tag, JdbcOptionsInWrite options, Dataset<Row> df, Seq<String> priKeys) {
        String url = options.url();
        String table = options.table();
        JdbcDialect dialect = JdbcDialects.get(url);
        java.util.Map<String, String> map = new HashMap<>();
        if (url.startsWith(JDBC_DB2)) {
            map = queryTableColumns(config, tag, table);
            df = castDataset(df, map);
        }
        int batchSize = options.batchSize();
        int isolationLevel = options.isolationLevel();
        Column[] colArray = new Column[priKeys.size()];
        colArray = scala.collection.JavaConverters.seqAsJavaListConverter(priKeys).asJava()
                .stream().map(functions::col).collect(Collectors.toList()).toArray(colArray);
        Dataset<Row> refrmdDF = df.select(colArray);
        StructType rddSchema = refrmdDF.schema();
        String deleteStmt = getDeleteStatement(table, rddSchema, dialect);
        Dataset<Row> repartitionedDF;
        if (!options.numPartitions().isEmpty()) {
            int n = Integer.parseInt(options.numPartitions().get().toString());
            repartitionedDF = n < df.rdd().getNumPartitions() ? df.coalesce(n) : df;
        } else {
            repartitionedDF = df;
        }
        java.util.Map<String, String> finalMap = map;
        repartitionedDF.javaRDD().foreachPartition(iterator ->
                runPartition(config, tag, iterator, rddSchema, deleteStmt, batchSize, dialect, isolationLevel, options,
                        scala.collection.JavaConverters.mapAsScalaMapConverter(finalMap).asScala()));

    }

    public static java.util.Map<String, String> queryTableColumns(Config config, String tag, final String table) {
        java.util.Map<String, String> columns;
        String sql = Constants.queryTableColumns.replace("{{sourceTable}}", table);
        logger.info("Qeury table info sql:\n {}", sql);
        List<HashMap<String, Object>> result = query(config, tag, sql);
        HashMap<String, ?> map;
        columns = new HashMap<>(result.size());
        for (HashMap<String, Object> stringObjectHashMap : result) {
            map = stringObjectHashMap;
            final String typeName = map.get("TYPENAME").toString().trim();
            final String convertTypeName;
            switch (typeName) {
                case "DATE":
                    convertTypeName = "date";
                    break;
                case "TIME":
                    convertTypeName = "time";
                    break;
                case "TIMESTAMP":
                    convertTypeName = "timestamp";
                    break;
                case "SMALLINT":
                    convertTypeName = "short";
                    break;
                case "INTEGER":
                    convertTypeName = "integer";
                    break;
                case "BIGINT":
                    convertTypeName = "long";
                    break;
                case "REAL":
                    convertTypeName = "float";
                    break;
                case "DOUBLE":
                    convertTypeName = "double";
                    break;
                case "DECIMAL": {
                    final String length = map.get("LENGTH").toString().trim();
                    final String scale = map.get("SCALE").toString().trim();
                    convertTypeName = "decimal(" + length + "," + scale + ")";
                }
                break;
                case "CHARACTER": {
                    final String length = map.get("LENGTH").toString().trim();
                    convertTypeName = "char(" + length + ")";
                }
                break;
                default:
                case "VARCHAR": {
                    final String length = map.get("LENGTH").toString().trim();
                    convertTypeName = "varchar(" + length + ")";
                }
                break;
            }
            columns.put(map.get("COLNAME").toString().trim().toUpperCase(), convertTypeName);
        }
        return columns;
    }

    public static List<HashMap<String, Object>> query(Config config, String tag, final String sql) {
        List<HashMap<String, Object>> list;
        try (Connection conn = JdbcConnectionPool.getPool(config, tag).getConnection(tag);
             PreparedStatement stmt = conn.prepareStatement(sql,
                     ResultSet.TYPE_FORWARD_ONLY,
                     ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery()
        ) {
            logger.info("query data  sql is \n{} ", sql);
            list = resultSet2List(rs);
        } catch (SQLException ex) {
            printSQLException(ex);
            throw new DataFlowException(ex);
        }
        return list;
    }

    private static List<HashMap<String, Object>> resultSet2List(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<HashMap<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            HashMap<String, Object> row = new HashMap<>(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(md.getColumnName(i).toUpperCase(), rs.getObject(i));
            }
            list.add(row);
        }
        return list;
    }


    public static List<JDBCValueSetter> getSetters(StructType schema,
                                                   Map<String, String> dbSchema) {
        return Arrays.stream(schema.fields()).map(f -> {
            try {
                return makeSetter(dbSchema, f);
            } catch (SQLException e) {
                printSQLException(e);
                throw new DataFlowException(e);
            }
        }).collect(Collectors.toList());
    }

    public static List<Integer> getNullTypes(StructType schema, JdbcDialect dialect) {
        return Arrays.stream(schema.fields()).map(f ->
                getJdbcType(f.dataType(), dialect).jdbcNullType()).collect(Collectors.toList());
    }

    private static int getIsolationLevel(int isolationLevel, Connection conn) {
        int finalIsolationLevel = 0;
        if (isolationLevel != Connection.TRANSACTION_NONE) {
            try {
                DatabaseMetaData metadata = conn.getMetaData();
                if (metadata.supportsTransactions()) {
                    finalIsolationLevel = metadata.getDefaultTransactionIsolation();
                    if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
                        finalIsolationLevel = isolationLevel;
                    }
                }
            } catch (Exception ex) {
                logger.error("error", ex);
            }
        }
        return finalIsolationLevel;
    }

    public static void upsertTable(Config config, String tag, JdbcOptionsInWrite options, Dataset<Row> df, String sql) {
        String url = options.url();
        JdbcDialect dialect = JdbcDialects.get(url);
        int batchSize = options.batchSize();
        int isolationLevel = options.isolationLevel();
        Dataset<Row> repartitionedDF;
        if (!options.numPartitions().isEmpty() &&
                !options.numPartitions().get().toString().equalsIgnoreCase("0")) {
            int n = Integer.parseInt(options.numPartitions().get().toString());
            if (n < df.rdd().getNumPartitions()) {
                repartitionedDF = df.coalesce(n);
            } else {
                repartitionedDF = df;
            }
        } else {
            repartitionedDF = df;
        }
        HashMap<String, String> map = new HashMap<>();
        StructType rddSchema = repartitionedDF.schema();
        repartitionedDF.javaRDD().foreachPartition(iterator -> runPartition(config, tag,
                iterator, rddSchema, sql, batchSize, dialect, isolationLevel, options,
                scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala()));
    }

    public static LinkedHashMap<String, String> getColNameAndType(Config config, String tag, String table) {
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        String sql = "SELECT COLNAME, TYPENAME||':'||LENGTH FROM syscat.columns WHERE trim(tabschema)||'.'||TABNAME = '" + table + "' and GENERATED not in ('A','D') ORDER BY colno WITH ur";
        try (Connection conn = JdbcConnectionPool.getPool(config, tag).getConnection(tag);
             PreparedStatement stmt = conn.prepareStatement(sql,
                     ResultSet.TYPE_SCROLL_INSENSITIVE,
                     ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery()
        ) {
            //if ResultSet is not empty
            if (rs.next()) {
                rs.previous();
                while (rs.next()) {
                    map.put(rs.getString(1), rs.getString(2));
                }
            }
        } catch (SQLException ex) {
            printSQLException(ex);
            throw new DataFlowException(ex);
        }
        return map;
    }

}
