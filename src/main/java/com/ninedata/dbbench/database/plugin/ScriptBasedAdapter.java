package com.ninedata.dbbench.database.plugin;

import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.database.AbstractDatabaseAdapter;
import com.zaxxer.hikari.HikariConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ScriptBasedAdapter extends AbstractDatabaseAdapter {

    private final DatabaseDefinition definition;
    private String[] createTableStatements = new String[0];
    private String[] createIndexStatements = new String[0];

    public ScriptBasedAdapter(DatabaseConfig config, DatabaseDefinition definition) {
        super(config);
        this.definition = definition;
        appendUrlParams();
        loadSqlFiles();
    }

    @Override
    public String getDatabaseType() {
        return definition.getName();
    }

    @Override
    public boolean supportsLimitSyntax() {
        return definition.supportsLimitSyntax();
    }

    @Override
    public boolean supportsForUpdate() {
        return definition.supportsForUpdate();
    }

    @Override
    public boolean supportsCsvLoad() {
        return definition.supportsCsvLoad();
    }

    // ── URL Params ──────────────────────────────────────────────────────
    private void appendUrlParams() {
        Map<String, String> params = definition.getUrlParams();
        if (params == null || params.isEmpty()) return;
        String url = config.getJdbcUrl();
        for (var entry : params.entrySet()) {
            String key = entry.getKey();
            if (!url.contains(key)) {
                String sep = url.contains("?") ? "&" : "?";
                url = url + sep + key + "=" + entry.getValue();
            }
        }
        config.setJdbcUrl(url);
    }

    // ── Initialize ──────────────────────────────────────────────────────
    @Override
    public void initialize() throws SQLException {
        super.initialize();
    }

    @Override
    protected void configureHikari(HikariConfig hikariConfig) {
        List<String> initStmts = definition.getInit();
        if (initStmts != null && !initStmts.isEmpty()) {
            String combined = String.join("; ", initStmts);
            hikariConfig.setConnectionInitSql(combined);
            log.info("Set connectionInitSql for {}: {}", definition.getName(), combined);
        }
    }

    // ── Drop / Create Schema ────────────────────────────────────────────
    @Override
    protected String getDropTableStatement(String tableName) {
        return definition.getDropTableStatement().replace("${tableName}", tableName);
    }

    @Override
    protected String[] getCreateTableStatements() {
        return createTableStatements;
    }

    @Override
    protected String[] getCreateIndexStatements() {
        return createIndexStatements;
    }

    // ── CSV Loading ──────────────────────────────────────────────────────
    @Override
    public void loadCsvFile(String tableName, String csvFilePath, String[] columns) throws SQLException {
        DatabaseDefinition.CsvLoadConfig csv = definition.getEffectiveCsvLoad();
        String method = csv.getMethod();
        switch (method.toLowerCase()) {
            case "sql_template" -> loadCsvBySqlTemplate(tableName, csvFilePath, columns, csv);
            case "pg_copy" -> loadCsvByPgCopy(tableName, csvFilePath, columns);
            case "batch_insert" -> loadCsvByBatchInsert(tableName, csvFilePath, columns, csv);
            default -> throw new UnsupportedOperationException("CSV load not supported (method=" + method + ")");
        }
    }

    private void loadCsvBySqlTemplate(String tableName, String csvFilePath, String[] columns,
                                       DatabaseDefinition.CsvLoadConfig csv) throws SQLException {
        String template = csv.getTemplate();
        String columnList = String.join(", ", columns);
        String sql = template
                .replace("${filePath}", csvFilePath.replace("\\", "\\\\"))
                .replace("${tableName}", tableName)
                .replace("${columns}", columnList);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            conn.commit();
        }
    }

    private void loadCsvByPgCopy(String tableName, String csvFilePath, String[] columns) throws SQLException {
        String columnList = String.join(", ", columns);
        String copySql = "COPY " + tableName + " (" + columnList + ") FROM STDIN WITH (FORMAT csv, NULL '\\N')";
        try (Connection conn = getConnection()) {
            Object pgConn = conn.unwrap(Class.forName("org.postgresql.PGConnection"));
            Object copyManager = pgConn.getClass().getMethod("getCopyAPI").invoke(pgConn);
            try (FileInputStream fis = new FileInputStream(csvFilePath)) {
                copyManager.getClass().getMethod("copyIn", String.class, InputStream.class)
                        .invoke(copyManager, copySql, fis);
            }
            conn.commit();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("PG COPY failed for " + tableName, e);
        }
    }

    private void loadCsvByBatchInsert(String tableName, String csvFilePath, String[] columns,
                                       DatabaseDefinition.CsvLoadConfig csv) throws SQLException {
        String columnList = String.join(", ", columns);
        String placeholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        String hint = csv.getHint();
        String hintStr = (hint != null && !hint.isBlank()) ? " " + hint : "";
        String sql = "INSERT" + hintStr + " INTO " + tableName + " (" + columnList + ") VALUES (" + placeholders + ")";
        int batchSize = csv.getBatch();
        if (batchSize <= 0) batchSize = 5000;

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            int count = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = parseCsvLine(line);
                for (int i = 0; i < columns.length && i < values.length; i++) {
                    String val = values[i];
                    if ("\\N".equals(val) || val.isEmpty()) {
                        ps.setNull(i + 1, Types.VARCHAR);
                    } else {
                        ps.setString(i + 1, val);
                    }
                }
                ps.addBatch();
                if (++count % batchSize == 0) {
                    ps.executeBatch();
                    conn.commit();
                }
            }
            if (count % batchSize != 0) {
                ps.executeBatch();
            }
            conn.commit();
            log.info("Batch-inserted {} rows into {}", count, tableName);
        } catch (Exception e) {
            throw new SQLException("Failed to read CSV file: " + csvFilePath, e);
        }
    }

    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') { sb.append('"'); i++; }
                    else { inQuotes = false; }
                } else { sb.append(c); }
            } else if (c == '"') { inQuotes = true; }
            else if (c == ',') { fields.add(sb.toString()); sb.setLength(0); }
            else { sb.append(c); }
        }
        fields.add(sb.toString());
        return fields.toArray(new String[0]);
    }

    // ── Metrics Collection ────────────────────────────────────────────────
    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        return executeMetrics(definition.getMetrics());
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        return executeMetrics(definition.getHostMetrics());
    }

    private Map<String, Object> executeMetrics(List<DatabaseDefinition.MetricsEntry> entries) throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        if (entries == null || entries.isEmpty()) return metrics;

        // Separate queries from computed
        List<DatabaseDefinition.MetricsEntry> queries = new ArrayList<>();
        List<DatabaseDefinition.MetricsEntry> computed = new ArrayList<>();
        for (var e : entries) {
            if (e.isComputed()) computed.add(e);
            else queries.add(e);
        }

        // Execute queries
        if (!queries.isEmpty()) {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                for (var mq : queries) {
                    try {
                        ResultSet rs = stmt.executeQuery(mq.getSql());
                        parseResultSet(rs, mq, metrics);
                        rs.close();
                    } catch (SQLException e) {
                        log.debug("Metrics query failed for {}: {}", definition.getName(), e.getMessage());
                    }
                }
                conn.commit();
            }
        }

        // Computed metrics
        if (!computed.isEmpty()) {
            Map<String, Double> vars = new HashMap<>();
            for (var entry : metrics.entrySet()) {
                if (entry.getValue() instanceof Number n) {
                    vars.put(entry.getKey(), n.doubleValue());
                }
            }
            for (var cm : computed) {
                double val = ExpressionEvaluator.evaluate(cm.getExpression(), vars);
                val = Math.round(val * 100.0) / 100.0;
                metrics.put(cm.getMetric(), val);
                vars.put(cm.getMetric(), val);
            }
        }
        return metrics;
    }

    private void parseResultSet(ResultSet rs, DatabaseDefinition.MetricsEntry mq, Map<String, Object> metrics) throws SQLException {
        String format = mq.getFormat();
        Map<String, String> mappings = mq.getMap();

        switch (format) {
            case "key_value" -> {
                while (rs.next()) {
                    String key = rs.getString(1);
                    String val = rs.getString(2);
                    String mapped = (mappings != null) ? mappings.get(key) : null;
                    if (mapped != null) {
                        metrics.put(mapped, parseNumeric(val));
                    }
                }
            }
            case "single_row" -> {
                if (rs.next()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        String colName = meta.getColumnLabel(i);
                        String mapped = (mappings != null && !mappings.isEmpty()) ? mappings.get(colName) : colName;
                        if (mapped != null) {
                            metrics.put(mapped, getNumericValue(rs, i));
                        }
                    }
                }
            }
            case "single_value" -> {
                if (rs.next()) {
                    String metricName = mq.getMetric();
                    if (metricName != null) {
                        metrics.put(metricName, getNumericValue(rs, 1));
                    }
                }
            }
        }
    }

    private Object parseNumeric(String val) {
        if (val == null) return 0L;
        try {
            if (val.contains(".")) return Double.parseDouble(val);
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return val;
        }
    }

    private Object getNumericValue(ResultSet rs, int col) throws SQLException {
        int type = rs.getMetaData().getColumnType(col);
        return switch (type) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT -> rs.getLong(col);
            case Types.FLOAT, Types.DOUBLE, Types.REAL, Types.DECIMAL, Types.NUMERIC -> rs.getDouble(col);
            default -> {
                String s = rs.getString(col);
                yield (s != null) ? parseNumeric(s) : 0L;
            }
        };
    }

    // ── SQL File Loading ──────────────────────────────────────────────────
    private void loadSqlFiles() {
        createTableStatements = loadSqlFile(definition.getCreateTablesFile());
        createIndexStatements = loadSqlFile(definition.getCreateIndexesFile());
        log.info("Loaded SQL for plugin {}: {} create-table, {} create-index statements",
                definition.getName(), createTableStatements.length, createIndexStatements.length);
    }

    private String[] loadSqlFile(String fileName) {
        if (fileName == null || fileName.isBlank()) return new String[0];
        String content = readSqlContent(fileName);
        if (content == null) return new String[0];
        return parseSqlStatements(content);
    }

    private String readSqlContent(String fileName) {
        Path extFile = Paths.get("./db-definitions", definition.getType(), fileName);
        if (Files.exists(extFile)) {
            try {
                return Files.readString(extFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                log.warn("Failed to read external SQL file: {}", extFile, e);
            }
        }
        String cpPath = "db-definitions/" + definition.getType() + "/" + fileName;
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(cpPath)) {
            if (is != null) {
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            log.warn("Failed to read classpath SQL file: {}", cpPath, e);
        }
        log.debug("SQL file not found for plugin {}: {}", definition.getName(), fileName);
        return null;
    }

    static String[] parseSqlStatements(String content) {
        return Arrays.stream(content.split(";"))
                .map(String::strip)
                .filter(s -> !s.isEmpty())
                .filter(s -> !s.startsWith("--"))
                .map(s -> Arrays.stream(s.split("\n"))
                        .filter(line -> !line.strip().startsWith("--"))
                        .collect(Collectors.joining("\n")))
                .filter(s -> !s.strip().isEmpty())
                .toArray(String[]::new);
    }
}
