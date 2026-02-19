package com.ninedata.dbbench.database.plugin;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;

import java.io.IOException;
import java.util.*;

/**
 * Simplified database definition format (convention over configuration).
 *
 * <p>Top-level fields replace nested structures:
 * <ul>
 *   <li>{@code driver} / {@code url} / {@code validation} / {@code urlParams} — replaces {@code jdbc.*}
 *   <li>{@code noLimit} / {@code noForUpdate} — replaces {@code capabilities.*}
 *   <li>{@code dropStatement} — replaces {@code schema.dropTableStatement}
 *   <li>{@code csvLoad} — string (sql_template), "pg_copy", or object with batch/hint
 *   <li>{@code metrics} / {@code hostMetrics} — simplified list format
 *   <li>{@code init} — replaces {@code initStatements}
 * </ul>
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabaseDefinition {
    private String name;
    private String type;
    @JsonSetter(nulls = Nulls.SKIP)
    private List<String> aliases = List.of();

    // ── JDBC (flat) ──
    private String driver;
    private String url;
    private String validation;
    @JsonSetter(nulls = Nulls.SKIP)
    private Map<String, String> urlParams = Map.of();

    // ── Capabilities (flat, only non-default) ──
    private boolean noLimit = false;
    private boolean noForUpdate = false;

    // ── Schema ──
    private String dropStatement;

    // ── Init ──
    @JsonSetter(nulls = Nulls.SKIP)
    private List<String> init = List.of();

    // ── CSV Load ──
    @JsonDeserialize(using = CsvLoadDeserializer.class)
    private CsvLoadConfig csvLoad;

    // ── Metrics ──
    @JsonDeserialize(using = MetricsListDeserializer.class)
    @JsonSetter(nulls = Nulls.SKIP)
    private List<MetricsEntry> metrics = List.of();

    @JsonDeserialize(using = MetricsListDeserializer.class)
    @JsonSetter(nulls = Nulls.SKIP)
    private List<MetricsEntry> hostMetrics = List.of();

    // ── Derived accessors (used by ScriptBasedAdapter) ──

    public String getDriverClassName() { return driver; }
    public String getUrlTemplate() { return url; }
    public String getValidationQuery() { return validation != null ? validation : "SELECT 1"; }
    public boolean supportsLimitSyntax() { return !noLimit; }
    public boolean supportsForUpdate() { return !noForUpdate; }
    public String getDropTableStatement() {
        return dropStatement != null ? dropStatement : "DROP TABLE IF EXISTS ${tableName}";
    }
    public String getCreateTablesFile() { return "create-tables.sql"; }
    public String getCreateIndexesFile() { return "create-indexes.sql"; }

    public boolean supportsCsvLoad() {
        return csvLoad != null && !"none".equalsIgnoreCase(csvLoad.getMethod());
    }

    public CsvLoadConfig getEffectiveCsvLoad() {
        return csvLoad != null ? csvLoad : new CsvLoadConfig();
    }

    // ── CSV Load Config ──
    @Data
    public static class CsvLoadConfig {
        private String method = "none";
        private String template;
        private int batch = 5000;
        private String hint;
    }

    // ── Metrics Entry (unified: query + computed) ──
    @Data
    public static class MetricsEntry {
        private String sql;
        private String format;       // inferred: key_value | single_row | single_value
        private String metric;       // for single_value
        private Map<String, String> map;  // for key_value
        private boolean computed;
        private String expression;   // for computed metrics
    }

    // ── CsvLoad Deserializer ──
    // Supports: string → sql_template, "pg_copy" → pg_copy, object with batch/hint → batch_insert, absent → none
    public static class CsvLoadDeserializer extends JsonDeserializer<CsvLoadConfig> {
        @Override
        public CsvLoadConfig deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            CsvLoadConfig cfg = new CsvLoadConfig();
            if (p.currentToken() == JsonToken.VALUE_STRING) {
                String val = p.getText();
                if ("pg_copy".equalsIgnoreCase(val)) {
                    cfg.setMethod("pg_copy");
                } else {
                    cfg.setMethod("sql_template");
                    cfg.setTemplate(val);
                }
            } else if (p.currentToken() == JsonToken.START_OBJECT) {
                while (p.nextToken() != JsonToken.END_OBJECT) {
                    String field = p.currentName();
                    p.nextToken();
                    switch (field) {
                        case "batch" -> { cfg.setBatch(p.getIntValue()); cfg.setMethod("batch_insert"); }
                        case "hint" -> cfg.setHint(p.getText());
                        case "method" -> cfg.setMethod(p.getText());
                        case "template" -> { cfg.setTemplate(p.getText()); cfg.setMethod("sql_template"); }
                        case "batchSize" -> { cfg.setBatch(p.getIntValue()); cfg.setMethod("batch_insert"); }
                        case "insertHint" -> cfg.setHint(p.getText());
                    }
                }
                // If method wasn't explicitly set but we have batch, it's batch_insert
                if ("none".equals(cfg.getMethod()) && cfg.getBatch() > 0) {
                    cfg.setMethod("batch_insert");
                }
            }
            return cfg;
        }
    }

    // ── Metrics List Deserializer ──
    // Supports the simplified list format:
    //   - metric_name: "SQL"                    → single_value
    //   - sql: "..."                            → single_row
    //   - sql: "..." + map: {...}               → key_value
    //   - metric_name: "expr" + computed: true   → computed
    public static class MetricsListDeserializer extends JsonDeserializer<List<MetricsEntry>> {
        @Override
        public List<MetricsEntry> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            List<MetricsEntry> entries = new ArrayList<>();
            if (p.currentToken() != JsonToken.START_ARRAY) return entries;

            while (p.nextToken() != JsonToken.END_ARRAY) {
                if (p.currentToken() == JsonToken.START_OBJECT) {
                    MetricsEntry entry = new MetricsEntry();
                    String unknownKey = null;
                    String unknownVal = null;

                    while (p.nextToken() != JsonToken.END_OBJECT) {
                        String field = p.currentName();
                        p.nextToken();
                        switch (field) {
                            case "sql" -> entry.setSql(p.getText());
                            case "map" -> entry.setMap(readStringMap(p));
                            case "computed" -> entry.setComputed(p.getBooleanValue());
                            case "format" -> entry.setFormat(p.getText());
                            case "metric" -> entry.setMetric(p.getText());
                            case "mappings" -> entry.setMap(readStringMap(p));
                            default -> {
                                // metric_name: "SQL or expression"
                                unknownKey = field;
                                unknownVal = p.getText();
                            }
                        }
                    }

                    // Resolve shorthand: { metric_name: "SQL/expr" }
                    if (unknownKey != null && entry.getSql() == null) {
                        if (entry.isComputed()) {
                            entry.setMetric(unknownKey);
                            entry.setExpression(unknownVal);
                        } else {
                            entry.setMetric(unknownKey);
                            entry.setSql(unknownVal);
                            entry.setFormat("single_value");
                        }
                    }

                    // Infer format if not set
                    if (entry.getFormat() == null && !entry.isComputed()) {
                        if (entry.getMap() != null && !entry.getMap().isEmpty()) {
                            entry.setFormat("key_value");
                        } else if (entry.getMetric() != null) {
                            entry.setFormat("single_value");
                        } else {
                            entry.setFormat("single_row");
                        }
                    }

                    entries.add(entry);
                }
            }
            return entries;
        }

        private Map<String, String> readStringMap(JsonParser p) throws IOException {
            if (p.currentToken() != JsonToken.START_OBJECT) return Map.of();
            Map<String, String> map = new LinkedHashMap<>();
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String key = p.currentName();
                p.nextToken();
                map.put(key, p.getText());
            }
            return map;
        }
    }
}
