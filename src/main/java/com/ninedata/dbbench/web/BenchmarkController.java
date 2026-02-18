package com.ninedata.dbbench.web;

import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.config.ProfileService;
import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.database.DatabaseFactory;
import com.ninedata.dbbench.database.plugin.DatabaseDefinitionRegistry;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import com.ninedata.dbbench.metrics.SshMetricsCollector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/benchmark")
@RequiredArgsConstructor
public class BenchmarkController {
    private final BenchmarkEngine engine;
    private final DatabaseConfig dbConfig;
    private final ProfileService profileService;

    @PostMapping("/init")
    public ResponseEntity<Map<String, Object>> initialize() {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            engine.initialize();
            response.put("success", true);
            response.put("message", "Engine initialized");
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to initialize", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            response.put("status", engine.getStatus());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/load")
    public ResponseEntity<Map<String, Object>> loadData() {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            engine.loadDataAsync();
            response.put("success", true);
            response.put("message", "Data loading started");
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to start data loading", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            response.put("status", engine.getStatus());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @GetMapping("/load/progress")
    public ResponseEntity<Map<String, Object>> loadProgress() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("loading", engine.isLoading());
        response.put("progress", engine.getLoadProgress());
        response.put("message", engine.getLoadMessage());
        response.put("status", engine.getStatus());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/load/cancel")
    public ResponseEntity<Map<String, Object>> cancelLoad() {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            engine.cancelLoad();
            response.put("success", true);
            response.put("message", "Data loading cancellation requested");
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to cancel data loading", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            response.put("status", engine.getStatus());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/clean")
    public ResponseEntity<Map<String, Object>> cleanData() {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            engine.cleanData();
            response.put("success", true);
            response.put("message", "Data cleaned");
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to clean data", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            response.put("status", engine.getStatus());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> start() {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            engine.start();
            response.put("success", true);
            response.put("message", "Benchmark started");
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to start", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            response.put("status", engine.getStatus());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/stop")
    public ResponseEntity<Map<String, Object>> stop() {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            engine.stop();
            response.put("success", true);
            response.put("message", "Benchmark stopped");
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to stop", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            response.put("status", engine.getStatus());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", engine.getStatus());
        response.put("running", engine.isRunning());
        response.put("loading", engine.isLoading());
        if (engine.isLoading()) {
            response.put("loadProgress", engine.getLoadProgress());
            response.put("loadMessage", engine.getLoadMessage());
        }
        return ResponseEntity.ok(response);
    }

    @GetMapping("/results")
    public ResponseEntity<Map<String, Object>> results() {
        return ResponseEntity.ok(engine.getResults());
    }

    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> config() {
        return ResponseEntity.ok(engine.getConfig());
    }

    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> updateConfig(@RequestBody Map<String, Object> newConfig) {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            engine.updateConfig(newConfig);
            response.put("success", true);
            response.put("message", "Configuration updated");
            response.put("config", engine.getConfig());
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to update config", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            response.put("status", engine.getStatus());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Test database connection with provided configuration
     */
    @PostMapping("/test-connection")
    public ResponseEntity<Map<String, Object>> testConnection(@RequestBody Map<String, Object> config) {
        Map<String, Object> response = new LinkedHashMap<>();
        DatabaseAdapter testAdapter = null;

        try {
            // Create temporary config for testing
            DatabaseConfig testConfig = new DatabaseConfig();

            @SuppressWarnings("unchecked")
            Map<String, Object> db = (Map<String, Object>) config.get("database");
            if (db == null) {
                db = config; // Allow direct database config
            }

            testConfig.setType((String) db.getOrDefault("type", dbConfig.getType()));
            testConfig.setJdbcUrl((String) db.getOrDefault("jdbcUrl", dbConfig.getJdbcUrl()));
            testConfig.setUsername((String) db.getOrDefault("username", dbConfig.getUsername()));

            // Use provided password or existing password
            String password = (String) db.get("password");
            if (password != null && !password.isEmpty()) {
                testConfig.setPassword(password);
            } else {
                testConfig.setPassword(dbConfig.getPassword());
            }

            testConfig.getPool().setSize(2); // Minimal pool for testing
            testConfig.getPool().setMinIdle(1);

            // Test connection
            long startTime = System.currentTimeMillis();
            testAdapter = DatabaseFactory.create(testConfig);
            testAdapter.initialize();

            // Try to get a connection and execute a simple query
            // Use database-specific validation query
            String validationQuery = getValidationQuery(testConfig.getType());
            try (Connection conn = testAdapter.getConnection()) {
                conn.createStatement().execute(validationQuery);
            }

            long elapsed = System.currentTimeMillis() - startTime;

            response.put("success", true);
            response.put("message", String.format("Connection successful (%dms)", elapsed));
            response.put("database", testConfig.getType());
            response.put("jdbcUrl", testConfig.getJdbcUrl());
            response.put("responseTime", elapsed);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Connection test failed", e);
            response.put("success", false);
            response.put("error", e.getMessage());

            // Provide more specific error messages
            String errorMsg = e.getMessage();
            if (errorMsg != null) {
                if (errorMsg.contains("Communications link failure") || errorMsg.contains("Connection refused")) {
                    response.put("errorType", "CONNECTION_REFUSED");
                    response.put("suggestion", "Check if database server is running and accessible");
                } else if (errorMsg.contains("Access denied")) {
                    response.put("errorType", "AUTH_FAILED");
                    response.put("suggestion", "Check username and password");
                } else if (errorMsg.contains("Unknown database")) {
                    response.put("errorType", "DATABASE_NOT_FOUND");
                    response.put("suggestion", "Database does not exist, create it first");
                } else {
                    response.put("errorType", "UNKNOWN");
                }
            }

            return ResponseEntity.badRequest().body(response);
        } finally {
            if (testAdapter != null) {
                try {
                    testAdapter.close();
                } catch (Exception ignored) {}
            }
        }
    }

    @PostMapping("/test-ssh")
    public ResponseEntity<Map<String, Object>> testSshConnection(@RequestBody Map<String, Object> config) {
        Map<String, Object> response = new LinkedHashMap<>();
        SshMetricsCollector collector = null;
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> sshMap = (Map<String, Object>) config.get("ssh");
            if (sshMap == null) sshMap = config;

            DatabaseConfig.SshConfig sshCfg = new DatabaseConfig.SshConfig();
            sshCfg.setEnabled(true);
            sshCfg.setHost((String) sshMap.getOrDefault("host", ""));
            sshCfg.setPort(((Number) sshMap.getOrDefault("port", 22)).intValue());
            sshCfg.setUsername((String) sshMap.getOrDefault("username", "root"));

            String password = (String) sshMap.get("password");
            if (password != null && !password.isEmpty()) {
                sshCfg.setPassword(password);
            } else {
                sshCfg.setPassword(dbConfig.getSsh().getPassword());
            }
            String privateKey = (String) sshMap.get("privateKey");
            if (privateKey != null && !privateKey.isEmpty()) {
                sshCfg.setPrivateKey(privateKey);
            }

            // Resolve effective host
            String host = sshCfg.getHost();
            if (host == null || host.isBlank()) {
                String jdbcUrl = (String) ((Map<String, Object>) config.getOrDefault("database", Map.of())).getOrDefault("jdbcUrl", dbConfig.getJdbcUrl());
                DatabaseConfig tmpCfg = new DatabaseConfig();
                tmpCfg.setJdbcUrl(jdbcUrl);
                tmpCfg.setSsh(sshCfg);
                host = tmpCfg.getEffectiveSshHost();
            }

            long startTime = System.currentTimeMillis();
            collector = new SshMetricsCollector(sshCfg, host);
            collector.connect();
            long elapsed = System.currentTimeMillis() - startTime;

            response.put("success", true);
            response.put("message", String.format("SSH connection successful (%dms)", elapsed));
            response.put("host", host);
            response.put("port", sshCfg.getPort());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("SSH connection test failed", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(response);
        } finally {
            if (collector != null) {
                collector.disconnect();
            }
        }
    }

    @GetMapping("/database-types")
    public ResponseEntity<List<Map<String, Object>>> databaseTypes() {
        return ResponseEntity.ok(DatabaseFactory.getAvailableTypes());
    }

    /**
     * Get database-specific validation query
     * Different databases require different syntax for simple validation queries
     */
    private String getValidationQuery(String dbType) {
        if (dbType == null) {
            return "SELECT 1";
        }
        DatabaseDefinitionRegistry registry = DatabaseDefinitionRegistry.getInstance();
        if (registry != null) {
            var def = registry.getDefinition(dbType);
            if (def != null && def.getValidationQuery() != null) {
                return def.getValidationQuery();
            }
        }
        return "SELECT 1";
    }

    @GetMapping("/logs")
    public ResponseEntity<?> logs(@RequestParam(defaultValue = "100") int limit) {
        var logs = engine.getLogHistory();
        int start = Math.max(0, logs.size() - limit);
        return ResponseEntity.ok(logs.subList(start, logs.size()));
    }

    @DeleteMapping("/logs")
    public ResponseEntity<Map<String, Object>> clearLogs() {
        Map<String, Object> response = new LinkedHashMap<>();
        engine.clearLogs();
        response.put("success", true);
        response.put("message", "Logs cleared");
        return ResponseEntity.ok(response);
    }

    // ==================== Profile Management ====================

    @GetMapping("/profiles")
    public ResponseEntity<List<String>> listProfiles() {
        return ResponseEntity.ok(profileService.listProfiles());
    }

    @GetMapping("/profiles/{name}")
    public ResponseEntity<Map<String, Object>> loadProfile(@PathVariable String name) {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            Map<String, Object> config = profileService.loadProfile(name);
            response.put("success", true);
            response.put("config", config);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to load profile: {}", name, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/profiles/{name}")
    public ResponseEntity<Map<String, Object>> saveProfile(@PathVariable String name, @RequestBody Map<String, Object> config) {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            profileService.saveProfile(name, config);
            response.put("success", true);
            response.put("message", "Profile saved: " + name);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to save profile: {}", name, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @DeleteMapping("/profiles/{name}")
    public ResponseEntity<Map<String, Object>> deleteProfile(@PathVariable String name) {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            profileService.deleteProfile(name);
            response.put("success", true);
            response.put("message", "Profile deleted: " + name);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to delete profile: {}", name, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/profiles/{name}/apply")
    public ResponseEntity<Map<String, Object>> applyProfile(@PathVariable String name) {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            Map<String, Object> config = profileService.loadProfile(name);
            engine.updateConfig(config);
            response.put("success", true);
            response.put("message", "Profile applied: " + name);
            response.put("config", engine.getConfig());
            response.put("status", engine.getStatus());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to apply profile: {}", name, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
}
