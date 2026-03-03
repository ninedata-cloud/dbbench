package com.ninedata.dbbench.web;

import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.config.ProfileService;
import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.database.DatabaseFactory;
import com.ninedata.dbbench.database.plugin.DatabaseDefinitionRegistry;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import com.ninedata.dbbench.metrics.SshMetricsCollector;
import com.ninedata.dbbench.web.dto.request.*;
import com.ninedata.dbbench.web.dto.response.*;
import com.ninedata.dbbench.web.util.ConfigMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/benchmark")
@RequiredArgsConstructor
@Validated
public class BenchmarkController {
    private final BenchmarkEngine engine;
    private final DatabaseConfig dbConfig;
    private final ProfileService profileService;

    @PostMapping("/init")
    public ResponseEntity<ApiResponse<Void>> initialize() throws Exception {
        engine.initialize();
        return ResponseEntity.ok(
            ApiResponse.success("Engine initialized", null, engine.getStatus())
        );
    }

    @PostMapping("/load")
    public ResponseEntity<ApiResponse<Void>> loadData() throws Exception {
        engine.loadDataAsync();
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(
            ApiResponse.success("Data loading started", null, engine.getStatus())
        );
    }

    @GetMapping("/load/progress")
    public ResponseEntity<LoadProgressResponse> loadProgress() {
        LoadProgressResponse response = LoadProgressResponse.builder()
            .loading(engine.isLoading())
            .progress(engine.getLoadProgress())
            .message(engine.getLoadMessage())
            .status(engine.getStatus())
            .build();
        return ResponseEntity.ok(response);
    }

    @PostMapping("/load/cancel")
    public ResponseEntity<ApiResponse<Void>> cancelLoad() {
        engine.cancelLoad();
        return ResponseEntity.ok(
            ApiResponse.success("Data loading cancellation requested", null, engine.getStatus())
        );
    }

    @PostMapping("/clean")
    public ResponseEntity<ApiResponse<Void>> cleanData() throws Exception {
        engine.cleanData();
        return ResponseEntity.ok(
            ApiResponse.success("Data cleaned successfully", null, engine.getStatus())
        );
    }

    @PostMapping("/start")
    public ResponseEntity<ApiResponse<Void>> start() throws Exception {
        engine.start();
        return ResponseEntity.ok(
            ApiResponse.success("Benchmark started", null, engine.getStatus())
        );
    }

    @PostMapping("/stop")
    public ResponseEntity<ApiResponse<Void>> stop() {
        engine.stop();
        return ResponseEntity.ok(
            ApiResponse.success("Benchmark stopped", null, engine.getStatus())
        );
    }

    @GetMapping("/status")
    public ResponseEntity<BenchmarkStatusResponse> status() {
        BenchmarkStatusResponse response = BenchmarkStatusResponse.builder()
            .status(engine.getStatus())
            .running(engine.isRunning())
            .loading(engine.isLoading())
            .loadProgress(engine.isLoading() ? engine.getLoadProgress() : null)
            .loadMessage(engine.isLoading() ? engine.getLoadMessage() : null)
            .build();
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
    public ResponseEntity<ApiResponse<Map<String, Object>>> updateConfig(
            @Valid @RequestBody ConfigUpdateRequest request) {
        Map<String, Object> configMap = ConfigMapper.toMap(request);
        engine.updateConfig(configMap);
        Map<String, Object> updatedConfig = engine.getConfig();
        return ResponseEntity.ok(
            ApiResponse.success("Configuration updated", updatedConfig, engine.getStatus())
        );
    }

    /**
     * Test database connection with provided configuration
     */
    @PostMapping("/test-connection")
    public ResponseEntity<ConnectionTestResponse> testConnection(
            @Valid @RequestBody ConnectionTestRequestWrapper wrapper) {
        // Support both nested {database: {...}} and flat {...} formats
        ConnectionTestRequest request = wrapper.getDatabase();
        if (request == null) {
            throw new IllegalArgumentException("Database configuration is required");
        }

        ConnectionTestResponse response = testConnectionInternal(request);
        HttpStatus status = response.isSuccess()
            ? HttpStatus.OK
            : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status).body(response);
    }

    private ConnectionTestResponse testConnectionInternal(ConnectionTestRequest request) {
        DatabaseAdapter testAdapter = null;

        try {
            // Create temporary config for testing
            DatabaseConfig testConfig = new DatabaseConfig();
            testConfig.setType(request.getType());
            testConfig.setJdbcUrl(request.getJdbcUrl());
            testConfig.setUsername(request.getUsername());

            // Use provided password or existing password
            if (request.getPassword() != null && !request.getPassword().isEmpty()) {
                testConfig.setPassword(request.getPassword());
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
            String validationQuery = getValidationQuery(testConfig.getType());
            try (Connection conn = testAdapter.getConnection()) {
                conn.createStatement().execute(validationQuery);
            }

            long elapsed = System.currentTimeMillis() - startTime;

            return ConnectionTestResponse.builder()
                .success(true)
                .message(String.format("Connection successful (%dms)", elapsed))
                .database(testConfig.getType())
                .jdbcUrl(testConfig.getJdbcUrl())
                .responseTime(elapsed)
                .build();

        } catch (Exception e) {
            log.error("Connection test failed", e);

            // Provide more specific error messages
            String errorMsg = e.getMessage();
            String errorType = "UNKNOWN";
            String suggestion = null;

            if (errorMsg != null) {
                if (errorMsg.contains("Communications link failure") || errorMsg.contains("Connection refused")) {
                    errorType = "CONNECTION_REFUSED";
                    suggestion = "Check if database server is running and accessible";
                } else if (errorMsg.contains("Access denied")) {
                    errorType = "AUTH_FAILED";
                    suggestion = "Check username and password";
                } else if (errorMsg.contains("Unknown database")) {
                    errorType = "DATABASE_NOT_FOUND";
                    suggestion = "Database does not exist, create it first";
                }
            }

            return ConnectionTestResponse.builder()
                .success(false)
                .error(errorMsg)
                .errorType(errorType)
                .suggestion(suggestion)
                .build();

        } finally {
            if (testAdapter != null) {
                try {
                    testAdapter.close();
                } catch (Exception ignored) {}
            }
        }
    }

    @PostMapping("/test-ssh")
    public ResponseEntity<SshTestResponse> testSshConnection(@Valid @RequestBody SshTestRequest request) {
        SshMetricsCollector collector = null;
        try {
            SshConfigDto sshDto = request.getSsh();
            if (sshDto == null) {
                throw new IllegalArgumentException("SSH configuration is required");
            }

            DatabaseConfig.SshConfig sshCfg = new DatabaseConfig.SshConfig();
            sshCfg.setEnabled(true);
            sshCfg.setHost(sshDto.getHost() != null ? sshDto.getHost() : "");
            sshCfg.setPort(sshDto.getPort() != null ? sshDto.getPort() : 22);
            sshCfg.setUsername(sshDto.getUsername() != null ? sshDto.getUsername() : "root");

            if (sshDto.getPassword() != null && !sshDto.getPassword().isEmpty()) {
                sshCfg.setPassword(sshDto.getPassword());
            } else {
                sshCfg.setPassword(dbConfig.getSsh().getPassword());
            }
            if (sshDto.getPrivateKey() != null && !sshDto.getPrivateKey().isEmpty()) {
                sshCfg.setPrivateKey(sshDto.getPrivateKey());
            }

            // Resolve effective host
            String host = sshCfg.getHost();
            if (host == null || host.isBlank()) {
                DatabaseConfigDto dbDto = request.getDatabase();
                String jdbcUrl = (dbDto != null && dbDto.getJdbcUrl() != null)
                    ? dbDto.getJdbcUrl()
                    : dbConfig.getJdbcUrl();
                DatabaseConfig tmpCfg = new DatabaseConfig();
                tmpCfg.setJdbcUrl(jdbcUrl);
                tmpCfg.setSsh(sshCfg);
                host = tmpCfg.getEffectiveSshHost();
            }

            long startTime = System.currentTimeMillis();
            collector = new SshMetricsCollector(sshCfg, host);
            collector.connect();
            long elapsed = System.currentTimeMillis() - startTime;

            return ResponseEntity.ok(
                SshTestResponse.builder()
                    .success(true)
                    .message(String.format("SSH connection successful (%dms)", elapsed))
                    .host(host)
                    .port(sshCfg.getPort())
                    .responseTime(elapsed)
                    .build()
            );
        } catch (Exception e) {
            log.error("SSH connection test failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(
                SshTestResponse.builder()
                    .success(false)
                    .message("SSH connection failed")
                    .error(e.getMessage())
                    .build()
            );
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
    public ResponseEntity<List<Map<String, Object>>> logs(
            @RequestParam(defaultValue = "100") @Min(1) int limit) {
        var logs = engine.getLogHistory();
        int start = Math.max(0, logs.size() - limit);
        return ResponseEntity.ok(logs.subList(start, logs.size()));
    }

    @DeleteMapping("/logs")
    public ResponseEntity<Void> clearLogs() {
        engine.clearLogs();
        return ResponseEntity.noContent().build();
    }

    // ==================== Profile Management ====================

    @GetMapping("/profiles")
    public ResponseEntity<List<String>> listProfiles() {
        return ResponseEntity.ok(profileService.listProfiles());
    }

    @GetMapping("/profiles/{name}")
    public ResponseEntity<ProfileResponse> loadProfile(@PathVariable String name) throws Exception {
        Map<String, Object> config = profileService.loadProfile(name);
        return ResponseEntity.ok(
            ProfileResponse.builder()
                .success(true)
                .config(config)
                .build()
        );
    }

    @PostMapping("/profiles/{name}")
    public ResponseEntity<ApiResponse<Void>> saveProfile(
            @PathVariable String name,
            @RequestBody Map<String, Object> config) throws Exception {
        profileService.saveProfile(name, config);
        return ResponseEntity.status(HttpStatus.CREATED).body(
            ApiResponse.success("Profile saved: " + name, null)
        );
    }

    @DeleteMapping("/profiles/{name}")
    public ResponseEntity<ApiResponse<Void>> deleteProfile(@PathVariable String name) throws Exception {
        profileService.deleteProfile(name);
        return ResponseEntity.ok(
            ApiResponse.success("Profile deleted: " + name, null)
        );
    }

    @PostMapping("/profiles/{name}/apply")
    public ResponseEntity<ApiResponse<Map<String, Object>>> applyProfile(@PathVariable String name) throws Exception {
        Map<String, Object> config = profileService.loadProfile(name);
        engine.updateConfig(config);
        return ResponseEntity.ok(
            ApiResponse.success("Profile applied: " + name, engine.getConfig(), engine.getStatus())
        );
    }
}
