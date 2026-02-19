package com.ninedata.dbbench.cli;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.ClientMetricsCollector;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Command(name = "dbbench", mixinStandardHelpOptions = true, version = "0.6.0",
        description = "NineData DBBench - TPC-C Database Benchmark Tool%n%n" +
                "Usage examples:%n" +
                "  dbbench -f config.properties load        Load test data%n" +
                "  dbbench -f config.properties run         Run benchmark%n" +
                "  dbbench -f config.properties clean       Clean data%n" +
                "  dbbench -f config.properties clean load run%n")
public class CLIRunner implements Callable<Integer> {

    @Option(names = {"-f", "--config"}, required = true,
            description = "Configuration file path (profiles/*.properties)")
    private String configFile;

    // Optional overrides
    @Option(names = {"-w", "--warehouses"}, description = "Override warehouses count")
    private Integer warehouses;

    @Option(names = {"-c", "--terminals"}, description = "Override terminal count")
    private Integer terminals;

    @Option(names = {"-d", "--duration"}, description = "Override test duration (seconds)")
    private Integer duration;

    @Parameters(description = "Actions: clean, load, run (can combine multiple)")
    private List<String> actions;

    public static void run(String[] args) {
        int exitCode = new CommandLine(new CLIRunner()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        printBanner();

        // Validate actions
        if (actions == null || actions.isEmpty()) {
            System.err.println("Error: No action specified. Use: clean, load, run");
            return 1;
        }
        Set<String> validActions = Set.of("clean", "load", "run");
        for (String action : actions) {
            if (!validActions.contains(action.toLowerCase())) {
                System.err.println("Error: Unknown action '" + action + "'. Valid actions: clean, load, run");
                return 1;
            }
        }
        Set<String> actionSet = new LinkedHashSet<>();
        actions.forEach(a -> actionSet.add(a.toLowerCase()));

        // Load config file
        Path configPath = Paths.get(configFile);
        if (!Files.exists(configPath)) {
            System.err.println("Error: Configuration file not found: " + configFile);
            return 1;
        }

        Properties props = new Properties();
        try (Reader reader = Files.newBufferedReader(configPath)) {
            props.load(reader);
        }
        System.out.println("  Config: " + configPath.toAbsolutePath());
        System.out.println("  Actions: " + String.join(" → ", actionSet));

        // Build DatabaseConfig
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setType(props.getProperty("db.type", "mysql"));
        dbConfig.setJdbcUrl(props.getProperty("db.jdbc-url", ""));
        dbConfig.setUsername(props.getProperty("db.username", "root"));
        dbConfig.setPassword(props.getProperty("db.password", ""));
        dbConfig.getPool().setMinIdle(intVal(props, "db.pool.min-idle", 10));

        // SSH config
        dbConfig.getSsh().setEnabled(Boolean.parseBoolean(props.getProperty("db.ssh.enabled", "false")));
        dbConfig.getSsh().setHost(props.getProperty("db.ssh.host", ""));
        dbConfig.getSsh().setPort(intVal(props, "db.ssh.port", 22));
        dbConfig.getSsh().setUsername(props.getProperty("db.ssh.username", "root"));
        dbConfig.getSsh().setPassword(props.getProperty("db.ssh.password", ""));
        dbConfig.getSsh().setPrivateKey(props.getProperty("db.ssh.private-key", ""));

        // Build BenchmarkConfig
        BenchmarkConfig benchConfig = new BenchmarkConfig();
        benchConfig.setWarehouses(intVal(props, "benchmark.warehouses", 10));
        benchConfig.setTerminals(intVal(props, "benchmark.terminals", 50));
        benchConfig.setDuration(intVal(props, "benchmark.duration", 60));
        benchConfig.setThinkTime(Boolean.parseBoolean(props.getProperty("benchmark.think-time", "false")));
        benchConfig.setLoadConcurrency(intVal(props, "benchmark.load-concurrency", 4));
        benchConfig.setLoadMode(props.getProperty("benchmark.load-mode", "auto"));
        benchConfig.getMix().setNewOrder(intVal(props, "benchmark.mix.new-order", 45));
        benchConfig.getMix().setPayment(intVal(props, "benchmark.mix.payment", 43));
        benchConfig.getMix().setOrderStatus(intVal(props, "benchmark.mix.order-status", 4));
        benchConfig.getMix().setDelivery(intVal(props, "benchmark.mix.delivery", 4));
        benchConfig.getMix().setStockLevel(intVal(props, "benchmark.mix.stock-level", 4));

        // Apply command-line overrides
        if (warehouses != null) benchConfig.setWarehouses(warehouses);
        if (terminals != null) benchConfig.setTerminals(terminals);
        if (duration != null) benchConfig.setDuration(duration);

        // Validate JDBC URL
        if (dbConfig.getJdbcUrl().isEmpty()) {
            System.err.println("Error: db.jdbc-url is not set in config file");
            return 1;
        }

        printConfig(dbConfig, benchConfig);

        // Create engine
        MetricsRegistry metricsRegistry = new MetricsRegistry();
        ClientMetricsCollector clientMetricsCollector = new ClientMetricsCollector();
        BenchmarkEngine engine = new BenchmarkEngine(dbConfig, benchConfig, metricsRegistry, clientMetricsCollector);

        try {
            System.out.println("\n  Connecting to database...");
            engine.initialize();
            System.out.println("  Connected successfully.\n");

            // Execute actions in order
            if (actionSet.contains("clean")) {
                System.out.println("══════════════════════════════════════════");
                System.out.println("  CLEAN DATA");
                System.out.println("══════════════════════════════════════════");
                engine.cleanData();
                System.out.println("  Data cleaned successfully.\n");
            }

            if (actionSet.contains("load")) {
                System.out.println("══════════════════════════════════════════");
                System.out.println("  LOAD DATA");
                System.out.println("══════════════════════════════════════════");
                long loadStart = System.currentTimeMillis();
                engine.loadData(msg -> System.out.println("  " + msg));
                long loadTime = (System.currentTimeMillis() - loadStart) / 1000;
                System.out.printf("  Data loaded in %d seconds.%n%n", loadTime);
            }

            if (actionSet.contains("run")) {
                System.out.println("══════════════════════════════════════════");
                System.out.println("  RUN BENCHMARK");
                System.out.printf("  Warehouses: %d  Terminals: %d  Duration: %ds%n",
                        benchConfig.getWarehouses(), benchConfig.getTerminals(), benchConfig.getDuration());
                System.out.println("══════════════════════════════════════════\n");

                engine.start();

                // Real-time metrics display
                ScheduledExecutorService display = Executors.newSingleThreadScheduledExecutor();
                display.scheduleAtFixedRate(() -> {
                    Map<String, Object> m = metricsRegistry.getCurrentMetrics();
                    if (m != null && m.containsKey("tps")) {
                        System.out.printf("\r  TPS: %8.1f | Tx: %6d | Success: %5.1f%% | Latency: %6.2f ms",
                                m.get("tps"), m.get("totalTransactions"),
                                m.get("overallSuccessRate"), m.get("avgLatencyMs"));
                    }
                }, 1, 1, TimeUnit.SECONDS);

                // Wait for benchmark to complete
                Thread.sleep(benchConfig.getDuration() * 1000L + 2000);

                display.shutdown();
                engine.stop();

                System.out.println("\n");
                Map<String, Object> results = metricsRegistry.getCurrentMetrics();
                if (results != null) {
                    printResults(results);
                }
            }

            return 0;
        } catch (Exception e) {
            System.err.println("\nError: " + e.getMessage());
            e.printStackTrace();
            return 1;
        } finally {
            engine.stop();
        }
    }

    private int intVal(Properties props, String key, int defaultVal) {
        String val = props.getProperty(key);
        if (val == null) return defaultVal;
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    private void printConfig(DatabaseConfig db, BenchmarkConfig bench) {
        System.out.println("\n  ┌─────────────────────────────────────────┐");
        System.out.println("  │           Configuration                 │");
        System.out.println("  ├─────────────────────────────────────────┤");
        System.out.printf("  │  Database:    %-26s│%n", db.getType());
        System.out.printf("  │  JDBC URL:    %-26s│%n", truncate(db.getJdbcUrl(), 26));
        System.out.printf("  │  User:        %-26s│%n", db.getUsername());
        System.out.printf("  │  Warehouses:  %-26d│%n", bench.getWarehouses());
        System.out.printf("  │  Terminals:   %-26d│%n", bench.getTerminals());
        System.out.printf("  │  Duration:    %-26s│%n", bench.getDuration() + "s");
        System.out.printf("  │  Load Mode:   %-26s│%n", bench.getLoadMode());
        System.out.println("  └─────────────────────────────────────────┘");
    }

    private String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 3) + "...";
    }

    private void printBanner() {
        System.out.println();
        System.out.println("  _   _ _            ____        _        ");
        System.out.println(" | \\ | (_)_ __   ___|  _ \\  __ _| |_ __ _ ");
        System.out.println(" |  \\| | | '_ \\ / _ \\ | | |/ _` | __/ _` |");
        System.out.println(" | |\\  | | | | |  __/ |_| | (_| | || (_| |");
        System.out.println(" |_| \\_|_|_| |_|\\___|____/ \\__,_|\\__\\__,_|");
        System.out.println();
        System.out.println("  ____  ____  ____                  _     ");
        System.out.println(" |  _ \\| __ )| __ )  ___ _ __   ___| |__  ");
        System.out.println(" | | | |  _ \\|  _ \\ / _ \\ '_ \\ / __| '_ \\ ");
        System.out.println(" | |_| | |_) | |_) |  __/ | | | (__| | | |");
        System.out.println(" |____/|____/|____/ \\___|_| |_|\\___|_| |_|");
        System.out.println();
        System.out.println("  TPC-C Database Benchmark Tool  v0.6.0");
        System.out.println();
    }

    private void printResults(Map<String, Object> metrics) {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║                    BENCHMARK RESULTS                      ║");
        System.out.println("╠═══════════════════════════════════════════════════════════╣");
        System.out.printf("║  Throughput (TPS):        %10.2f                      ║%n", toDouble(metrics.get("tps")));
        System.out.printf("║  Total Transactions:      %10d                      ║%n", toInt(metrics.get("totalTransactions")));
        System.out.printf("║  Successful:              %10d                      ║%n", toInt(metrics.get("totalSuccess")));
        System.out.printf("║  Failed:                  %10d                      ║%n", toInt(metrics.get("totalFailure")));
        System.out.printf("║  Success Rate:            %10.2f%%                     ║%n", toDouble(metrics.get("overallSuccessRate")));
        System.out.printf("║  Average Latency:         %10.2f ms                   ║%n", toDouble(metrics.get("avgLatencyMs")));
        System.out.printf("║  Duration:                %10d seconds               ║%n", toInt(metrics.get("elapsedSeconds")));
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
    }

    private double toDouble(Object val) {
        if (val instanceof Number) return ((Number) val).doubleValue();
        return 0.0;
    }

    private int toInt(Object val) {
        if (val instanceof Number) return ((Number) val).intValue();
        return 0;
    }
}
