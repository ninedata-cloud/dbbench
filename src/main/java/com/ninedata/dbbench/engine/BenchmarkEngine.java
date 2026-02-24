package com.ninedata.dbbench.engine;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.database.DatabaseFactory;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.ClientMetricsCollector;
import com.ninedata.dbbench.metrics.SshMetricsCollector;
import com.ninedata.dbbench.tpcc.TPCCUtil;
import com.ninedata.dbbench.tpcc.loader.TPCCLoader;
import com.ninedata.dbbench.tpcc.transaction.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
@Component
public class BenchmarkEngine {
    private final DatabaseConfig dbConfig;
    private final BenchmarkConfig benchConfig;
    private final MetricsRegistry metricsRegistry;
    private final ClientMetricsCollector clientMetricsCollector;

    private DatabaseAdapter adapter;
    private SshMetricsCollector sshCollector;
    private boolean dbHostIsLocal;
    private ExecutorService executorService;
    private ScheduledExecutorService metricsScheduler;
    private DeliveryScheduler deliveryScheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean loading = new AtomicBoolean(false);
    @Getter
    private volatile String status = "IDLE";
    private Consumer<Map<String, Object>> metricsCallback;
    private Consumer<Map<String, Object>> logCallback;

    // Log history
    private final List<Map<String, Object>> logHistory = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_LOG_HISTORY = 1000;

    // Loading progress
    @Getter
    private volatile int loadProgress = 0;
    @Getter
    private volatile String loadMessage = "";
    private volatile TPCCLoader currentLoader = null;

    // Cached hardware info (static, collected once)
    private volatile Map<String, Object> cachedClientHardwareInfo = null;
    private volatile Map<String, Object> cachedDbServerHardwareInfo = null;

    // Previous cumulative values for rate calculation (SQL-sourced host metrics)
    private long lastHostMetricsTime = 0;
    private double lastDiskReadBytes = -1;
    private double lastDiskWriteBytes = -1;
    private double lastNetRecvBytes = -1;
    private double lastNetSentBytes = -1;
    private double lastCpuUserTime = -1;
    private double lastCpuSysTime = -1;
    private double lastCpuIdleTime = -1;

    // TPC-C Keying time means (seconds) per transaction type
    // Index: 0=NewOrder, 1=Payment, 2=OrderStatus, 3=Delivery, 4=StockLevel
    private static final double[] KEYING_TIME_MEAN = {18.0, 3.0, 2.0, 2.0, 2.0};
    // TPC-C Think time means (seconds) per transaction type
    private static final double[] THINK_TIME_MEAN = {12.0, 12.0, 10.0, 5.0, 5.0};

    public BenchmarkEngine(DatabaseConfig dbConfig, BenchmarkConfig benchConfig,
                           MetricsRegistry metricsRegistry, ClientMetricsCollector clientMetricsCollector) {
        this.dbConfig = dbConfig;
        this.benchConfig = benchConfig;
        this.metricsRegistry = metricsRegistry;
        this.clientMetricsCollector = clientMetricsCollector;
    }

    public void setMetricsCallback(Consumer<Map<String, Object>> callback) {
        this.metricsCallback = callback;
    }

    public void setLogCallback(Consumer<Map<String, Object>> callback) {
        this.logCallback = callback;
    }

    private void addLog(String level, String message) {
        Map<String, Object> logEntry = new LinkedHashMap<>();
        logEntry.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        logEntry.put("level", level);
        logEntry.put("message", message);

        logHistory.add(logEntry);
        while (logHistory.size() > MAX_LOG_HISTORY) {
            logHistory.remove(0);
        }

        if (logCallback != null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("type", "log");
            data.put("log", logEntry);
            logCallback.accept(data);
        }

        switch (level) {
            case "ERROR" -> log.error(message);
            case "WARN" -> log.warn(message);
            default -> log.info(message);
        }
    }

    public List<Map<String, Object>> getLogHistory() {
        return new ArrayList<>(logHistory);
    }

    public void clearLogs() {
        logHistory.clear();
    }

    public void updateConfig(Map<String, Object> newConfig) {
        if (running.get() || loading.get()) {
            throw new IllegalStateException("Cannot update config while running or loading");
        }

        if (newConfig.containsKey("database")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> db = (Map<String, Object>) newConfig.get("database");
            if (db.containsKey("type")) dbConfig.setType((String) db.get("type"));
            if (db.containsKey("jdbcUrl")) dbConfig.setJdbcUrl((String) db.get("jdbcUrl"));
            if (db.containsKey("username")) dbConfig.setUsername((String) db.get("username"));
            if (db.containsKey("password")) dbConfig.setPassword((String) db.get("password"));
        }

        if (newConfig.containsKey("benchmark")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> bench = (Map<String, Object>) newConfig.get("benchmark");
            if (bench.containsKey("warehouses")) benchConfig.setWarehouses(((Number) bench.get("warehouses")).intValue());
            if (bench.containsKey("terminals")) benchConfig.setTerminals(((Number) bench.get("terminals")).intValue());
            if (bench.containsKey("duration")) benchConfig.setDuration(((Number) bench.get("duration")).intValue());
            if (bench.containsKey("thinkTime")) benchConfig.setThinkTime((Boolean) bench.get("thinkTime"));
            if (bench.containsKey("loadConcurrency")) benchConfig.setLoadConcurrency(((Number) bench.get("loadConcurrency")).intValue());
            if (bench.containsKey("loadMode")) benchConfig.setLoadMode((String) bench.get("loadMode"));
        }

        if (newConfig.containsKey("transactionMix")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mix = (Map<String, Object>) newConfig.get("transactionMix");
            if (mix.containsKey("newOrder")) benchConfig.getMix().setNewOrder(((Number) mix.get("newOrder")).intValue());
            if (mix.containsKey("payment")) benchConfig.getMix().setPayment(((Number) mix.get("payment")).intValue());
            if (mix.containsKey("orderStatus")) benchConfig.getMix().setOrderStatus(((Number) mix.get("orderStatus")).intValue());
            if (mix.containsKey("delivery")) benchConfig.getMix().setDelivery(((Number) mix.get("delivery")).intValue());
            if (mix.containsKey("stockLevel")) benchConfig.getMix().setStockLevel(((Number) mix.get("stockLevel")).intValue());
        }

        if (adapter != null) {
            adapter.close();
            adapter = null;
            status = "IDLE";
        }

        if (newConfig.containsKey("ssh")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> ssh = (Map<String, Object>) newConfig.get("ssh");
            DatabaseConfig.SshConfig sshCfg = dbConfig.getSsh();
            if (ssh.containsKey("enabled")) sshCfg.setEnabled(Boolean.TRUE.equals(ssh.get("enabled")));
            if (ssh.containsKey("host")) sshCfg.setHost((String) ssh.get("host"));
            if (ssh.containsKey("port")) sshCfg.setPort(((Number) ssh.get("port")).intValue());
            if (ssh.containsKey("username")) sshCfg.setUsername((String) ssh.get("username"));
            if (ssh.containsKey("password")) sshCfg.setPassword((String) ssh.get("password"));
            if (ssh.containsKey("privateKey")) sshCfg.setPrivateKey((String) ssh.get("privateKey"));
            if (ssh.containsKey("passphrase")) sshCfg.setPassphrase((String) ssh.get("passphrase"));
        }

        if (sshCollector != null) {
            sshCollector.disconnect();
            sshCollector = null;
        }
        cachedDbServerHardwareInfo = null;

        addLog("INFO", "Configuration updated");
    }

    public void initialize() throws SQLException {
        status = "INITIALIZING";

        int poolSize = benchConfig.getTerminals() + 10;
        dbConfig.getPool().setSize(poolSize);
        addLog("INFO", String.format("Connection pool size auto-set to %d (terminals %d + 10)", poolSize, benchConfig.getTerminals()));

        addLog("INFO", "Initializing database connection...");
        addLog("INFO", String.format("Database: %s, URL: %s", dbConfig.getType(), dbConfig.getJdbcUrl()));

        if (adapter != null) {
            adapter.close();
            adapter = null;
        }

        try {
            adapter = DatabaseFactory.create(dbConfig);
            adapter.initialize();
            status = "INITIALIZED";
            addLog("INFO", "Database connection initialized successfully");
        } catch (Exception e) {
            adapter = null;
            status = "ERROR";
            throw new SQLException("Failed to initialize database connection: " + e.getMessage(), e);
        }

        connectSshCollector();
    }

    private void connectSshCollector() {
        if (sshCollector != null) {
            sshCollector.disconnect();
            sshCollector = null;
        }
        cachedDbServerHardwareInfo = null;
        dbHostIsLocal = false;

        String host = dbConfig.getEffectiveSshHost();
        if (!host.isBlank() && isLocalHost(host)) {
            dbHostIsLocal = true;
            addLog("INFO", "DB host is local (" + host + "), using OS metrics directly");
        }

        if (dbConfig.getSsh().isEnabled() && !dbHostIsLocal) {
            if (host.isBlank()) {
                addLog("WARN", "SSH enabled but no host could be determined");
                return;
            }
            try {
                sshCollector = new SshMetricsCollector(dbConfig.getSsh(), host);
                sshCollector.connect();
                addLog("INFO", "SSH metrics collector connected to " + host);
            } catch (Exception e) {
                addLog("WARN", "SSH metrics connection failed: " + e.getMessage() + " (will retry on next collection)");
            }
        }
    }

    private static boolean isLocalHost(String host) {
        return "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host) || "::1".equals(host);
    }

    private void ensureInitialized() throws SQLException {
        if (adapter == null || !isAdapterReady()) {
            initialize();
        }
    }

    private boolean isAdapterReady() {
        if (adapter == null) return false;
        try {
            adapter.getConnection().close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isDataLoaded() {
        try (var conn = adapter.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT COUNT(*) FROM warehouse")) {
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    public void loadData(Consumer<String> progressCallback) throws SQLException {
        if (loading.get()) {
            throw new IllegalStateException("Data loading already in progress");
        }

        ensureInitialized();

        loading.set(true);
        status = "LOADING";

        try {
            progressCallback.accept("Dropping existing schema...");
            adapter.dropSchema();

            progressCallback.accept("Creating schema...");
            adapter.createSchema();

            TPCCLoader loader = new TPCCLoader(adapter, benchConfig.getWarehouses(), benchConfig.getLoadConcurrency(), benchConfig.getLoadMode());
            currentLoader = loader;
            loader.setProgressCallback(progressCallback);
            loader.load();
            currentLoader = null;

            progressCallback.accept("Creating indexes...");
            adapter.createIndexes();

            status = "LOADED";
            progressCallback.accept("Data load completed successfully");
        } catch (Exception e) {
            status = "ERROR";
            e.printStackTrace();
            throw new SQLException("Data load failed: " + e.getMessage(), e);
        } finally {
            loading.set(false);
        }
    }

    public void loadDataAsync() throws SQLException {
        if (loading.get()) {
            throw new IllegalStateException("Data loading already in progress");
        }

        ensureInitialized();

        loading.set(true);
        loadProgress = 0;
        loadMessage = "Starting data load...";

        CompletableFuture.runAsync(() -> {
            try {
                status = "LOADING";
                startMetricsCollection();
                addLog("INFO", String.format("Starting TPC-C data load for %d warehouse(s) with %d threads",
                        benchConfig.getWarehouses(), benchConfig.getLoadConcurrency()));

                broadcastLoadProgress(0, "Dropping existing schema...");
                adapter.dropSchema();

                broadcastLoadProgress(5, "Creating schema...");
                adapter.createSchema();
                addLog("INFO", "Schema created successfully");

                TPCCLoader loader = new TPCCLoader(adapter, benchConfig.getWarehouses(), benchConfig.getLoadConcurrency(), benchConfig.getLoadMode());
                currentLoader = loader;
                loader.setStructuredProgressCallback((pct, msg) -> {
                    addLog("INFO", msg);
                    broadcastLoadProgress(pct, msg);
                });
                loader.load();
                currentLoader = null;

                broadcastLoadProgress(95, "Creating indexes...");
                addLog("INFO", "Creating indexes...");
                adapter.createIndexes();

                broadcastLoadProgress(100, "Data load completed");
                status = "LOADED";
                addLog("INFO", "Data load completed successfully");

                broadcastStatusChange("LOADED");
            } catch (Exception e) {
                currentLoader = null;
                if (e.getMessage() != null && e.getMessage().contains("cancelled")) {
                    status = "CANCELLED";
                    addLog("WARN", "Data load cancelled by user");
                    broadcastLoadProgress(-1, "Cancelled by user");
                    broadcastStatusChange("CANCELLED");
                } else {
                    status = "ERROR";
                    e.printStackTrace();
                    addLog("ERROR", "Data load failed: " + e.getMessage());
                    broadcastLoadProgress(-1, "Error: " + e.getMessage());
                    broadcastStatusChange("ERROR");
                }
            } finally {
                loading.set(false);
                currentLoader = null;
                stopMetricsCollection();
            }
        });
    }

    public void cancelLoad() {
        if (!loading.get()) {
            throw new IllegalStateException("No data loading in progress");
        }

        addLog("INFO", "Cancelling data load...");
        if (currentLoader != null) {
            currentLoader.cancel();
        }
    }

    private void broadcastStatusChange(String newStatus) {
        if (logCallback != null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("type", "status");
            data.put("status", newStatus);
            data.put("loading", loading.get());
            data.put("running", running.get());
            logCallback.accept(data);
        }
    }

    private void broadcastLoadProgress(int progress, String message) {
        loadProgress = progress;
        loadMessage = message;

        if (logCallback != null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("type", "progress");
            data.put("progress", progress);
            data.put("message", message);
            data.put("status", status);
            logCallback.accept(data);
        }
    }

    private void startMetricsCollection() {
        stopMetricsCollection();
        long intervalMs = benchConfig.getMetricsInterval();
        metricsScheduler = Executors.newScheduledThreadPool(2);
        metricsScheduler.scheduleAtFixedRate(this::collectAndBroadcastMetrics, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void stopMetricsCollection() {
        if (metricsScheduler != null && !metricsScheduler.isShutdown()) {
            metricsScheduler.shutdownNow();
            metricsScheduler = null;
        }
    }

    public void cleanData() throws SQLException {
        if (running.get() || loading.get()) {
            throw new IllegalStateException("Cannot clean data while running or loading");
        }

        ensureInitialized();

        addLog("INFO", "Cleaning TPC-C data...");
        adapter.dropSchema();
        status = "INITIALIZED";
        addLog("INFO", "Data cleaned successfully");
    }

    public void start() throws SQLException {
        if (running.get()) {
            addLog("WARN", "Benchmark already running");
            return;
        }

        ensureInitialized();

        if (!isDataLoaded()) {
            throw new IllegalStateException("No TPC-C data found. Please load data first.");
        }

        running.set(true);
        status = "RUNNING";
        metricsRegistry.reset();

        // Reset cumulative rate calculation state
        lastHostMetricsTime = 0;
        lastDiskReadBytes = -1;
        lastDiskWriteBytes = -1;
        lastNetRecvBytes = -1;
        lastNetSentBytes = -1;
        lastCpuUserTime = -1;
        lastCpuSysTime = -1;
        lastCpuIdleTime = -1;

        AbstractTransaction.setErrorCallback(this::addLog);

        int terminals = benchConfig.getTerminals();
        executorService = Executors.newFixedThreadPool(terminals);

        // Create async delivery scheduler
        deliveryScheduler = new DeliveryScheduler(adapter, metricsRegistry, benchConfig.getWarehouses());

        int rampup = benchConfig.getRampup();
        int duration = benchConfig.getDuration();

        addLog("INFO", String.format("Starting benchmark: %d terminals, %ds rampup, %ds duration, thinkTime=%s",
                terminals, rampup, duration, benchConfig.isThinkTime()));
        if (benchConfig.isThinkTime()) {
            addLog("INFO", String.format("Keying time multiplier: %.1f, Think time multiplier: %.1f",
                    benchConfig.getKeyingTimeMultiplier(), benchConfig.getThinkTimeMultiplier()));
        }
        addLog("INFO", String.format("Transaction mix: NewOrder=%d%%, Payment=%d%%, OrderStatus=%d%%, Delivery=%d%%, StockLevel=%d%%",
                benchConfig.getMix().getNewOrder(), benchConfig.getMix().getPayment(),
                benchConfig.getMix().getOrderStatus(), benchConfig.getMix().getDelivery(),
                benchConfig.getMix().getStockLevel()));

        broadcastStatusChange("RUNNING");

        startMetricsCollection();

        // Start terminal workers
        for (int i = 0; i < terminals; i++) {
            int terminalId = i + 1;
            int warehouseId = (i % benchConfig.getWarehouses()) + 1;
            int districtId = (i % TPCCUtil.DISTRICTS_PER_WAREHOUSE) + 1;
            executorService.submit(() -> runTerminal(terminalId, warehouseId, districtId));
        }

        // Schedule rampup completion and stop
        metricsScheduler.schedule(() -> {
            addLog("INFO", "Rampup complete, starting measurement");
            metricsRegistry.startRecording();
        }, rampup, TimeUnit.SECONDS);

        metricsScheduler.schedule(this::stop, rampup + duration, TimeUnit.SECONDS);
    }

    private void runTerminal(int terminalId, int warehouseId, int districtId) {
        Random random = new Random();
        int[] weights = {
            benchConfig.getMix().getNewOrder(),
            benchConfig.getMix().getPayment(),
            benchConfig.getMix().getOrderStatus(),
            benchConfig.getMix().getDelivery(),
            benchConfig.getMix().getStockLevel()
        };
        int totalWeight = Arrays.stream(weights).sum();

        // Phase 1: Long-lived connection via TerminalContext
        TerminalContext ctx = null;
        try {
            ctx = new TerminalContext(adapter, warehouseId, districtId, benchConfig.getWarehouses());
        } catch (SQLException e) {
            log.error("Terminal {} failed to get connection: {}", terminalId, e.getMessage());
            return;
        }

        try {
            while (running.get()) {
                // Select transaction based on mix
                int r = random.nextInt(totalWeight);
                int cumulative = 0;
                int txType = 0;
                for (int i = 0; i < weights.length; i++) {
                    cumulative += weights[i];
                    if (r < cumulative) {
                        txType = i;
                        break;
                    }
                }

                // Phase 3a: Keying time (before transaction)
                if (benchConfig.isThinkTime()) {
                    double keyMean = KEYING_TIME_MEAN[txType] * benchConfig.getKeyingTimeMultiplier();
                    long keyTimeMs = (long) (-Math.log(random.nextDouble()) * keyMean * 1000);
                    if (keyTimeMs > 0) {
                        try {
                            Thread.sleep(keyTimeMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }

                if (!running.get()) break;

                // Phase 6: Delivery is async
                if (txType == 3) {
                    deliveryScheduler.submit(warehouseId, districtId);
                } else {
                    AbstractTransaction tx = switch (txType) {
                        case 0 -> new NewOrderTransaction(adapter, warehouseId, districtId);
                        case 1 -> new PaymentTransaction(adapter, warehouseId, districtId);
                        case 2 -> new OrderStatusTransaction(adapter, warehouseId, districtId);
                        case 4 -> new StockLevelTransaction(adapter, warehouseId, districtId);
                        default -> new NewOrderTransaction(adapter, warehouseId, districtId);
                    };

                    long startTime = System.nanoTime();
                    TransactionResult result;
                    try {
                        result = tx.execute(ctx);
                    } catch (Exception e) {
                        // Connection may be broken, try reconnect
                        log.warn("Terminal {} transaction error, reconnecting: {}", terminalId, e.getMessage());
                        boolean reconnected = false;
                        for (int attempt = 1; attempt <= 3 && running.get(); attempt++) {
                            try {
                                Thread.sleep(1000L * attempt);
                                ctx.reconnect();
                                reconnected = true;
                                break;
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (SQLException re) {
                                log.warn("Terminal {} reconnect attempt {}/3 failed: {}", terminalId, attempt, re.getMessage());
                            }
                        }
                        if (!reconnected) {
                            log.error("Terminal {} giving up after 3 reconnect attempts", terminalId);
                            break;
                        }
                        continue;
                    }
                    long latency = System.nanoTime() - startTime;

                    metricsRegistry.recordTransaction(tx.getName(), result, latency);
                }

                // Phase 3a: Think time (after transaction)
                if (benchConfig.isThinkTime()) {
                    double thinkMean = THINK_TIME_MEAN[txType] * benchConfig.getThinkTimeMultiplier();
                    long thinkTimeMs = (long) (-Math.log(random.nextDouble()) * thinkMean * 1000);
                    if (thinkTimeMs > 0) {
                        try {
                            Thread.sleep(thinkTimeMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                } else {
                    // No think time — minimal yield to avoid busy loop
                }
            }
        } finally {
            ctx.close();
        }
    }

    private void collectAndBroadcastMetrics() {
        try {
            Map<String, Object> dbMetrics = adapter.collectMetrics();
            Map<String, Object> clientMetrics = clientMetricsCollector.collect();

            Map<String, Object> hostMetrics;
            if (dbHostIsLocal) {
                hostMetrics = clientMetrics;
            } else if (sshCollector != null && sshCollector.isConnected()) {
                hostMetrics = sshCollector.collect();
            } else if (sshCollector != null) {
                hostMetrics = adapter.collectHostMetrics();
            } else {
                hostMetrics = adapter.collectHostMetrics();
            }

            // Convert cumulative host metrics to per-second rates
            convertCumulativeToRates(hostMetrics);

            metricsRegistry.takeSnapshot(dbMetrics, clientMetrics, hostMetrics);

            if (metricsCallback != null) {
                Map<String, Object> allMetrics = new LinkedHashMap<>();
                allMetrics.put("transaction", metricsRegistry.getCurrentMetrics());
                allMetrics.put("database", dbMetrics);
                allMetrics.put("client", clientMetrics);
                allMetrics.put("dbHost", hostMetrics);
                allMetrics.put("status", status);

                Map<String, Object> sshStatus = new LinkedHashMap<>();
                sshStatus.put("enabled", dbConfig.getSsh().isEnabled());
                sshStatus.put("connected", sshCollector != null && sshCollector.isConnected());
                if (dbConfig.getSsh().isEnabled()) {
                    sshStatus.put("host", dbConfig.getEffectiveSshHost());
                    sshStatus.put("port", dbConfig.getSsh().getPort());
                }
                allMetrics.put("ssh", sshStatus);

                metricsCallback.accept(allMetrics);
            }
        } catch (Exception e) {
            log.warn("Error collecting metrics: {}", e.getMessage());
        }
    }

    /**
     * Convert cumulative byte counters (from SQL queries) to per-second rates.
     * If the metrics already contain *PerSec fields (e.g. from SSH), they are left as-is.
     */
    private void convertCumulativeToRates(Map<String, Object> metrics) {
        if (metrics == null || metrics.isEmpty()) return;

        // Skip if already has per-sec fields (SSH or OS-level collectors provide these directly)
        if (metrics.containsKey("diskReadBytesPerSec")) return;

        long now = System.currentTimeMillis();
        double diskRead = toDouble(metrics.get("diskReadBytes"));
        double diskWrite = toDouble(metrics.get("diskWriteBytes"));
        double netRecv = toDouble(metrics.get("networkRecvBytes"));
        double netSent = toDouble(metrics.get("networkSentBytes"));
        double cpuUser = toDouble(metrics.get("cpuUserTime"));
        double cpuSys = toDouble(metrics.get("cpuSysTime"));
        double cpuIdle = toDouble(metrics.get("cpuIdleTime"));

        if (lastHostMetricsTime > 0) {
            double elapsed = (now - lastHostMetricsTime) / 1000.0;
            if (elapsed > 0) {
                if (lastDiskReadBytes >= 0 && diskRead >= 0) {
                    metrics.put("diskReadBytesPerSec", Math.max(0, (diskRead - lastDiskReadBytes) / elapsed));
                    metrics.put("diskWriteBytesPerSec", Math.max(0, (diskWrite - lastDiskWriteBytes) / elapsed));
                }
                if (lastNetRecvBytes >= 0 && netRecv >= 0) {
                    metrics.put("networkRecvBytesPerSec", Math.max(0, (netRecv - lastNetRecvBytes) / elapsed));
                    metrics.put("networkSentBytesPerSec", Math.max(0, (netSent - lastNetSentBytes) / elapsed));
                }
                // CPU cumulative time -> usage percentage (e.g. HANA M_HOST_RESOURCE_UTILIZATION)
                if (lastCpuUserTime >= 0 && cpuUser >= 0) {
                    double deltaUser = cpuUser - lastCpuUserTime;
                    double deltaSys = cpuSys - lastCpuSysTime;
                    double deltaIdle = cpuIdle - lastCpuIdleTime;
                    double deltaTotal = deltaUser + deltaSys + deltaIdle;
                    if (deltaTotal > 0) {
                        metrics.put("cpuUsage", (deltaUser + deltaSys) / deltaTotal * 100.0);
                    }
                }
            }
        }

        lastHostMetricsTime = now;
        if (diskRead >= 0) { lastDiskReadBytes = diskRead; lastDiskWriteBytes = diskWrite; }
        if (netRecv >= 0) { lastNetRecvBytes = netRecv; lastNetSentBytes = netSent; }
        if (cpuUser >= 0) { lastCpuUserTime = cpuUser; lastCpuSysTime = cpuSys; lastCpuIdleTime = cpuIdle; }
    }

    private static double toDouble(Object val) {
        if (val instanceof Number) return ((Number) val).doubleValue();
        if (val == null) return -1;
        try { return Double.parseDouble(val.toString()); } catch (NumberFormatException e) { return -1; }
    }

    public void stop() {
        if (!running.get()) {
            return;
        }
        running.set(false);
        status = "STOPPING";
        metricsRegistry.stopRecording();
        metricsRegistry.markEnd();

        addLog("INFO", "Stopping benchmark...");

        if (deliveryScheduler != null) {
            deliveryScheduler.shutdown();
            deliveryScheduler = null;
        }

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                    executorService.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        stopMetricsCollection();

        status = "STOPPED";
        addLog("INFO", "Benchmark stopped");

        Map<String, Object> metrics = metricsRegistry.getCurrentMetrics();
        addLog("INFO", String.format("Final Results: NOPM=%d, TPS=%d, Total=%d, Success=%.2f%%, AvgLatency=%.2fms",
                metrics.get("nopm"), metrics.get("tps"), metrics.get("totalTransactions"),
                metrics.get("overallSuccessRate"), metrics.get("avgLatencyMs")));

        collectAndBroadcastMetrics();

        broadcastStatusChange("STOPPED");
    }

    public void shutdown() {
        stop();
        if (sshCollector != null) {
            sshCollector.disconnect();
            sshCollector = null;
        }
        if (adapter != null) {
            adapter.close();
        }
        status = "SHUTDOWN";
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isLoading() {
        return loading.get();
    }

    public Map<String, Object> getResults() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("status", status);
        results.put("metrics", metricsRegistry.getCurrentMetrics());
        return results;
    }

    public Map<String, Object> getHardwareInfo() {
        Map<String, Object> info = new LinkedHashMap<>();

        if (cachedClientHardwareInfo == null) {
            cachedClientHardwareInfo = clientMetricsCollector.collectHardwareInfo();
        }
        info.put("client", cachedClientHardwareInfo);

        if (cachedDbServerHardwareInfo == null && sshCollector != null && sshCollector.isConnected()) {
            try {
                cachedDbServerHardwareInfo = sshCollector.collectHardwareInfo();
            } catch (Exception e) {
                log.warn("Failed to collect DB server hardware info: {}", e.getMessage());
            }
        }
        info.put("dbServer", cachedDbServerHardwareInfo != null ? cachedDbServerHardwareInfo : Collections.emptyMap());

        return info;
    }

    public Map<String, Object> getConfig() {
        Map<String, Object> config = new LinkedHashMap<>();

        Map<String, Object> db = new LinkedHashMap<>();
        db.put("type", dbConfig.getType());
        db.put("jdbcUrl", dbConfig.getJdbcUrl());
        db.put("username", dbConfig.getUsername());
        db.put("poolSize", dbConfig.getPool().getSize());
        config.put("database", db);

        Map<String, Object> bench = new LinkedHashMap<>();
        bench.put("warehouses", benchConfig.getWarehouses());
        bench.put("terminals", benchConfig.getTerminals());
        bench.put("duration", benchConfig.getDuration());
        bench.put("rampup", benchConfig.getRampup());
        bench.put("thinkTime", benchConfig.isThinkTime());
        bench.put("keyingTimeMultiplier", benchConfig.getKeyingTimeMultiplier());
        bench.put("thinkTimeMultiplier", benchConfig.getThinkTimeMultiplier());
        bench.put("loadConcurrency", benchConfig.getLoadConcurrency());
        bench.put("loadMode", benchConfig.getLoadMode());
        config.put("benchmark", bench);

        Map<String, Object> mix = new LinkedHashMap<>();
        mix.put("newOrder", benchConfig.getMix().getNewOrder());
        mix.put("payment", benchConfig.getMix().getPayment());
        mix.put("orderStatus", benchConfig.getMix().getOrderStatus());
        mix.put("delivery", benchConfig.getMix().getDelivery());
        mix.put("stockLevel", benchConfig.getMix().getStockLevel());
        config.put("transactionMix", mix);

        Map<String, Object> ssh = new LinkedHashMap<>();
        ssh.put("enabled", dbConfig.getSsh().isEnabled());
        ssh.put("host", dbConfig.getEffectiveSshHost());
        ssh.put("port", dbConfig.getSsh().getPort());
        ssh.put("username", dbConfig.getSsh().getUsername());
        ssh.put("hasPassword", dbConfig.getSsh().getPassword() != null && !dbConfig.getSsh().getPassword().isBlank());
        ssh.put("hasPrivateKey", dbConfig.getSsh().getPrivateKey() != null && !dbConfig.getSsh().getPrivateKey().isBlank());
        ssh.put("connected", sshCollector != null && sshCollector.isConnected());
        config.put("ssh", ssh);

        return config;
    }
}
