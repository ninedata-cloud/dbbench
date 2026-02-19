package com.ninedata.dbbench.report;

import com.ninedata.dbbench.config.BenchmarkConfig;
import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.engine.BenchmarkEngine;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.metrics.MetricsSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@RequiredArgsConstructor
public class ReportGenerator {
    private final BenchmarkEngine engine;
    private final MetricsRegistry metricsRegistry;
    private final BenchmarkConfig benchConfig;
    private final DatabaseConfig dbConfig;

    public String generateMarkdown() {
        StringBuilder sb = new StringBuilder();
        Map<String, Object> metrics = metricsRegistry.getCurrentMetrics();
        Map<String, Object> hwInfo = engine.getHardwareInfo();
        List<MetricsSnapshot> history = metricsRegistry.getHistorySlice(0, metricsRegistry.getHistorySize());

        appendTitle(sb);
        appendEnvironment(sb, hwInfo);
        appendBenchmarkConfig(sb);
        appendDatabaseConfig(sb);
        appendOverallResults(sb, metrics);
        appendTransactionDetails(sb, metrics);
        appendPerformanceTrend(sb, history);
        appendMonitoringCharts(sb, history);
        appendResourceUsage(sb, history);
        appendSummary(sb, metrics, history);

        return sb.toString();
    }

    private void appendTitle(StringBuilder sb) {
        sb.append("# DBBench TPC-C Benchmark Report\n\n");
        sb.append("**Generated:** ").append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n\n");
    }

    @SuppressWarnings("unchecked")
    private void appendEnvironment(StringBuilder sb, Map<String, Object> hwInfo) {
        sb.append("## Test Environment\n\n");
        Map<String, Object> client = (Map<String, Object>) hwInfo.getOrDefault("client", Collections.emptyMap());
        Map<String, Object> dbServer = (Map<String, Object>) hwInfo.getOrDefault("dbServer", Collections.emptyMap());

        sb.append("### Client\n\n");
        sb.append("| Item | Value |\n|------|-------|\n");
        appendHwRow(sb, client, "OS", "os");
        appendHwRow(sb, client, "CPU", "cpu");
        appendHwCores(sb, client);
        appendHwMemory(sb, client);
        appendHwRow(sb, client, "Disk", "disks");
        appendHwRow(sb, client, "Network", "network");
        sb.append("\n");

        if (!dbServer.isEmpty()) {
            sb.append("### Database Server\n\n");
            sb.append("| Item | Value |\n|------|-------|\n");
            appendHwRow(sb, dbServer, "OS", "os");
            appendHwRow(sb, dbServer, "CPU", "cpu");
            appendHwCores(sb, dbServer);
            appendHwMemory(sb, dbServer);
            appendHwRow(sb, dbServer, "Disk", "disks");
            appendHwRow(sb, dbServer, "Network", "network");
            sb.append("\n");
        }
    }

    private void appendHwRow(StringBuilder sb, Map<String, Object> hw, String label, String key) {
        Object val = hw.get(key);
        if (val != null) sb.append("| ").append(label).append(" | ").append(val).append(" |\n");
    }

    private void appendHwCores(StringBuilder sb, Map<String, Object> hw) {
        StringBuilder cores = new StringBuilder();
        if (hw.containsKey("cpuPhysicalCores")) cores.append(hw.get("cpuPhysicalCores")).append("P");
        if (hw.containsKey("cpuLogicalCores")) {
            if (!cores.isEmpty()) cores.append("/");
            cores.append(hw.get("cpuLogicalCores")).append("L");
        }
        if (hw.containsKey("cpuFreqMHz")) {
            Number freq = (Number) hw.get("cpuFreqMHz");
            if (freq.doubleValue() > 0) cores.append(" @ ").append(freq).append(" MHz");
        }
        if (!cores.isEmpty()) sb.append("| Cores | ").append(cores).append(" |\n");
    }

    private void appendHwMemory(StringBuilder sb, Map<String, Object> hw) {
        if (hw.containsKey("memoryTotalGB")) {
            sb.append("| Memory | ").append(hw.get("memoryTotalGB")).append(" GB |\n");
        }
    }

    private void appendBenchmarkConfig(StringBuilder sb) {
        sb.append("## Benchmark Configuration\n\n");
        sb.append("| Parameter | Value |\n|-----------|-------|\n");
        sb.append("| Warehouses | ").append(benchConfig.getWarehouses()).append(" |\n");
        sb.append("| Terminals | ").append(benchConfig.getTerminals()).append(" |\n");
        sb.append("| Duration | ").append(benchConfig.getDuration()).append("s |\n");
        sb.append("| Think Time | ").append(benchConfig.isThinkTime() ? "Enabled" : "Disabled").append(" |\n");
        sb.append("| Transaction Mix | NewOrder=").append(benchConfig.getMix().getNewOrder()).append("%, ");
        sb.append("Payment=").append(benchConfig.getMix().getPayment()).append("%, ");
        sb.append("OrderStatus=").append(benchConfig.getMix().getOrderStatus()).append("%, ");
        sb.append("Delivery=").append(benchConfig.getMix().getDelivery()).append("%, ");
        sb.append("StockLevel=").append(benchConfig.getMix().getStockLevel()).append("% |\n\n");
    }

    private void appendDatabaseConfig(StringBuilder sb) {
        sb.append("## Database Configuration\n\n");
        sb.append("| Parameter | Value |\n|-----------|-------|\n");
        sb.append("| Type | ").append(dbConfig.getType().toUpperCase()).append(" |\n");
        String url = dbConfig.getJdbcUrl();
        // Mask password in URL if present
        url = url.replaceAll("password=[^&]*", "password=***");
        sb.append("| JDBC URL | ").append(url).append(" |\n");
        sb.append("| Username | ").append(dbConfig.getUsername()).append(" |\n");
        sb.append("| Connection Pool | ").append(dbConfig.getPool().getSize()).append(" |\n\n");
    }

    @SuppressWarnings("unchecked")
    private void appendOverallResults(StringBuilder sb, Map<String, Object> metrics) {
        sb.append("## Overall Results\n\n");

        // Highlight NOPM and TPM
        sb.append("> **NOPM: ").append(metrics.getOrDefault("nopm", 0));
        sb.append(" | TPM: ").append(metrics.getOrDefault("tpm", 0)).append("**\n\n");

        sb.append("| Metric | Value |\n|--------|-------|\n");
        sb.append("| NOPM (New Orders Per Minute) | ").append(metrics.getOrDefault("nopm", 0)).append(" |\n");
        sb.append("| Total Transactions | ").append(metrics.getOrDefault("totalTransactions", 0)).append(" |\n");
        sb.append("| TPS | ").append(metrics.getOrDefault("tps", 0)).append(" |\n");
        sb.append("| TPM | ").append(metrics.getOrDefault("tpm", 0)).append(" |\n");
        sb.append("| New Order TPM (tpmC) | ").append(metrics.getOrDefault("newOrderTpm", 0)).append(" |\n");
        sb.append("| New Order Count | ").append(metrics.getOrDefault("newOrderCount", 0)).append(" |\n");
        sb.append("| New Order Success | ").append(metrics.getOrDefault("newOrderSuccess", 0)).append(" |\n");
        sb.append("| New Order Avg Latency | ").append(metrics.getOrDefault("newOrderAvgLatencyMs", 0)).append(" ms |\n");
        sb.append("| Success | ").append(metrics.getOrDefault("totalSuccess", 0)).append(" |\n");
        sb.append("| Rollback | ").append(metrics.getOrDefault("totalRollback", 0)).append(" |\n");
        sb.append("| Error | ").append(metrics.getOrDefault("totalError", 0)).append(" |\n");
        sb.append("| Success Rate | ").append(metrics.getOrDefault("overallSuccessRate", 0)).append("% |\n");
        sb.append("| Average Latency | ").append(metrics.getOrDefault("avgLatencyMs", 0)).append(" ms |\n");
        sb.append("| Elapsed Time | ").append(metrics.getOrDefault("elapsedSeconds", 0)).append("s |\n\n");
        sb.append("> **Rollback** = expected TPC-C business rollbacks (e.g. invalid item, customer not found). ");
        sb.append("**Error** = unexpected failures (connection issues, SQL exceptions).\n\n");
    }

    @SuppressWarnings("unchecked")
    private void appendTransactionDetails(StringBuilder sb, Map<String, Object> metrics) {
        sb.append("## Transaction Details\n\n");
        sb.append("| Type | Count | Success | Rollback | Error | Success Rate | Avg Latency | Min Latency | Max Latency |\n");
        sb.append("|------|-------|---------|----------|-------|-------------|-------------|-------------|-------------|\n");
        List<Map<String, Object>> txList = (List<Map<String, Object>>) metrics.get("transactions");
        if (txList != null) {
            for (Map<String, Object> tx : txList) {
                sb.append("| ").append(tx.get("name"));
                sb.append(" | ").append(tx.get("count"));
                sb.append(" | ").append(tx.get("success"));
                sb.append(" | ").append(tx.getOrDefault("rollback", 0));
                sb.append(" | ").append(tx.getOrDefault("error", 0));
                sb.append(" | ").append(tx.get("successRate")).append("%");
                sb.append(" | ").append(tx.get("avgLatencyMs")).append("ms");
                sb.append(" | ").append(tx.get("minLatencyMs")).append("ms");
                sb.append(" | ").append(tx.get("maxLatencyMs")).append("ms |\n");
            }
        }
        sb.append("\n");
    }

    private void appendPerformanceTrend(StringBuilder sb, List<MetricsSnapshot> history) {
        sb.append("## Performance Trend\n\n");
        if (history.isEmpty()) {
            sb.append("No historical data available.\n\n");
            return;
        }
        double maxTps = 0, minTps = Double.MAX_VALUE, sumTps = 0;
        double maxNoTpm = 0, minNoTpm = Double.MAX_VALUE, sumNoTpm = 0;
        int count = 0;
        int noCount = 0;
        for (MetricsSnapshot snap : history) {
            Map<String, Object> tx = snap.getTransactionMetrics();
            if (tx == null) continue;
            Object tpsObj = tx.get("tps");
            if (tpsObj instanceof Number) {
                double tps = ((Number) tpsObj).doubleValue();
                maxTps = Math.max(maxTps, tps);
                minTps = Math.min(minTps, tps);
                sumTps += tps;
                count++;
            }
            Object noTpmObj = tx.get("newOrderTpm");
            if (noTpmObj instanceof Number) {
                double tpm = ((Number) noTpmObj).doubleValue();
                maxNoTpm = Math.max(maxNoTpm, tpm);
                minNoTpm = Math.min(minNoTpm, tpm);
                sumNoTpm += tpm;
                noCount++;
            }
        }
        if (count == 0) {
            sb.append("No TPS data available.\n\n");
            return;
        }
        sb.append("| Metric | Average | Peak | Minimum |\n|--------|---------|------|---------|\n");
        sb.append("| TPS | ").append(Math.round(sumTps / count));
        sb.append(" | ").append(Math.round(maxTps));
        sb.append(" | ").append(Math.round(minTps)).append(" |\n");
        sb.append("| TPM | ").append(Math.round(sumTps / count * 60));
        sb.append(" | ").append(Math.round(maxTps * 60));
        sb.append(" | ").append(Math.round(minTps * 60)).append(" |\n");
        if (noCount > 0) {
            sb.append("| New Order TPM | ").append(Math.round(sumNoTpm / noCount));
            sb.append(" | ").append(Math.round(maxNoTpm));
            sb.append(" | ").append(Math.round(minNoTpm)).append(" |\n");
        }
        sb.append("| Data Points | ").append(count).append(" | | |\n\n");
    }

    private void appendResourceUsage(StringBuilder sb, List<MetricsSnapshot> history) {
        sb.append("## System Resource Usage\n\n");
        if (history.isEmpty()) {
            sb.append("No resource data available.\n\n");
            return;
        }

        // Client metrics
        double sumCpu = 0, maxCpu = 0, sumMem = 0, maxMem = 0;
        int clientCount = 0;
        // DB host metrics
        double sumDbCpu = 0, maxDbCpu = 0, sumDbMem = 0, maxDbMem = 0;
        int dbCount = 0;

        for (MetricsSnapshot snap : history) {
            Map<String, Object> cm = snap.getClientMetrics();
            if (cm != null) {
                double cpu = getDouble(cm, "cpuUsage");
                double mem = getDouble(cm, "memoryUsage");
                if (cpu >= 0) { sumCpu += cpu; maxCpu = Math.max(maxCpu, cpu); clientCount++; }
                if (mem >= 0) { sumMem += mem; maxMem = Math.max(maxMem, mem); }
            }
            Map<String, Object> hm = snap.getDbHostMetrics();
            if (hm != null && !hm.isEmpty()) {
                double cpu = getDouble(hm, "cpuUsage");
                double mem = getDouble(hm, "memoryUsage");
                if (cpu >= 0) { sumDbCpu += cpu; maxDbCpu = Math.max(maxDbCpu, cpu); dbCount++; }
                if (mem >= 0) { sumDbMem += mem; maxDbMem = Math.max(maxDbMem, mem); }
            }
        }

        sb.append("### Client\n\n");
        sb.append("| Metric | Average | Peak |\n|--------|---------|------|\n");
        if (clientCount > 0) {
            sb.append("| CPU Usage | ").append(String.format("%.1f%%", sumCpu / clientCount));
            sb.append(" | ").append(String.format("%.1f%%", maxCpu)).append(" |\n");
            sb.append("| Memory Usage | ").append(String.format("%.1f%%", sumMem / clientCount));
            sb.append(" | ").append(String.format("%.1f%%", maxMem)).append(" |\n");
        }
        sb.append("\n");

        if (dbCount > 0) {
            sb.append("### Database Server\n\n");
            sb.append("| Metric | Average | Peak |\n|--------|---------|------|\n");
            sb.append("| CPU Usage | ").append(String.format("%.1f%%", sumDbCpu / dbCount));
            sb.append(" | ").append(String.format("%.1f%%", maxDbCpu)).append(" |\n");
            sb.append("| Memory Usage | ").append(String.format("%.1f%%", sumDbMem / dbCount));
            sb.append(" | ").append(String.format("%.1f%%", maxDbMem)).append(" |\n");
            sb.append("\n");
        }
    }

    private void appendSummary(StringBuilder sb, Map<String, Object> metrics, List<MetricsSnapshot> history) {
        sb.append("## Summary\n\n");
        sb.append("- **Database:** ").append(dbConfig.getType().toUpperCase()).append("\n");
        sb.append("- **Warehouses:** ").append(benchConfig.getWarehouses()).append("\n");
        sb.append("- **Terminals:** ").append(benchConfig.getTerminals()).append("\n");
        sb.append("- **Duration:** ").append(metrics.getOrDefault("elapsedSeconds", 0)).append("s\n");
        sb.append("- **Total Transactions:** ").append(metrics.getOrDefault("totalTransactions", 0)).append("\n");
        sb.append("- **TPS:** ").append(metrics.getOrDefault("tps", 0)).append("\n");
        sb.append("- **TPM:** ").append(metrics.getOrDefault("tpm", 0)).append("\n");
        sb.append("- **New Order TPM (tpmC):** ").append(metrics.getOrDefault("newOrderTpm", 0)).append("\n");
        sb.append("- **Success Rate:** ").append(metrics.getOrDefault("overallSuccessRate", 0)).append("%\n");
        sb.append("- **Average Latency:** ").append(metrics.getOrDefault("avgLatencyMs", 0)).append("ms\n");

        if (!history.isEmpty()) {
            double sumTps = 0;
            double maxTps = 0;
            int count = 0;
            for (MetricsSnapshot snap : history) {
                Map<String, Object> tx = snap.getTransactionMetrics();
                if (tx == null) continue;
                Object tpsObj = tx.get("tps");
                if (tpsObj instanceof Number) {
                    double tps = ((Number) tpsObj).doubleValue();
                    maxTps = Math.max(maxTps, tps);
                    sumTps += tps;
                    count++;
                }
            }
            if (count > 0) {
                sb.append("- **Average TPS:** ").append(Math.round(sumTps / count)).append("\n");
                sb.append("- **Peak TPS:** ").append(Math.round(maxTps)).append("\n");
            }
        }
        sb.append("\n---\n*Report generated by NineData DBBench*\n");
    }

    private void appendMonitoringCharts(StringBuilder sb, List<MetricsSnapshot> history) {
        if (history.size() < 2) return;

        sb.append("## Monitoring Charts\n\n");

        // Collect data series from history
        List<Double> tpsList = new ArrayList<>();
        List<Double> noTpmList = new ArrayList<>();
        List<Double> latencyList = new ArrayList<>();
        List<Double> clientCpuList = new ArrayList<>();
        List<Double> dbCpuList = new ArrayList<>();

        for (MetricsSnapshot snap : history) {
            Map<String, Object> tx = snap.getTransactionMetrics();
            if (tx != null) {
                tpsList.add(getDouble(tx, "tps"));
                noTpmList.add(getDouble(tx, "newOrderTpm"));
                latencyList.add(getDouble(tx, "avgLatencyMs"));
            }
            Map<String, Object> cm = snap.getClientMetrics();
            clientCpuList.add(cm != null ? getDouble(cm, "cpuUsage") : -1);
            Map<String, Object> hm = snap.getDbHostMetrics();
            dbCpuList.add(hm != null && !hm.isEmpty() ? getDouble(hm, "cpuUsage") : -1);
        }

        sb.append("### TPS\n\n");
        appendSvgChart(sb, tpsList, "#00b8d9", "TPS");

        sb.append("### New Order TPM (tpmC)\n\n");
        appendSvgChart(sb, noTpmList, "#2ecc71", "tpmC");

        sb.append("### Average Latency (ms)\n\n");
        appendSvgChart(sb, latencyList, "#f39c12", "Latency");

        sb.append("### Client CPU Usage (%)\n\n");
        appendSvgChart(sb, clientCpuList, "#e74c3c", "CPU%");

        boolean hasDbCpu = dbCpuList.stream().anyMatch(v -> v >= 0);
        if (hasDbCpu) {
            sb.append("### Database Server CPU Usage (%)\n\n");
            appendSvgChart(sb, dbCpuList, "#ff6b6b", "DB CPU%");
        }
    }

    private void appendSvgChart(StringBuilder sb, List<Double> data, String color, String label) {
        // Filter valid data points
        List<Double> valid = new ArrayList<>();
        for (Double v : data) {
            if (v != null && v >= 0) valid.add(v);
        }
        if (valid.size() < 2) {
            sb.append("*No data available.*\n\n");
            return;
        }

        int w = 700, h = 160, padL = 55, padR = 15, padT = 15, padB = 25;
        int chartW = w - padL - padR;
        int chartH = h - padT - padB;

        double maxVal = valid.stream().mapToDouble(Double::doubleValue).max().orElse(1);
        double minVal = valid.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        if (maxVal == minVal) maxVal = minVal + 1;
        // Add 10% headroom
        double range = maxVal - minVal;
        maxVal += range * 0.1;
        minVal = Math.max(0, minVal - range * 0.05);
        range = maxVal - minVal;

        sb.append("<svg width=\"").append(w).append("\" height=\"").append(h)
          .append("\" xmlns=\"http://www.w3.org/2000/svg\" style=\"background:#1a1a2e;border-radius:8px;margin-bottom:16px;\">\n");

        // Grid lines and Y-axis labels
        int gridLines = 4;
        for (int i = 0; i <= gridLines; i++) {
            int y = padT + (int)(chartH * i / (double) gridLines);
            double val = maxVal - (range * i / gridLines);
            sb.append("  <line x1=\"").append(padL).append("\" y1=\"").append(y)
              .append("\" x2=\"").append(w - padR).append("\" y2=\"").append(y)
              .append("\" stroke=\"#333\" stroke-width=\"1\"/>\n");
            String valStr = val >= 1000 ? String.valueOf(Math.round(val)) : String.format("%.1f", val);
            sb.append("  <text x=\"").append(padL - 5).append("\" y=\"").append(y + 4)
              .append("\" fill=\"#888\" font-size=\"11\" text-anchor=\"end\">").append(valStr).append("</text>\n");
        }

        // Build polyline points
        StringBuilder points = new StringBuilder();
        StringBuilder areaPoints = new StringBuilder();
        int n = valid.size();
        areaPoints.append(padL).append(",").append(padT + chartH).append(" ");
        for (int i = 0; i < n; i++) {
            int x = padL + (int)(chartW * i / (double)(n - 1));
            int y = padT + chartH - (int)(chartH * (valid.get(i) - minVal) / range);
            y = Math.max(padT, Math.min(padT + chartH, y));
            points.append(x).append(",").append(y);
            areaPoints.append(x).append(",").append(y);
            if (i < n - 1) { points.append(" "); areaPoints.append(" "); }
        }
        int lastX = padL + (int)(chartW * (n - 1) / (double)(n - 1));
        areaPoints.append(" ").append(lastX).append(",").append(padT + chartH);

        // Area fill
        sb.append("  <polygon points=\"").append(areaPoints).append("\" fill=\"").append(color).append("\" opacity=\"0.15\"/>\n");
        // Line
        sb.append("  <polyline points=\"").append(points).append("\" fill=\"none\" stroke=\"")
          .append(color).append("\" stroke-width=\"2\"/>\n");

        // Label
        sb.append("  <text x=\"").append(padL + 5).append("\" y=\"").append(padT + 12)
          .append("\" fill=\"").append(color).append("\" font-size=\"12\" font-weight=\"bold\">").append(label).append("</text>\n");

        // Min/Max/Avg annotation
        double avg = valid.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        String stats = String.format("avg=%s  max=%s  min=%s",
                formatChartVal(avg), formatChartVal(maxVal - range * 0.1), formatChartVal(minVal + range * 0.05));
        sb.append("  <text x=\"").append(w - padR).append("\" y=\"").append(h - 5)
          .append("\" fill=\"#666\" font-size=\"10\" text-anchor=\"end\">").append(stats).append("</text>\n");

        sb.append("</svg>\n\n");
    }

    private String formatChartVal(double val) {
        if (val >= 1000) return String.valueOf(Math.round(val));
        if (val >= 10) return String.format("%.1f", val);
        return String.format("%.2f", val);
    }

    private double getDouble(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val instanceof Number) return ((Number) val).doubleValue();
        return -1;
    }
}
