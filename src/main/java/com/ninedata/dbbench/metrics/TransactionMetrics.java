package com.ninedata.dbbench.metrics;

import lombok.Data;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Data
public class TransactionMetrics {
    private final String name;
    private final LongAdder count = new LongAdder();
    private final LongAdder successCount = new LongAdder();
    private final LongAdder rollbackCount = new LongAdder();
    private final LongAdder errorCount = new LongAdder();
    private final LongAdder totalLatencyNanos = new LongAdder();
    private final AtomicLong minLatencyNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatencyNanos = new AtomicLong(0);

    public TransactionMetrics(String name) {
        this.name = name;
    }

    public void recordSuccess(long latencyNanos) {
        count.increment();
        successCount.increment();
        totalLatencyNanos.add(latencyNanos);
        updateMinMax(latencyNanos);
    }

    public void recordRollback(long latencyNanos) {
        count.increment();
        rollbackCount.increment();
        totalLatencyNanos.add(latencyNanos);
        updateMinMax(latencyNanos);
    }

    public void recordError(long latencyNanos) {
        count.increment();
        errorCount.increment();
        totalLatencyNanos.add(latencyNanos);
        updateMinMax(latencyNanos);
    }

    /** @deprecated use {@link #recordRollback(long)} or {@link #recordError(long)} */
    public void recordFailure(long latencyNanos) {
        recordRollback(latencyNanos);
    }

    private void updateMinMax(long latencyNanos) {
        long currentMin;
        do {
            currentMin = minLatencyNanos.get();
            if (latencyNanos >= currentMin) break;
        } while (!minLatencyNanos.compareAndSet(currentMin, latencyNanos));

        long currentMax;
        do {
            currentMax = maxLatencyNanos.get();
            if (latencyNanos <= currentMax) break;
        } while (!maxLatencyNanos.compareAndSet(currentMax, latencyNanos));
    }

    public long getCount() { return count.sum(); }
    public long getSuccessCount() { return successCount.sum(); }
    public long getRollbackCount() { return rollbackCount.sum(); }
    public long getErrorCount() { return errorCount.sum(); }
    public long getFailureCount() { return rollbackCount.sum() + errorCount.sum(); }

    public double getAverageLatencyMs() {
        long c = count.sum();
        return c > 0 ? (totalLatencyNanos.sum() / 1_000_000.0) / c : 0;
    }

    public double getMinLatencyMs() {
        long min = minLatencyNanos.get();
        return min == Long.MAX_VALUE ? 0 : min / 1_000_000.0;
    }

    public double getMaxLatencyMs() {
        return maxLatencyNanos.get() / 1_000_000.0;
    }

    public double getSuccessRate() {
        long c = count.sum();
        return c > 0 ? (successCount.sum() * 100.0) / c : 0;
    }
}
