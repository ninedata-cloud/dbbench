package com.ninedata.dbbench.engine;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.metrics.MetricsRegistry;
import com.ninedata.dbbench.tpcc.TPCCUtil;
import com.ninedata.dbbench.tpcc.transaction.DeliveryTransaction;
import com.ninedata.dbbench.tpcc.transaction.TransactionResult;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Async delivery scheduler per TPC-C spec.
 * Each warehouse can only have one delivery in flight at a time.
 */
@Slf4j
public class DeliveryScheduler {
    private final DatabaseAdapter adapter;
    private final MetricsRegistry metricsRegistry;
    private final int totalWarehouses;
    private final AtomicBoolean[] warehouseBusy;
    private final ExecutorService executor;

    public DeliveryScheduler(DatabaseAdapter adapter, MetricsRegistry metricsRegistry, int totalWarehouses) {
        this.adapter = adapter;
        this.metricsRegistry = metricsRegistry;
        this.totalWarehouses = totalWarehouses;
        this.warehouseBusy = new AtomicBoolean[totalWarehouses + 1]; // 1-indexed
        for (int i = 0; i <= totalWarehouses; i++) {
            warehouseBusy[i] = new AtomicBoolean(false);
        }
        // Use a small thread pool for async delivery
        this.executor = Executors.newFixedThreadPool(Math.max(1, Math.min(totalWarehouses, 4)));
    }

    /**
     * Submit an async delivery for the given warehouse.
     * If the warehouse already has a delivery in flight, this is a no-op (skipped).
     */
    public void submit(int warehouseId, int districtId) {
        if (!warehouseBusy[warehouseId].compareAndSet(false, true)) {
            // Warehouse busy, skip this delivery
            return;
        }

        executor.submit(() -> {
            TerminalContext deliveryCtx = null;
            try {
                deliveryCtx = new TerminalContext(adapter, warehouseId, districtId, totalWarehouses);
                DeliveryTransaction tx = new DeliveryTransaction(adapter, warehouseId, districtId);

                long startTime = System.nanoTime();
                TransactionResult result = tx.execute(deliveryCtx);
                long latency = System.nanoTime() - startTime;

                metricsRegistry.recordTransaction(tx.getName(), result, latency);
            } catch (SQLException e) {
                log.error("Async delivery failed for warehouse {}: {}", warehouseId, e.getMessage());
                metricsRegistry.recordTransaction("DELIVERY", TransactionResult.ERROR, 0);
            } finally {
                if (deliveryCtx != null) {
                    deliveryCtx.close();
                }
                warehouseBusy[warehouseId].set(false);
            }
        });
    }

    public void shutdown() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
