package com.ninedata.dbbench.web.util;

import com.ninedata.dbbench.web.dto.request.*;

import java.util.LinkedHashMap;
import java.util.Map;

public class ConfigMapper {

    public static Map<String, Object> toMap(ConfigUpdateRequest request) {
        Map<String, Object> config = new LinkedHashMap<>();

        if (request.getDatabase() != null) {
            config.put("database", toMap(request.getDatabase()));

            // Also put ssh at top level — engine.updateConfig() reads it there
            if (request.getDatabase().getSsh() != null) {
                config.put("ssh", toSshMap(request.getDatabase().getSsh()));
            }
        }

        if (request.getBenchmark() != null) {
            Map<String, Object> benchmark = toBenchmarkMap(request.getBenchmark());
            config.put("benchmark", benchmark);

            if (request.getBenchmark().getMix() != null) {
                config.put("transactionMix", toTransactionMixMap(request.getBenchmark().getMix()));
            }
        }

        return config;
    }

    public static Map<String, Object> toMap(DatabaseConfigDto dto) {
        Map<String, Object> db = new LinkedHashMap<>();

        if (dto.getType() != null) db.put("type", dto.getType());
        if (dto.getJdbcUrl() != null) db.put("jdbcUrl", dto.getJdbcUrl());
        if (dto.getUsername() != null) db.put("username", dto.getUsername());
        if (dto.getPassword() != null) db.put("password", dto.getPassword());

        if (dto.getPool() != null) {
            Map<String, Object> pool = new LinkedHashMap<>();
            if (dto.getPool().getSize() != null) pool.put("size", dto.getPool().getSize());
            if (dto.getPool().getMinIdle() != null) pool.put("minIdle", dto.getPool().getMinIdle());
            db.put("pool", pool);
        }

        if (dto.getSsh() != null) {
            db.put("ssh", toSshMap(dto.getSsh()));
        }

        return db;
    }

    public static Map<String, Object> toBenchmarkMap(BenchmarkConfigDto dto) {
        Map<String, Object> bench = new LinkedHashMap<>();

        if (dto.getWarehouses() != null) bench.put("warehouses", dto.getWarehouses());
        if (dto.getTerminals() != null) bench.put("terminals", dto.getTerminals());
        if (dto.getDuration() != null) bench.put("duration", dto.getDuration());
        if (dto.getRampup() != null) bench.put("rampup", dto.getRampup());
        if (dto.getThinkTime() != null) bench.put("thinkTime", dto.getThinkTime());
        if (dto.getLoadConcurrency() != null) bench.put("loadConcurrency", dto.getLoadConcurrency());
        if (dto.getLoadMode() != null) bench.put("loadMode", dto.getLoadMode());

        return bench;
    }

    public static Map<String, Object> toTransactionMixMap(TransactionMixDto dto) {
        Map<String, Object> mix = new LinkedHashMap<>();

        if (dto.getNewOrder() != null) mix.put("newOrder", dto.getNewOrder());
        if (dto.getPayment() != null) mix.put("payment", dto.getPayment());
        if (dto.getOrderStatus() != null) mix.put("orderStatus", dto.getOrderStatus());
        if (dto.getDelivery() != null) mix.put("delivery", dto.getDelivery());
        if (dto.getStockLevel() != null) mix.put("stockLevel", dto.getStockLevel());

        return mix;
    }

    public static Map<String, Object> toSshMap(SshConfigDto dto) {
        Map<String, Object> ssh = new LinkedHashMap<>();

        if (dto.getEnabled() != null) ssh.put("enabled", dto.getEnabled());
        if (dto.getHost() != null) ssh.put("host", dto.getHost());
        if (dto.getPort() != null) ssh.put("port", dto.getPort());
        if (dto.getUsername() != null) ssh.put("username", dto.getUsername());
        if (dto.getPassword() != null) ssh.put("password", dto.getPassword());
        if (dto.getPrivateKey() != null) ssh.put("privateKey", dto.getPrivateKey());

        return ssh;
    }
}
