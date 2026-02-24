package com.ninedata.dbbench.engine;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds a long-lived connection and cached PreparedStatements for a terminal.
 */
@Slf4j
@Getter
public class TerminalContext {
    private Connection connection;
    private final int warehouseId;
    private final int districtId;
    private final int totalWarehouses;
    private final com.ninedata.dbbench.database.DatabaseAdapter adapter;
    private final Map<String, PreparedStatement> stmtCache = new HashMap<>();

    public TerminalContext(com.ninedata.dbbench.database.DatabaseAdapter adapter,
                           int warehouseId, int districtId, int totalWarehouses) throws SQLException {
        this.adapter = adapter;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.totalWarehouses = totalWarehouses;
        this.connection = adapter.getConnection();
    }

    /**
     * Get or create a cached PreparedStatement for the given SQL.
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement ps = stmtCache.get(sql);
        if (ps != null) {
            try {
                if (!ps.isClosed()) {
                    ps.clearParameters();
                    return ps;
                }
            } catch (AbstractMethodError | Exception ignored) {
                // Driver doesn't support isClosed() (JDBC3) - assume open
                ps.clearParameters();
                return ps;
            }
        }
        ps = connection.prepareStatement(sql);
        stmtCache.put(sql, ps);
        return ps;
    }

    /**
     * Reconnect if the connection is broken.
     */
    public void reconnect() throws SQLException {
        closeStatements();
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignored) {}
        this.connection = adapter.getConnection();
        log.info("Terminal reconnected (w={}, d={})", warehouseId, districtId);
    }

    private void closeStatements() {
        for (PreparedStatement ps : stmtCache.values()) {
            try { ps.close(); } catch (SQLException ignored) {}
        }
        stmtCache.clear();
    }

    public void close() {
        closeStatements();
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignored) {}
    }
}
