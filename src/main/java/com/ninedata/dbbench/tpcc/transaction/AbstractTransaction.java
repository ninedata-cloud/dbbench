package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.engine.TerminalContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BiConsumer;

@Slf4j
@Getter
public abstract class AbstractTransaction {
    protected final DatabaseAdapter adapter;
    protected final int warehouseId;
    protected final int districtId;
    protected final boolean useLimitSyntax;
    protected final boolean useRowIdForLimitForUpdate;
    protected final boolean supportsForUpdate;

    @Setter
    private static BiConsumer<String, String> errorCallback;

    public AbstractTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        this.adapter = adapter;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.useLimitSyntax = adapter.supportsLimitSyntax();
        this.useRowIdForLimitForUpdate = adapter.requiresRowIdForLimitForUpdate();
        this.supportsForUpdate = adapter.supportsForUpdate();
    }

    public abstract String getName();

    /**
     * Execute using a TerminalContext (long-lived connection + PreparedStatement cache).
     */
    public TransactionResult execute(TerminalContext ctx) {
        Connection conn = ctx.getConnection();
        try {
            boolean success = doExecute(ctx);
            if (success) {
                conn.commit();
                return TransactionResult.SUCCESS;
            } else {
                conn.rollback();
                return TransactionResult.ROLLBACK;
            }
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            String errorMsg = String.format("[%s] %s", getName(), e.getMessage());
            log.error(errorMsg);
            if (errorCallback != null) {
                errorCallback.accept("ERROR", errorMsg);
            }
            return TransactionResult.ERROR;
        }
    }

    /**
     * Legacy execute: gets a connection from pool per transaction.
     */
    public TransactionResult execute() {
        try (Connection conn = adapter.getConnection()) {
            boolean success = doExecuteLegacy(conn);
            if (success) {
                conn.commit();
                return TransactionResult.SUCCESS;
            } else {
                conn.rollback();
                return TransactionResult.ROLLBACK;
            }
        } catch (SQLException e) {
            String errorMsg = String.format("[%s] %s", getName(), e.getMessage());
            log.error(errorMsg);
            if (errorCallback != null) {
                errorCallback.accept("ERROR", errorMsg);
            }
            return TransactionResult.ERROR;
        }
    }

    /**
     * Execute with TerminalContext — subclasses implement this.
     */
    protected abstract boolean doExecute(TerminalContext ctx) throws SQLException;

    /**
     * Legacy execute with raw Connection — default delegates to doExecute with null ctx.
     * Subclasses that still need raw Connection support can override.
     */
    protected boolean doExecuteLegacy(Connection conn) throws SQLException {
        // Not used in normal benchmark flow; kept for backward compatibility
        throw new UnsupportedOperationException("Use execute(TerminalContext) instead");
    }

    /**
     * Build a SELECT query with LIMIT 1 that works across databases.
     */
    protected String buildSelectFirstRowQuery(String baseQuery) {
        String dbType = adapter.getDatabaseType().toLowerCase();
        if (dbType.contains("sql server")) {
            return baseQuery.replaceFirst("(?i)SELECT\\s+", "SELECT TOP 1 ");
        } else if (useLimitSyntax) {
            return baseQuery + " LIMIT 1";
        } else if (dbType.contains("oracle")) {
            return "SELECT * FROM (" + baseQuery + ") WHERE ROWNUM = 1";
        } else {
            return baseQuery + " FETCH FIRST 1 ROWS ONLY";
        }
    }

    /**
     * Build a SELECT ... LIMIT 1 FOR UPDATE query that works across databases.
     */
    protected String buildSelectFirstRowForUpdateQuery(String baseQuery) {
        if (!supportsForUpdate) {
            return buildSelectFirstRowQuery(baseQuery);
        }
        String dbType = adapter.getDatabaseType().toLowerCase();
        if (dbType.contains("sql server")) {
            String query = baseQuery.replaceFirst("(?i)SELECT\\s+", "SELECT TOP 1 ");
            query = addSqlServerLockHint(query);
            return query;
        } else if (useLimitSyntax) {
            return baseQuery + " LIMIT 1 FOR UPDATE";
        } else if (useRowIdForLimitForUpdate) {
            return buildOracleRowIdForUpdateQuery(baseQuery);
        } else {
            return baseQuery + " FETCH FIRST 1 ROWS ONLY FOR UPDATE";
        }
    }

    private String buildOracleRowIdForUpdateQuery(String baseQuery) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "(?i)SELECT\\s+.+?\\s+FROM\\s+(\\w+)\\s+(WHERE\\s+.+?)?(ORDER\\s+BY\\s+.+)?$"
        );
        java.util.regex.Matcher matcher = pattern.matcher(baseQuery.trim());

        if (matcher.find()) {
            String tableName = matcher.group(1);
            String whereClause = matcher.group(2) != null ? matcher.group(2).trim() : "";
            String orderByClause = matcher.group(3) != null ? matcher.group(3).trim() : "";

            StringBuilder innerQuery = new StringBuilder("SELECT ROWID FROM ");
            innerQuery.append(tableName);
            if (!whereClause.isEmpty()) {
                innerQuery.append(" ").append(whereClause);
            }
            if (!orderByClause.isEmpty()) {
                innerQuery.append(" ").append(orderByClause);
            }

            java.util.regex.Pattern selectPattern = java.util.regex.Pattern.compile("(?i)SELECT\\s+(.+?)\\s+FROM");
            java.util.regex.Matcher selectMatcher = selectPattern.matcher(baseQuery);
            String selectColumns = selectMatcher.find() ? selectMatcher.group(1) : "*";

            StringBuilder result = new StringBuilder("SELECT ");
            result.append(selectColumns);
            result.append(" FROM ").append(tableName);
            result.append(" WHERE ROWID = (SELECT ROWID FROM (");
            result.append(innerQuery);
            result.append(") WHERE ROWNUM = 1) FOR UPDATE");

            return result.toString();
        }

        return baseQuery + " FOR UPDATE";
    }

    /**
     * Build a SELECT FOR UPDATE query (without LIMIT) that works across databases.
     */
    protected String buildSelectForUpdateQuery(String baseQuery) {
        if (!supportsForUpdate) {
            return baseQuery;
        }
        String dbType = adapter.getDatabaseType().toLowerCase();
        if (dbType.contains("sql server")) {
            return addSqlServerLockHint(baseQuery);
        } else {
            return baseQuery + " FOR UPDATE";
        }
    }

    private String addSqlServerLockHint(String query) {
        return query.replaceFirst(
            "(?i)(FROM\\s+)(\\w+)(\\s+(?:WHERE|ORDER|GROUP|HAVING|$))",
            "$1$2 WITH (UPDLOCK, ROWLOCK)$3"
        );
    }
}
