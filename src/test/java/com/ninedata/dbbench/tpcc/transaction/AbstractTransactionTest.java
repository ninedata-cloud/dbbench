package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AbstractTransaction SQL Generation Tests")
class AbstractTransactionTest {

    // Mock adapter for MySQL (supports LIMIT syntax)
    static class MockMySQLAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "MySQL"; }
        @Override public boolean supportsLimitSyntax() { return true; }
    }

    // Mock adapter for PostgreSQL (supports LIMIT syntax)
    static class MockPostgreSQLAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "PostgreSQL"; }
        @Override public boolean supportsLimitSyntax() { return true; }
    }

    // Mock adapter for Oracle (no LIMIT syntax)
    static class MockOracleAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "Oracle"; }
        @Override public boolean supportsLimitSyntax() { return false; }
    }

    // Mock adapter for DB2 (uses FETCH FIRST syntax)
    static class MockDB2Adapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "DB2"; }
        @Override public boolean supportsLimitSyntax() { return false; }
    }

    // Mock adapter for SQL Server (uses TOP syntax)
    static class MockSQLServerAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "SQL Server"; }
        @Override public boolean supportsLimitSyntax() { return false; }
    }

    // Mock adapter for SQLite (no FOR UPDATE support)
    static class MockSQLiteAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "SQLite"; }
        @Override public boolean supportsLimitSyntax() { return true; }
        @Override public boolean supportsForUpdate() { return false; }
    }

    // Testable transaction class to expose protected methods
    static class TestableTransaction extends AbstractTransaction {
        public TestableTransaction(DatabaseAdapter adapter) {
            super(adapter, 1, 1);
        }

        @Override
        public String getName() { return "TEST"; }

        @Override
        protected boolean doExecute(com.ninedata.dbbench.engine.TerminalContext ctx) { return true; }

        public String testBuildSelectFirstRowQuery(String baseQuery) {
            return buildSelectFirstRowQuery(baseQuery);
        }

        public String testBuildSelectForUpdateQuery(String baseQuery) {
            return buildSelectForUpdateQuery(baseQuery);
        }
    }

    // ==================== MySQL Tests ====================

    @Test
    @DisplayName("MySQL: Should use LIMIT 1 for SELECT first row")
    void testMySQLSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockMySQLAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";
        assertEquals(baseQuery + " LIMIT 1", tx.testBuildSelectFirstRowQuery(baseQuery));
    }

    @Test
    @DisplayName("MySQL: Should append FOR UPDATE for SELECT for update")
    void testMySQLSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockMySQLAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";
        assertEquals(baseQuery + " FOR UPDATE", tx.testBuildSelectForUpdateQuery(baseQuery));
    }

    // ==================== PostgreSQL Tests ====================

    @Test
    @DisplayName("PostgreSQL: Should use LIMIT 1 for SELECT first row")
    void testPostgreSQLSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockPostgreSQLAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";
        assertEquals(baseQuery + " LIMIT 1", tx.testBuildSelectFirstRowQuery(baseQuery));
    }

    // ==================== Oracle Tests ====================

    @Test
    @DisplayName("Oracle: Should use ROWNUM subquery for SELECT first row")
    void testOracleSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockOracleAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? ORDER BY c_first";
        String result = tx.testBuildSelectFirstRowQuery(baseQuery);
        assertTrue(result.contains("ROWNUM = 1"));
        assertFalse(result.contains("LIMIT"));
        assertFalse(result.contains("FETCH FIRST"));
    }

    @Test
    @DisplayName("Oracle: Should append FOR UPDATE for SELECT for update")
    void testOracleSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockOracleAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";
        assertEquals(baseQuery + " FOR UPDATE", tx.testBuildSelectForUpdateQuery(baseQuery));
    }

    // ==================== DB2 Tests ====================

    @Test
    @DisplayName("DB2: Should use FETCH FIRST 1 ROWS ONLY for SELECT first row")
    void testDB2SelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockDB2Adapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";
        assertEquals(baseQuery + " FETCH FIRST 1 ROWS ONLY", tx.testBuildSelectFirstRowQuery(baseQuery));
    }

    @Test
    @DisplayName("DB2: Should append FOR UPDATE for SELECT for update")
    void testDB2SelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockDB2Adapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";
        assertEquals(baseQuery + " FOR UPDATE", tx.testBuildSelectForUpdateQuery(baseQuery));
    }

    // ==================== SQL Server Tests ====================

    @Test
    @DisplayName("SQL Server: Should use TOP 1 for SELECT first row")
    void testSQLServerSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockSQLServerAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";
        String result = tx.testBuildSelectFirstRowQuery(baseQuery);
        assertTrue(result.contains("SELECT TOP 1"));
        assertFalse(result.contains("LIMIT"));
    }

    @Test
    @DisplayName("SQL Server: Should use lock hints for SELECT for update")
    void testSQLServerSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockSQLServerAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";
        String result = tx.testBuildSelectForUpdateQuery(baseQuery);
        assertTrue(result.contains("WITH (UPDLOCK, ROWLOCK)"));
        assertFalse(result.contains("FOR UPDATE"));
    }

    // ==================== SQLite Tests ====================

    @Test
    @DisplayName("SQLite: Should not append FOR UPDATE for SELECT for update")
    void testSQLiteSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockSQLiteAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";
        String result = tx.testBuildSelectForUpdateQuery(baseQuery);
        assertEquals(baseQuery, result);
        assertFalse(result.contains("FOR UPDATE"));
    }

    // ==================== Transaction Properties Tests ====================

    @Test
    @DisplayName("Should initialize with correct warehouse and district IDs")
    void testTransactionInitialization() {
        TestableTransaction tx = new TestableTransaction(new MockMySQLAdapter());
        assertEquals(1, tx.getWarehouseId());
        assertEquals(1, tx.getDistrictId());
    }

    @Test
    @DisplayName("Should detect LIMIT syntax support correctly")
    void testLimitSyntaxSupport() {
        TestableTransaction mysqlTx = new TestableTransaction(new MockMySQLAdapter());
        TestableTransaction oracleTx = new TestableTransaction(new MockOracleAdapter());
        assertTrue(mysqlTx.isUseLimitSyntax());
        assertFalse(oracleTx.isUseLimitSyntax());
    }
}
