package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.database.plugin.DatabaseDefinitionRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DatabaseAdapter Interface Tests")
class DatabaseAdapterTest {

    @BeforeAll
    static void initRegistry() {
        if (DatabaseDefinitionRegistry.getInstance() == null) {
            new DatabaseDefinitionRegistry().init();
        }
    }

    @Test
    @DisplayName("MySQL adapter should support LIMIT syntax")
    void testMySQLSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("mysql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertTrue(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("PostgreSQL adapter should support LIMIT syntax")
    void testPostgreSQLSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("postgresql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertTrue(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("Oracle adapter should not support LIMIT syntax")
    void testOracleNoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("oracle");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertFalse(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("DB2 adapter should not support LIMIT syntax")
    void testDB2NoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("db2");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertFalse(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("SQL Server adapter should not support LIMIT syntax")
    void testSQLServerNoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("sqlserver");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertFalse(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("TiDB adapter should support LIMIT syntax")
    void testTiDBSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("tidb");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertTrue(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("OceanBase adapter should support LIMIT syntax")
    void testOceanBaseSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("oceanbase");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertTrue(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("Dameng adapter should not support LIMIT syntax")
    void testDamengNoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("dameng");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertFalse(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("All adapters should return correct database type")
    void testDatabaseTypes() {
        DatabaseConfig config = new DatabaseConfig();

        config.setType("mysql");
        assertEquals("MySQL", DatabaseFactory.create(config).getDatabaseType());
        config.setType("postgresql");
        assertEquals("PostgreSQL", DatabaseFactory.create(config).getDatabaseType());
        config.setType("oracle");
        assertEquals("Oracle", DatabaseFactory.create(config).getDatabaseType());
        config.setType("sqlserver");
        assertEquals("SQL Server", DatabaseFactory.create(config).getDatabaseType());
        config.setType("db2");
        assertEquals("DB2", DatabaseFactory.create(config).getDatabaseType());
        config.setType("dameng");
        assertEquals("Dameng", DatabaseFactory.create(config).getDatabaseType());
        config.setType("tidb");
        assertEquals("TiDB", DatabaseFactory.create(config).getDatabaseType());
        config.setType("oceanbase");
        assertEquals("OceanBase(MySQL)", DatabaseFactory.create(config).getDatabaseType());
    }

    @Test
    @DisplayName("Default collectHostMetrics interface method should return empty map")
    void testDefaultCollectHostMetrics() throws Exception {
        DatabaseAdapter mockAdapter = new DatabaseAdapter() {
            @Override public void initialize() {}
            @Override public java.sql.Connection getConnection() { return null; }
            @Override public void close() {}
            @Override public void createSchema() {}
            @Override public void dropSchema() {}
            @Override public java.util.Map<String, Object> collectMetrics() { return new java.util.HashMap<>(); }
            @Override public String getDatabaseType() { return "Mock"; }
        };
        assertTrue(mockAdapter.collectHostMetrics().isEmpty());
    }
}
