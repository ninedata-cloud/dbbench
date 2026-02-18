package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.database.plugin.DatabaseDefinitionRegistry;
import com.ninedata.dbbench.database.plugin.ScriptBasedAdapter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DatabaseFactory Tests")
class DatabaseFactoryTest {

    private DatabaseConfig config;

    @BeforeAll
    static void initRegistry() {
        if (DatabaseDefinitionRegistry.getInstance() == null) {
            new DatabaseDefinitionRegistry().init();
        }
    }

    @BeforeEach
    void setUp() {
        config = new DatabaseConfig();
    }

    @Test
    @DisplayName("Should create adapter for mysql type")
    void testCreateMySQLAdapter() {
        config.setType("mysql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertInstanceOf(ScriptBasedAdapter.class, adapter);
        assertEquals("MySQL", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create adapter for postgresql type")
    void testCreatePostgreSQLAdapter() {
        config.setType("postgresql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertInstanceOf(ScriptBasedAdapter.class, adapter);
        assertEquals("PostgreSQL", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create adapter for postgres alias")
    void testCreatePostgresAdapter() {
        config.setType("postgres");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertInstanceOf(ScriptBasedAdapter.class, adapter);
    }

    @Test
    @DisplayName("Should create adapter for oracle type")
    void testCreateOracleAdapter() {
        config.setType("oracle");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertEquals("Oracle", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create adapter for sqlserver type")
    void testCreateSQLServerAdapter() {
        config.setType("sqlserver");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertEquals("SQL Server", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create adapter for mssql alias")
    void testCreateMSSQLAdapter() {
        config.setType("mssql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertInstanceOf(ScriptBasedAdapter.class, adapter);
    }

    @Test
    @DisplayName("Should create adapter for db2 type")
    void testCreateDB2Adapter() {
        config.setType("db2");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertEquals("DB2", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create adapter for dameng type")
    void testCreateDamengAdapter() {
        config.setType("dameng");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertEquals("Dameng", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create adapter for dm alias")
    void testCreateDMAdapter() {
        config.setType("dm");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertInstanceOf(ScriptBasedAdapter.class, adapter);
    }

    @Test
    @DisplayName("Should create adapter for oceanbase type")
    void testCreateOceanBaseAdapter() {
        config.setType("oceanbase");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertEquals("OceanBase", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create adapter for tidb type")
    void testCreateTiDBAdapter() {
        config.setType("tidb");
        DatabaseAdapter adapter = DatabaseFactory.create(config);
        assertNotNull(adapter);
        assertEquals("TiDB", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should throw exception for unsupported type")
    void testUnsupportedType() {
        config.setType("unsupported");
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> DatabaseFactory.create(config)
        );
        assertTrue(exception.getMessage().contains("Unsupported database type"));
    }

    @Test
    @DisplayName("Should be case insensitive for type")
    void testCaseInsensitive() {
        config.setType("MYSQL");
        DatabaseAdapter a1 = DatabaseFactory.create(config);
        assertInstanceOf(ScriptBasedAdapter.class, a1);

        config.setType("MySQL");
        DatabaseAdapter a2 = DatabaseFactory.create(config);
        assertInstanceOf(ScriptBasedAdapter.class, a2);

        config.setType("PostgreSQL");
        DatabaseAdapter a3 = DatabaseFactory.create(config);
        assertInstanceOf(ScriptBasedAdapter.class, a3);
    }
}
