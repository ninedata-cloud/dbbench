package com.ninedata.dbbench.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "db")
public class DatabaseConfig {
    private String type = "mysql";
    private String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/tpcc?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true";
    private String username = "sysbench";
    private String password = "sysbench";
    private PoolConfig pool = new PoolConfig();
    private SshConfig ssh = new SshConfig();

    @Data
    public static class PoolConfig {
        private int size = 50;
        private int minIdle = 10;
    }

    @Data
    public static class SshConfig {
        private boolean enabled = false;
        private String host = "";
        private int port = 22;
        private String username = "root";
        private String password = "";
        private String privateKey = "";
        private String passphrase = "";
    }

    /**
     * Get the effective SSH host - uses explicit SSH host if set, otherwise extracts from JDBC URL.
     */
    public String getEffectiveSshHost() {
        if (ssh.getHost() != null && !ssh.getHost().isBlank()) {
            return ssh.getHost();
        }
        // Try to extract host from JDBC URL
        try {
            String url = jdbcUrl;
            // Remove jdbc: prefix and protocol
            int slashIdx = url.indexOf("//");
            if (slashIdx >= 0) {
                String hostPart = url.substring(slashIdx + 2);
                // Remove path/query
                int pathIdx = hostPart.indexOf('/');
                if (pathIdx >= 0) hostPart = hostPart.substring(0, pathIdx);
                int queryIdx = hostPart.indexOf('?');
                if (queryIdx >= 0) hostPart = hostPart.substring(0, queryIdx);
                // Remove port
                int colonIdx = hostPart.lastIndexOf(':');
                if (colonIdx >= 0) hostPart = hostPart.substring(0, colonIdx);
                return hostPart;
            }
        } catch (Exception ignored) {}
        return "";
    }

    public String getDriverClassName() {
        var registry = com.ninedata.dbbench.database.plugin.DatabaseDefinitionRegistry.getInstance();
        if (registry != null) {
            var def = registry.getDefinition(type);
            if (def != null && def.getDriverClassName() != null) {
                return def.getDriverClassName();
            }
        }
        return "com.mysql.cj.jdbc.Driver";
    }
}
