package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import com.ninedata.dbbench.database.plugin.DatabaseDefinition;
import com.ninedata.dbbench.database.plugin.DatabaseDefinitionRegistry;
import com.ninedata.dbbench.database.plugin.ScriptBasedAdapter;

import java.util.*;

public class DatabaseFactory {

    public static DatabaseAdapter create(DatabaseConfig config) {
        DatabaseDefinitionRegistry registry = DatabaseDefinitionRegistry.getInstance();
        if (registry != null) {
            DatabaseDefinition def = registry.getDefinition(config.getType());
            if (def != null) {
                return new ScriptBasedAdapter(config, def);
            }
        }
        throw new IllegalArgumentException("Unsupported database type: " + config.getType()
                + ". No plugin found. Available plugins are loaded from db-definitions/.");
    }

    public static List<Map<String, Object>> getAvailableTypes() {
        List<Map<String, Object>> types = new ArrayList<>();
        DatabaseDefinitionRegistry registry = DatabaseDefinitionRegistry.getInstance();
        if (registry != null) {
            for (DatabaseDefinition def : registry.getAllDefinitions()) {
                String type = def.getType().toLowerCase();
                if (type.startsWith("_")) continue;
                Map<String, Object> t = new LinkedHashMap<>();
                t.put("value", type);
                t.put("label", def.getName());
                t.put("builtin", true);
                if (def.getUrlTemplate() != null) {
                    t.put("urlTemplate", def.getUrlTemplate());
                }
                types.add(t);
            }
        }
        return types;
    }
}
