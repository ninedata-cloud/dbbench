package com.ninedata.dbbench.database.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@Slf4j
@Component
public class DatabaseDefinitionRegistry {

    private static DatabaseDefinitionRegistry instance;

    private final Map<String, DatabaseDefinition> definitions = new ConcurrentHashMap<>();
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    @PostConstruct
    public void init() {
        instance = this;
        scanExternalDefinitions();
        scanClasspathDefinitions();
        log.info("Database plugin registry initialized: {} definitions loaded", definitions.size());
    }

    public static DatabaseDefinitionRegistry getInstance() {
        return instance;
    }

    private void scanExternalDefinitions() {
        Path extDir = Paths.get("./db-definitions");
        if (!Files.isDirectory(extDir)) return;
        try (Stream<Path> dirs = Files.list(extDir)) {
            dirs.filter(Files::isDirectory).forEach(dir -> {
                Path descriptor = dir.resolve("descriptor.yml");
                if (Files.exists(descriptor)) {
                    try (InputStream is = Files.newInputStream(descriptor)) {
                        registerDefinition(is, dir.getFileName().toString(), descriptor.toString());
                    } catch (IOException e) {
                        log.warn("Failed to load plugin descriptor: {}", descriptor, e);
                    }
                }
            });
        } catch (IOException e) {
            log.warn("Failed to scan external db-definitions directory", e);
        }
    }

    private void scanClasspathDefinitions() {
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources("classpath:db-definitions/*/descriptor.yml");
            for (Resource resource : resources) {
                String path = resource.getURL().getPath();
                // Extract directory name from path
                String[] parts = path.split("/");
                String dirName = parts.length >= 2 ? parts[parts.length - 2] : "unknown";
                // Skip if already registered from external (external takes priority)
                if (!definitions.containsKey(dirName)) {
                    try (InputStream is = resource.getInputStream()) {
                        registerDefinition(is, dirName, resource.getURL().toString());
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to scan classpath db-definitions", e);
        }
    }

    private void registerDefinition(InputStream is, String dirName, String source) throws IOException {
        DatabaseDefinition def = yamlMapper.readValue(is, DatabaseDefinition.class);
        if (def.getType() == null || def.getType().isBlank()) {
            def.setType(dirName);
        }
        if (def.getName() == null || def.getName().isBlank()) {
            def.setName(def.getType());
        }
        String key = def.getType().toLowerCase();
        definitions.put(key, def);
        if (def.getAliases() != null) {
            for (String alias : def.getAliases()) {
                definitions.putIfAbsent(alias.toLowerCase(), def);
            }
        }
        log.info("Discovered database plugin: {} (type={}, source={})", def.getName(), def.getType(), source);
    }

    public DatabaseDefinition getDefinition(String type) {
        return type == null ? null : definitions.get(type.toLowerCase());
    }

    public Collection<DatabaseDefinition> getAllDefinitions() {
        // Return unique definitions (aliases point to same object)
        return new LinkedHashSet<>(definitions.values());
    }

    public boolean hasDefinition(String type) {
        return type != null && definitions.containsKey(type.toLowerCase());
    }
}
