package com.ninedata.dbbench.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class ProfileService {
    private static final Path PROFILES_DIR = Paths.get("profiles");

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(PROFILES_DIR);
            log.info("Profiles directory: {}", PROFILES_DIR);
        } catch (IOException e) {
            log.error("Failed to create profiles directory", e);
        }
    }

    public List<String> listProfiles() {
        try (Stream<Path> files = Files.list(PROFILES_DIR)) {
            return files
                    .filter(p -> p.toString().endsWith(".properties"))
                    .map(p -> p.getFileName().toString().replace(".properties", ""))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to list profiles", e);
            return Collections.emptyList();
        }
    }

    public Map<String, Object> loadProfile(String name) throws IOException {
        Path file = PROFILES_DIR.resolve(sanitizeName(name) + ".properties");
        if (!Files.exists(file)) {
            throw new FileNotFoundException("Profile not found: " + name);
        }
        Properties props = new Properties();
        try (Reader reader = Files.newBufferedReader(file)) {
            props.load(reader);
        }
        return propertiesToConfig(props);
    }
    public void saveProfile(String name, Map<String, Object> config) throws IOException {
        String safeName = sanitizeName(name);
        if (safeName.isEmpty()) {
            throw new IllegalArgumentException("Invalid profile name");
        }
        Path file = PROFILES_DIR.resolve(safeName + ".properties");
        Properties props = configToProperties(config);
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            writer.write("# Profile: " + name);
            writer.newLine();
            // Write properties manually to avoid Java's default escaping of : and =
            List<String> keys = new ArrayList<>(props.stringPropertyNames());
            Collections.sort(keys);
            for (String key : keys) {
                writer.write(key + "=" + props.getProperty(key));
                writer.newLine();
            }
        }
        log.info("Profile saved: {}", name);
    }

    public void deleteProfile(String name) throws IOException {
        Path file = PROFILES_DIR.resolve(sanitizeName(name) + ".properties");
        if (!Files.exists(file)) {
            throw new FileNotFoundException("Profile not found: " + name);
        }
        Files.delete(file);
        log.info("Profile deleted: {}", name);
    }

    private String sanitizeName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_\\-.]", "_");
    }

    @SuppressWarnings("unchecked")
    private Properties configToProperties(Map<String, Object> config) {
        Properties props = new Properties();
        Map<String, Object> db = (Map<String, Object>) config.getOrDefault("database", Map.of());
        putIfPresent(props, "db.type", db.get("type"));
        putIfPresent(props, "db.jdbc-url", db.get("jdbcUrl"));
        putIfPresent(props, "db.username", db.get("username"));
        putIfPresent(props, "db.password", db.get("password"));
        putIfPresent(props, "db.pool.size", db.get("poolSize"));
        Map<String, Object> ssh = (Map<String, Object>) config.getOrDefault("ssh", Map.of());
        putIfPresent(props, "db.ssh.enabled", ssh.get("enabled"));
        putIfPresent(props, "db.ssh.host", ssh.get("host"));
        putIfPresent(props, "db.ssh.port", ssh.get("port"));
        putIfPresent(props, "db.ssh.username", ssh.get("username"));
        putIfPresent(props, "db.ssh.password", ssh.get("password"));
        putIfPresent(props, "db.ssh.private-key", ssh.get("privateKey"));

        Map<String, Object> bench = (Map<String, Object>) config.getOrDefault("benchmark", Map.of());
        putIfPresent(props, "benchmark.warehouses", bench.get("warehouses"));
        putIfPresent(props, "benchmark.terminals", bench.get("terminals"));
        putIfPresent(props, "benchmark.duration", bench.get("duration"));
        putIfPresent(props, "benchmark.think-time", bench.get("thinkTime"));
        putIfPresent(props, "benchmark.load-concurrency", bench.get("loadConcurrency"));
        putIfPresent(props, "benchmark.load-mode", bench.get("loadMode"));

        Map<String, Object> mix = (Map<String, Object>) config.getOrDefault("transactionMix", Map.of());
        putIfPresent(props, "benchmark.mix.new-order", mix.get("newOrder"));
        putIfPresent(props, "benchmark.mix.payment", mix.get("payment"));
        putIfPresent(props, "benchmark.mix.order-status", mix.get("orderStatus"));
        putIfPresent(props, "benchmark.mix.delivery", mix.get("delivery"));
        putIfPresent(props, "benchmark.mix.stock-level", mix.get("stockLevel"));
        return props;
    }

    private void putIfPresent(Properties props, String key, Object value) {
        if (value != null) {
            props.setProperty(key, String.valueOf(value));
        }
    }

    private Map<String, Object> propertiesToConfig(Properties props) {
        Map<String, Object> config = new LinkedHashMap<>();

        Map<String, Object> db = new LinkedHashMap<>();
        db.put("type", props.getProperty("db.type", "mysql"));
        db.put("jdbcUrl", props.getProperty("db.jdbc-url", ""));
        db.put("username", props.getProperty("db.username", ""));
        db.put("password", props.getProperty("db.password", ""));
        db.put("poolSize", intVal(props, "db.pool.size", 50));
        config.put("database", db);

        Map<String, Object> ssh = new LinkedHashMap<>();
        ssh.put("enabled", Boolean.parseBoolean(props.getProperty("db.ssh.enabled", "false")));
        ssh.put("host", props.getProperty("db.ssh.host", ""));
        ssh.put("port", intVal(props, "db.ssh.port", 22));
        ssh.put("username", props.getProperty("db.ssh.username", "root"));
        ssh.put("password", props.getProperty("db.ssh.password", ""));
        ssh.put("privateKey", props.getProperty("db.ssh.private-key", ""));
        config.put("ssh", ssh);

        Map<String, Object> bench = new LinkedHashMap<>();
        bench.put("warehouses", intVal(props, "benchmark.warehouses", 10));
        bench.put("terminals", intVal(props, "benchmark.terminals", 50));
        bench.put("duration", intVal(props, "benchmark.duration", 60));
        bench.put("thinkTime", Boolean.parseBoolean(props.getProperty("benchmark.think-time", "false")));
        bench.put("loadConcurrency", intVal(props, "benchmark.load-concurrency", 4));
        bench.put("loadMode", props.getProperty("benchmark.load-mode", "auto"));
        config.put("benchmark", bench);

        Map<String, Object> mix = new LinkedHashMap<>();
        mix.put("newOrder", intVal(props, "benchmark.mix.new-order", 45));
        mix.put("payment", intVal(props, "benchmark.mix.payment", 43));
        mix.put("orderStatus", intVal(props, "benchmark.mix.order-status", 4));
        mix.put("delivery", intVal(props, "benchmark.mix.delivery", 4));
        mix.put("stockLevel", intVal(props, "benchmark.mix.stock-level", 4));
        config.put("transactionMix", mix);

        return config;
    }

    private int intVal(Properties props, String key, int defaultVal) {
        String val = props.getProperty(key);
        if (val == null) return defaultVal;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }
}
