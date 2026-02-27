package com.ninedata.dbbench;

import com.ninedata.dbbench.cli.CLIRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DBBenchApplication {

    public static void main(String[] args) {
        if (args.length > 0 && !args[0].equals("--server") && !args[0].equals("--port")) {
            // CLI mode: dbbench -f config.properties [actions...]
            CLIRunner.run(args);
        } else {
            // Web mode
            SpringApplication app = new SpringApplication(DBBenchApplication.class);
            app.setDefaultProperties(getPortProperties(args));
            app.run(args);
        }
    }

    private static java.util.Map<String, Object> getPortProperties(String[] args) {
        java.util.Map<String, Object> properties = new java.util.HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if ("--port".equals(args[i])) {
                try {
                    int port = Integer.parseInt(args[i + 1]);
                    properties.put("server.port", port);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port: " + args[i + 1] + ", using default 1929");
                }
                break;
            }
        }
        return properties;
    }
}
