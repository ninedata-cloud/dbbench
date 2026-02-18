package com.ninedata.dbbench;

import com.ninedata.dbbench.cli.CLIRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DBBenchApplication {

    public static void main(String[] args) {
        if (args.length > 0 && !args[0].equals("--server")) {
            // CLI mode: dbbench -f config.properties [actions...]
            CLIRunner.run(args);
        } else {
            // Web mode
            SpringApplication.run(DBBenchApplication.class, args);
        }
    }
}
