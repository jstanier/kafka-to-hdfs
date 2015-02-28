package com.jstanier.hdfswriter;

import java.util.Arrays;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;

public class Main {

    private static final List<String> REQUIRED_PROPERTIES = Arrays.asList(new String[] {
            "zookeeper.host", "output.path", "kafka.topic", "hdfs.site.xml", "core.site.xml" });

    public static void main(String[] args) {
        checkArguments(args);
        ConfigurableApplicationContext context = SpringApplication.run(Config.class, args);
        context.registerShutdownHook();
    }

    private static void checkArguments(String[] args) {
        SimpleCommandLinePropertySource commandLinePropertySource = new SimpleCommandLinePropertySource(
                args);
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!commandLinePropertySource.containsProperty(requiredProperty)) {
                printHelpTextAndExit(requiredProperty);
            }
        }
    }

    private static void printHelpTextAndExit(String missingProperty) {
        System.out.println("Missing required argument: " + missingProperty);
        System.exit(1);
    }
}
