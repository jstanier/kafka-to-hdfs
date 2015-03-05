package com.jstanier.hdfswriter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@ComponentScan
public class Config {

    @Autowired
    private Environment environment;

    @Bean
    public FileSystem fileSystem() throws IOException, URISyntaxException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.addResource(new Path(environment.getProperty("core.site.xml")));
        configuration.addResource(new Path(environment.getProperty("hdfs.site.xml")));
        return FileSystem.get(configuration);
    }

    @Bean
    public ConsumerConnector consumerConnector() throws UnknownHostException {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", environment.getProperty("zookeeper.host"));
        properties.put("zookeeper.connectiontimeout.ms", "5000");
        properties.put("group.id", InetAddress.getLocalHost().getHostName());
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

    }
}
