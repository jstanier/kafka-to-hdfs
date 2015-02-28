package com.jstanier.hdfswriter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Properties;

import javax.annotation.PreDestroy;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.brandwatch.kafka.consumer.StreamFactory;
import com.brandwatch.kafka.consumer.StreamIterator;

@Configuration
@ComponentScan
public class Config {

    @Autowired
    private Environment environment;

    private StreamFactory streamFactory;

    @Bean
    public FileSystem fileSystem() throws IOException, URISyntaxException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.addResource(new Path(environment.getProperty("core.site.xml")));
        configuration.addResource(new Path(environment.getProperty("hdfs.site.xml")));
        return FileSystem.get(configuration);
    }

    @Bean
    public StreamIterator<String, String> streamIterator() throws UnknownHostException {
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", environment.getProperty("zookeeper.host"));
        consumerProperties.put("zookeeper.connectiontimeout.ms", "5000");
        consumerProperties.put("group.id", InetAddress.getLocalHost().getHostName());
        streamFactory = new StreamFactory(consumerProperties);
        return streamFactory.createStream(environment.getProperty("kafka.topic"),
                StringDecoder.class, StringDecoder.class);
    }

    @PreDestroy
    public void close() {
        streamFactory.shutdown();
    }
}
