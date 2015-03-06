package com.jstanier.hdfswriter;

import java.io.IOException;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamConsumer implements Runnable {

    private final Logger log = LoggerFactory.getLogger(StreamConsumer.class);

    private HDFSWriter hdfsWriter;
    private KafkaStream<byte[], byte[]> stream;

    public StreamConsumer(HDFSWriter hdfsWriter, KafkaStream<byte[], byte[]> stream) {
        this.hdfsWriter = hdfsWriter;
        this.stream = stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        try {
            while (iterator.hasNext()) {
                hdfsWriter.write(new String(iterator.next().message()));
            }
        } catch (IOException e) {
            log.error("IOException when writing to HDFS.", e);
        }
    }
}
