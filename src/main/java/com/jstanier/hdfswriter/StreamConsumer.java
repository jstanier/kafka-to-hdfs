package com.jstanier.hdfswriter;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.brandwatch.kafka.consumer.StreamIterator;

@Component
@Scope("prototype")
final class StreamConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumer.class);

    @Autowired
    private StreamIterator<String, String> streamIterator;

    @Autowired
    private HDFSWriter hdfsWriter;

    public void run() {
        try {
            while (streamIterator.hasNext()) {
                hdfsWriter.write(streamIterator.next().message());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.error("IOException when writing to HDFS.", e);
        }
    }
}
