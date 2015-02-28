package com.jstanier.hdfswriter;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.brandwatch.kafka.consumer.StreamIterator;

@Component
@Scope("prototype")
final class StreamConsumer implements Runnable {

    @Autowired
    private StreamIterator<String, String> streamIterator;

    @Autowired
    private HDFSWriter hdfsWriter;

    public void run() {
        try {
            while (streamIterator.hasNext()) {
                hdfsWriter.write(streamIterator.next().message());
            }
        } catch (InterruptedException e) {} catch (IOException e) {}
    }
}
