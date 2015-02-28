package com.jstanier.hdfswriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class WriteRecorder {

    private static final Logger LOG = LoggerFactory.getLogger(WriteRecorder.class);

    private long writes = 0;

    public void recordWrite() {
        writes++;
        printMessageIfNecessary();
    }

    private void printMessageIfNecessary() {
        if (writes % HDFSWriter.FLUSH_SIZE == 0) {
            printWrites();
        }
    }

    public void printWrites() {
        LOG.info("Wrote " + writes + " messages");
    }

}
