package com.jstanier.hdfswriter;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WriteRecorder {

    @Autowired
    private HDFSWriter hdfsWriter;

    private final Logger log = LoggerFactory.getLogger(WriteRecorder.class);

    private Long writes = 0L;

    public void recordWrite() {
        incrementWrites();
        printMessageIfNecessary();
    }

    protected void incrementWrites() {
        writes++;
    }

    private void printMessageIfNecessary() {
        if (writes % hdfsWriter.getFlushSize() == 0) {
            printWrites();
        }
    }

    public void printWrites() {
        log.info("Wrote " + writes + " message(s)");
    }

    @PreDestroy
    public void close() {
        printWrites();
    }

}
