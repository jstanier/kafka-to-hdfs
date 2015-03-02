package com.jstanier.hdfswriter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class HDFSWriter {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSWriter.class);

    @Autowired
    private Environment environment;

    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private ObjectFactory<StreamConsumer> streamConsumerFactory;

    @Autowired
    private WriteRecorder writeRecorder;

    private FSDataOutputStream outputStream;
    private ExecutorService pool = Executors.newSingleThreadExecutor();
    private int flushBatchSize = 0;
    private int flushSize;

    @PostConstruct
    public void initialise() {
        flushSize = Integer.parseInt(environment.getProperty("flush.size"));
    }

    @PostConstruct
    public void createOutputStream() throws IOException {
        Path path = createPath();
        deleteIfExists(path);
        beginWriting(path);
    }

    protected Path createPath() {
        return new Path(environment.getProperty("output.path"));
    }

    private void deleteIfExists(Path path) throws IOException {
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    private void beginWriting(Path path) throws IOException {
        LOG.info("Writing...");
        outputStream = fileSystem.create(path);
        pool.execute(streamConsumerFactory.getObject());
    }

    public void write(String message) throws IOException {
        outputStream.writeUTF(message + "\n");
        flushBatchSize++;
        writeRecorder.recordWrite();
        flushWritesIfNeeded();
    }

    private void flushWritesIfNeeded() throws IOException {
        if (flushBatchSize == flushSize) {
            outputStream.hflush();
            flushBatchSize = 0;
        }
    }

    @PreDestroy
    public void close() throws IOException {
        outputStream.hflush();
        outputStream.close();
        pool.shutdown();
        fileSystem.close();
    }

    public int getFlushSize() {
        return flushSize;
    }
}
