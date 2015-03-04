package com.jstanier.hdfswriter;

import java.io.IOException;
import java.util.Iterator;
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

import com.google.common.base.Splitter;

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

    @Autowired
    private PathCreator pathCreator;

    @Autowired
    private PathRotator pathRotator;

    private FSDataOutputStream outputStream;
    private ExecutorService pool = Executors.newSingleThreadExecutor();
    private int flushBatchSize = 0;
    private int flushSize;
    private int currentMessages = 0;
    private int maximumMessagesInFile;
    private Path currentPath;

    @PostConstruct
    public void initialise() throws IOException {
        flushSize = Integer.parseInt(environment.getProperty("flush.size"));
        maximumMessagesInFile = Integer.parseInt(environment.getProperty("messages.per.file"));
        currentPath = pathCreator.createNewPath(environment.getProperty("output.path"));
        deleteIfExists(currentPath);
    }

    @PostConstruct
    public void startWriting() throws IOException {
        beginWriting(currentPath);
    }

    private void rotateFile() throws IllegalArgumentException, Exception {
        outputStream.hflush();
        Path rotatedPath = pathRotator.rotatePath(currentPath);
        outputStream = fileSystem.create(rotatedPath);
        currentMessages = 0;
        currentPath = rotatedPath;
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
        writeMessageInChunks(message);
        flushBatchSize++;
        try {
            possiblyRotateFile();
        } catch (Exception e) {
            close();
        }
        writeRecorder.recordWrite();
        flushWritesIfNeeded();
    }

    private void writeMessageInChunks(String message) throws IOException {
        Iterator<String> chunks = Splitter.fixedLength(100).split(message).iterator();
        while (chunks.hasNext()) {
            outputStream.writeUTF(chunks.next());
        }
        outputStream.writeUTF("\n");
    }

    private void possiblyRotateFile() throws Exception {
        currentMessages++;
        if (currentMessages == maximumMessagesInFile) {
            rotateFile();
        }
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
