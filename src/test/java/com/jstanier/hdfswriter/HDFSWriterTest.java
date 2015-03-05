package com.jstanier.hdfswriter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.env.Environment;

@RunWith(MockitoJUnitRunner.class)
public class HDFSWriterTest {

    @Mock
    private Environment environment;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private WriteRecorder writeRecorder;

    @Mock
    private ConsumerConnector consumerConnector;

    @Mock
    private Iterator<KafkaStream<byte[], byte[]>> streamIterator;

    @Mock
    private FSDataOutputStream outputStream;

    @Mock
    private ExecutorService pool;

    @Mock
    private PathCreator pathCreator;

    @Mock
    private PathRotator pathRotator;

    @InjectMocks
    private HDFSWriter hdfsWriter;

    @Mock
    private List<KafkaStream<byte[], byte[]>> kafkaStreams;

    @Mock
    private Map<String, List<KafkaStream<byte[], byte[]>>> topicsToStreams;

    @Mock
    private Path path;

    @SuppressWarnings("unchecked")
    @Test
    public void whenPathExists_initialise_DeletesPathBeforeCreating() throws IOException {
        Mockito.when(environment.getProperty("flush.size")).thenReturn("100");
        Mockito.when(environment.getProperty("messages.per.file")).thenReturn("5");
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(pathCreator.createNewPath("exampleFile.txt")).thenReturn(path);
        Mockito.when(fileSystem.exists(path)).thenReturn(true);
        Mockito.when(consumerConnector.createMessageStreams(Mockito.anyMap())).thenReturn(topicsToStreams);
        Mockito.when(topicsToStreams.get(Mockito.any())).thenReturn(kafkaStreams);
        Mockito.when(kafkaStreams.iterator()).thenReturn(streamIterator);
        hdfsWriter.initialise();
        hdfsWriter.startWriting();
        Mockito.verify(fileSystem).delete(path, true);
        Mockito.verify(fileSystem).create(path);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void whenPathDoesNotExist_initialise_DoesNotDeletePath() throws IOException {
        Mockito.when(environment.getProperty("flush.size")).thenReturn("100");
        Mockito.when(environment.getProperty("messages.per.file")).thenReturn("5");
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        Mockito.when(pathCreator.createNewPath("exampleFile.txt")).thenReturn(path);
        Mockito.when(consumerConnector.createMessageStreams(Mockito.anyMap())).thenReturn(
                topicsToStreams);
        Mockito.when(topicsToStreams.get(Mockito.any())).thenReturn(kafkaStreams);
        Mockito.when(kafkaStreams.iterator()).thenReturn(streamIterator);
        Mockito.when(kafkaStreams.iterator()).thenReturn(streamIterator);
        hdfsWriter.initialise();
        hdfsWriter.startWriting();
        Mockito.verify(fileSystem).create(path);
        Mockito.verify(fileSystem, Mockito.never()).delete(path, true);
    }

    @Test
    public void whenBatchSizeReachesFlushSize_write_FlushesBatch() throws IOException {
        Mockito.when(environment.getProperty("messages.per.file")).thenReturn("100");
        Mockito.when(environment.getProperty("flush.size")).thenReturn("1");
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(pathCreator.createNewPath("exampleFile.txt")).thenReturn(path);
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        hdfsWriter.initialise();
        hdfsWriter.write("Hello!");
        Mockito.verify(outputStream).hflush();
    }

    @Test
    public void whenBatchSizeIsLessThanFlushSize_write_DoesNotFlushBatch() throws IOException {
        Mockito.when(environment.getProperty("messages.per.file")).thenReturn("5");
        Mockito.when(environment.getProperty("flush.size")).thenReturn("100");
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(pathCreator.createNewPath("exampleFile.txt")).thenReturn(path);
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        hdfsWriter.initialise();
        hdfsWriter.write("Hello!");
        Mockito.verify(outputStream, Mockito.never()).hflush();
    }

    @Test
    public void close_FlushesOutputStreamAndClosesConnections() throws IOException {
        hdfsWriter.close();
        Mockito.verify(pool).shutdown();
        Mockito.verify(outputStream).hflush();
        Mockito.verify(outputStream).close();
        Mockito.verify(fileSystem).close();
    }

    @Test
    public void write_writesMessageWithNewLine() throws IOException {
        hdfsWriter.write("Hello!");
        Mockito.verify(outputStream).writeUTF("Hello!");
        Mockito.verify(outputStream).writeUTF("\n");
    }

    @Test
    public void givenMaximumMessagesInFileIsReached_write_rotatesTheFile()
            throws IllegalArgumentException, Exception {
        Mockito.when(environment.getProperty("flush.size")).thenReturn("100");
        Mockito.when(environment.getProperty("messages.per.file")).thenReturn("5");
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(pathCreator.createNewPath("exampleFile.txt")).thenReturn(path);
        hdfsWriter.initialise();
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        Path rotatedPath = Mockito.mock(Path.class);
        Mockito.when(pathRotator.rotatePath(path)).thenReturn(rotatedPath);
        FSDataOutputStream rotatedOutputStream = Mockito.mock(FSDataOutputStream.class);
        Mockito.when(fileSystem.create(rotatedPath)).thenReturn(rotatedOutputStream);

        for (int i = 0; i < 6; i++) {
            hdfsWriter.write("Hello!");
        }

        Mockito.verify(outputStream).hflush();
        Mockito.verify(fileSystem).create(rotatedPath);
    }

    @Test
    public void givenMaximumMessagesInFileIsNotReached_write_doesNotRotateTheFile()
            throws IllegalArgumentException, Exception {
        Mockito.when(environment.getProperty("flush.size")).thenReturn("100");
        Mockito.when(environment.getProperty("messages.per.file")).thenReturn("5");
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(pathCreator.createNewPath("exampleFile.txt")).thenReturn(path);
        hdfsWriter.initialise();
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        for (int i = 0; i < 4; i++) {
            hdfsWriter.write("Hello!");
        }
        Mockito.verify(fileSystem, Mockito.never()).create(Mockito.any(Path.class));
    }
}
