package com.jstanier.hdfswriter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.core.env.Environment;

@RunWith(MockitoJUnitRunner.class)
public class HDFSWriterTest {

    @Mock
    private Environment environment;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private ObjectFactory<StreamConsumer> streamConsumerFactory;

    @Mock
    private WriteRecorder writeRecorder;

    @Mock
    private FSDataOutputStream outputStream;

    @Mock
    private ExecutorService pool;

    @Mock
    private Path path;

    private HDFSWriter hdfsWriter;

    @Before
    public void setup() {
        hdfsWriter = Mockito.spy(HDFSWriter.class);
        Whitebox.setInternalState(hdfsWriter, "outputStream", outputStream);
        Whitebox.setInternalState(hdfsWriter, "pool", pool);
        Whitebox.setInternalState(hdfsWriter, "environment", environment);
        Whitebox.setInternalState(hdfsWriter, "fileSystem", fileSystem);
        Whitebox.setInternalState(hdfsWriter, "streamConsumerFactory", streamConsumerFactory);
        Whitebox.setInternalState(hdfsWriter, "writeRecorder", writeRecorder);
    }

    @Test
    public void whenPathExists_createOutputStream_DeletesPathBeforeCreating() throws IOException {
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(fileSystem.exists(path)).thenReturn(true);
        Mockito.doReturn(path).when(hdfsWriter).createPath();
        hdfsWriter.createOutputStream();
        Mockito.verify(fileSystem).delete(path, true);
        Mockito.verify(fileSystem).create(path);
    }

    @Test
    public void whenPathDoesNotExist_createOutputStream_DoesNotDeletePath() throws IOException {
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        Mockito.doReturn(path).when(hdfsWriter).createPath();
        hdfsWriter.createOutputStream();
        Mockito.verify(fileSystem).create(path);
        Mockito.verify(fileSystem, Mockito.never()).delete(path, true);
    }

    @Test
    public void whenBatchSizeReachesFlushSize_write_FlushesBatch() throws IOException {
        Mockito.when(environment.getProperty("flush.size")).thenReturn("1");
        hdfsWriter.initialise();
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        Mockito.doReturn(path).when(hdfsWriter).createPath();
        hdfsWriter.write("Hello!");
        Mockito.verify(outputStream).writeUTF("Hello!\n");
        Mockito.verify(outputStream).hflush();
    }

    @Test
    public void whenBatchSizeIsLessThanFlushSize_write_DoesNotFlushBatch() throws IOException {
        Mockito.when(environment.getProperty("flush.size")).thenReturn("100");
        hdfsWriter.initialise();
        Mockito.when(environment.getProperty("output.path")).thenReturn("exampleFile.txt");
        Mockito.when(fileSystem.exists(path)).thenReturn(false);
        Mockito.doReturn(path).when(hdfsWriter).createPath();
        hdfsWriter.write("Hello!");
        Mockito.verify(outputStream).writeUTF("Hello!\n");
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
        Mockito.verify(outputStream).writeUTF("Hello!\n");
    }
}
