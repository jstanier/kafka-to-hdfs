package com.jstanier.hdfswriter;

import java.io.IOException;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class StreamConsumerTest {

    @Mock
    private HDFSWriter hdfsWriter;

    @Mock
    private KafkaStream<byte[], byte[]> stream;

    @Mock
    private MessageAndMetadata<byte[], byte[]> message;

    @Mock
    private Logger log;

    @Mock
    private ConsumerIterator<byte[], byte[]> iterator;

    private StreamConsumer streamConsumer;

    @Before
    public void setup() {
        streamConsumer = new StreamConsumer(hdfsWriter, stream);
        Whitebox.setInternalState(streamConsumer, "log", log);
    }

    @Test
    public void whenStreamIteratorDoesNotHaveNext_run_returns() throws InterruptedException {
        Mockito.when(stream.iterator()).thenReturn(iterator);
        Mockito.when(iterator.hasNext()).thenReturn(false);
        streamConsumer.run();
        Mockito.verifyNoMoreInteractions(hdfsWriter);
    }

    @Test
    public void whenStreamIteratorHasNext_run_writesToHdfs() throws InterruptedException,
            IOException {
        Mockito.when(stream.iterator()).thenReturn(iterator);
        Mockito.when(iterator.hasNext()).thenReturn(true, false);
        Mockito.when(iterator.next()).thenReturn(message);
        byte[] messageBytes = "Hello!".getBytes();
        Mockito.when(message.message()).thenReturn(messageBytes);
        streamConsumer.run();
        Mockito.verify(hdfsWriter).write(messageBytes.toString());
    }

    @Test
    public void whenHdfsWriterThrowsIOException_run_logsAnErrorThenReturnsWithoutThrowingAnException()
            throws InterruptedException, IOException {
        Mockito.when(stream.iterator()).thenReturn(iterator);
        Mockito.when(iterator.hasNext()).thenReturn(true);
        Mockito.when(iterator.next()).thenReturn(message);
        byte[] messageBytes = "Hello!".getBytes();
        Mockito.when(message.message()).thenReturn(messageBytes);
        try {
            Mockito.doThrow(new IOException()).when(hdfsWriter).write(messageBytes.toString());
            streamConsumer.run();
        } catch (IOException e) {
            throw e;
        }
        Mockito.verify(log).error(Mockito.anyString(), Mockito.any(IOException.class));
    }
}
