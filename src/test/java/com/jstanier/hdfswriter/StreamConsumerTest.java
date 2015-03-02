package com.jstanier.hdfswriter;

import java.io.IOException;

import kafka.message.MessageAndMetadata;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import com.brandwatch.kafka.consumer.StreamIterator;

@RunWith(MockitoJUnitRunner.class)
public class StreamConsumerTest {

    @Mock
    private StreamIterator<String, String> streamIterator;

    @Mock
    private HDFSWriter hdfsWriter;

    @InjectMocks
    private StreamConsumer streamConsumer;

    @Mock
    private MessageAndMetadata<String, String> message;

    @Mock
    private Logger log;

    @Before
    public void setup() {
        Whitebox.setInternalState(streamConsumer, "log", log);
    }

    @Test
    public void whenStreamIteratorDoesNotHaveNext_run_returns() throws InterruptedException {
        Mockito.when(streamIterator.hasNext()).thenReturn(false);
        streamConsumer.run();
        Mockito.verifyNoMoreInteractions(hdfsWriter);
    }

    @Test
    public void whenStreamIteratorHasNext_run_writesToHdfs() throws InterruptedException,
            IOException {
        Mockito.when(streamIterator.hasNext()).thenReturn(true, false);
        Mockito.when(message.message()).thenReturn("Hello!");
        Mockito.when(streamIterator.next()).thenReturn(message);
        streamConsumer.run();
        Mockito.verify(hdfsWriter).write("Hello!");
    }

    @Test
    public void whenHdfsWriterThrowsIOException_run_logsAnErrorThenReturnsWithoutThrowingAnException()
            throws InterruptedException, IOException {
        Mockito.when(streamIterator.hasNext()).thenReturn(true);
        Mockito.when(streamIterator.next()).thenReturn(message);
        Mockito.when(message.message()).thenReturn("Hello!");
        try {
            Mockito.doThrow(new IOException()).when(hdfsWriter).write("Hello!");
            streamConsumer.run();
        } catch (IOException e) {
            throw e;
        }
        Mockito.verify(log).error(Mockito.anyString(), Mockito.any(IOException.class));
    }
}
