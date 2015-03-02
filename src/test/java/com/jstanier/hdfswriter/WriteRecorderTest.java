package com.jstanier.hdfswriter;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class WriteRecorderTest {

    @Mock
    private Logger log;

    @Mock
    private HDFSWriter hdfsWriter;

    private WriteRecorder writeRecorder;

    @Before
    public void setup() {
        writeRecorder = Mockito.spy(WriteRecorder.class);
        Whitebox.setInternalState(writeRecorder, "log", log);
        Whitebox.setInternalState(writeRecorder, "hdfsWriter", hdfsWriter);
    }

    @Test
    public void givenZeroWrites_recordWrite_incrementsWriteCounterByOneAndDoesNotPrintMessage() {
        Mockito.when(hdfsWriter.getFlushSize()).thenReturn(10);
        writeRecorder.recordWrite();
        Mockito.verify(writeRecorder).incrementWrites();
        Mockito.verifyNoMoreInteractions(log);
    }

    @Test
    public void givenOneLessThanFlushSizeWrites_recordWrite_printsLogMessage() {
        Mockito.when(hdfsWriter.getFlushSize()).thenReturn(1);
        writeRecorder.recordWrite();
        Mockito.verify(writeRecorder).incrementWrites();
        Mockito.verify(log).info("Wrote 1 message(s)");
    }
}
