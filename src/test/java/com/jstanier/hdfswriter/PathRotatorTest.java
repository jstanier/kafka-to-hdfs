package com.jstanier.hdfswriter;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PathRotatorTest {

    @Mock
    private FilenameService filenameService;

    @InjectMocks
    private PathRotator pathRotator;

    @Mock
    private Path path;

    @Test(expected = NullPointerException.class)
    public void givenANullPath_rotatePath_throwsANullPointerException()
            throws IllegalArgumentException, Exception {
        pathRotator.rotatePath(null);
    }

    @Test
    public void givenAValidPath_rotatePath_rotatesItToTheNextIncrement()
            throws IllegalArgumentException, Exception {
        String existingFilename = "/users/me/test.txt-000000000";
        Mockito.when(path.toString()).thenReturn(existingFilename);
        String incrementedFilename = "/users/me/test.txt-000000001";
        Mockito.when(filenameService.incrementFilename(existingFilename)).thenReturn(
                incrementedFilename);
        Path rotatedPath = pathRotator.rotatePath(path);
        Assert.assertEquals(incrementedFilename, rotatedPath.toString());
    }
}
