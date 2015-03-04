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
public class PathCreatorTest {

    @Mock
    private FilenameService filenameService;

    @InjectMocks
    private PathCreator pathCreator;

    @Test(expected = NullPointerException.class)
    public void givenANullRequestedPath_createNewPath_throwsANullPointerException() {
        pathCreator.createNewPath(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void givenAnEmptyRequestedPath_createNewPath_throwsAnIllegalArgumentException() {
        pathCreator.createNewPath("");
    }

    @Test
    public void givenAValidPath_createNewPath_returnsAPath() {
        String requestedPath = "/users/me/test.txt";
        String incrementedPath = "/users/me/test.txt-000000000";
        Mockito.when(filenameService.getNewFilename(requestedPath)).thenReturn(incrementedPath);
        Path path = pathCreator.createNewPath(requestedPath);
        Assert.assertEquals(incrementedPath, path.toString());
    }
}
