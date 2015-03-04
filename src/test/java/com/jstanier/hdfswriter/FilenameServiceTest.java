package com.jstanier.hdfswriter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FilenameServiceTest {

    private FilenameService filenameService;

    @Before
    public void setup() {
        filenameService = new FilenameService();
    }

    @Test
    public void givenANewFilename_getNewFilename_returnsWithInitialIncrement() {
        String filename = "/Users/me/tweets.txt";
        String newFilename = filenameService.getNewFilename(filename);
        Assert.assertEquals("/Users/me/tweets.txt-0", newFilename);
    }

    @Test(expected = Exception.class)
    public void givenAnExistingFilenameThatDoesNotEndInANumber_incrementFilename_throwsException()
            throws Exception {
        String incorrectExistingFilename = "/Users/me/tweets.txt";
        filenameService.incrementFilename(incorrectExistingFilename);
    }

    @Test
    public void givenAnExistingNewlyCreatedFilename_incrementFilename_incrementsItByOne()
            throws Exception {
        String existingFilename = "/Users/me/tweets.txt-0";
        String incrementedFilename = filenameService.incrementFilename(existingFilename);
        Assert.assertEquals("/Users/me/tweets.txt-1", incrementedFilename);
    }

    @Test
    public void givenAnExistingCreatedFilenameWithPaddedZeros_incrementFilename_incrementsItByOneAndMaintainsZeroPadding()
            throws Exception {
        String existingFilename = "/Users/me/tweets.txt-22";
        String incrementedFilename = filenameService.incrementFilename(existingFilename);
        Assert.assertEquals("/Users/me/tweets.txt-23", incrementedFilename);
    }

    @Test
    public void givenAnExistingFilename_incrementFilename_incrementsItByOne() throws Exception {
        String existingFilename = "/Users/me/tweets.txt-563947999";
        String incrementedFilename = filenameService.incrementFilename(existingFilename);
        Assert.assertEquals("/Users/me/tweets.txt-563948000", incrementedFilename);
    }

    @Test(expected = Exception.class)
    public void givenAnExistingMaximumFilename_incrementFilename_throwsException() throws Exception {
        String existingFilename = "/Users/me/tweets.txt-9223372036854775807";
        filenameService.incrementFilename(existingFilename);
    }
}
