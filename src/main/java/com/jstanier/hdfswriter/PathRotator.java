package com.jstanier.hdfswriter;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

@Component
public class PathRotator {

    private Logger log = LoggerFactory.getLogger(PathRotator.class);

    @Autowired
    private FilenameService filenameService;

    public Path rotatePath(Path existingPath) throws IllegalArgumentException, Exception {
        Preconditions.checkNotNull(existingPath);
        Path rotatedPath = new Path(filenameService.incrementFilename(existingPath.toString()));
        log.info("Rotated file {}", rotatedPath.toString());
        return rotatedPath;

    }

}
