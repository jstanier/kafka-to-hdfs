package com.jstanier.hdfswriter;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

@Component
public class PathCreator {

    private Logger log = LoggerFactory.getLogger(PathCreator.class);

    @Autowired
    private FilenameService filenameService;

    public Path createNewPath(String requestedPath) {
        Preconditions.checkNotNull(requestedPath);
        Preconditions.checkArgument(Strings.isNotEmpty(requestedPath));
        Path path = new Path(filenameService.getNewFilename(requestedPath));
        log.info("Created path {}", path.toString());
        return path;
    }
}
