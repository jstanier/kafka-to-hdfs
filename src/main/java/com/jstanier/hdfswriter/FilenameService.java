package com.jstanier.hdfswriter;

import org.springframework.stereotype.Component;

@Component
public class FilenameService {

    public String getNewFilename(String filename) {
        return filename + "-0";
    }

    public String incrementFilename(String existingFilename) throws Exception {
        int indexOfLastHyphen = existingFilename.lastIndexOf("-");
        String increment = existingFilename.substring(indexOfLastHyphen + 1,
                existingFilename.length());

        String originalFilename = existingFilename.substring(0, indexOfLastHyphen);

        long currentIncrement = Long.parseLong(increment);
        if (currentIncrement == Long.MAX_VALUE) {
            throw new Exception();
        }

        String result = originalFilename += "-";
        return result += (currentIncrement + 1);
    }
}
