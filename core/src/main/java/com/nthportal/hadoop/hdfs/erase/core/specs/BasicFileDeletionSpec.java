package com.nthportal.hadoop.hdfs.erase.core.specs;

import com.nthportal.hadoop.hdfs.erase.core.FileErasureSpec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * An {@link FileErasureSpec} which deletes a file.
 */
public final class BasicFileDeletionSpec extends FileDeletionSpec {
    private static final Logger logger = Logger.getLogger(BasicFileDeletionSpec.class);

    @Override
    public void eraseFile(FileSystem fs, Path path) throws IOException {
        if (isLoggingEnabled()) {
            logger.info("Deleting file: " + path);
        }

        fs.delete(path, false);
    }
}
