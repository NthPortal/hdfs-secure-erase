package com.nthportal.hadoop.hdfs.erase.core.specs;

import com.nthportal.hadoop.hdfs.erase.core.FileErasureSpec;

/**
 * Abstract specification for deleting files.
 */
abstract class FileDeletionSpec extends FileErasureSpec {
    @Override
    public final boolean isTerminal() {
        return true;
    }
}
