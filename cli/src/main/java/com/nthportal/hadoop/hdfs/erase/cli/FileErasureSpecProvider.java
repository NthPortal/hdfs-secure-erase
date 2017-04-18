package com.nthportal.hadoop.hdfs.erase.cli;

import com.nthportal.hadoop.hdfs.erase.core.FileErasureSpec;

/**
 * Provides an {@link FileErasureSpec}.
 */
public interface FileErasureSpecProvider {
    /**
     * Returns an {@link FileErasureSpec}.
     *
     * @return an ErasureSpec
     */
    FileErasureSpec get();
}
