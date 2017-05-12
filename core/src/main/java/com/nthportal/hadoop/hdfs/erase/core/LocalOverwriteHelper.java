package com.nthportal.hadoop.hdfs.erase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.IOException;

final class LocalOverwriteHelper {
    private LocalOverwriteHelper() {}

    static void overwriteBlock(BlockLocation location, int index, long length) throws IOException {

    }
}
