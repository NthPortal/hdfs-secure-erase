package com.nthportal.hadoop.hdfs.erase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.IOException;

final class BlockHelper {
    private BlockHelper() {}

    static void overwriteLocalFile(Path path) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        String dataDir = conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
        System.out.println(dataDir);


    }
}
