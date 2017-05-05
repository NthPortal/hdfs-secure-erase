package com.nthportal.hadoop.hdfs.erase.core;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * {@link org.apache.hadoop.conf.Configurable Configurable} object for
 * securely erasing files.
 */
public final class SecureErase extends NonNullConfigured {
    /**
     * Creates a new {@code SecureErase} with a default {@link Configuration}.
     */
    public SecureErase() {
        this(new Configuration());
    }

    /**
     * Creates a new {@code SecureErase} with the specified {@link Configuration}.
     *
     * @param conf the Configuration to use
     * @see Conf#LOG_ACTIONS
     */
    public SecureErase(Configuration conf) {
        super(conf);
    }

    /**
     * Erases a file on HDFS.
     *
     * @param filePath    the string path to the file
     * @param erasureSpec the {@link FileErasureSpec} with which to erase the file
     * @throws IllegalArgumentException if the path does not exist or does not refer
     *                                  to a regular file
     * @throws IOException              if an I/O error occurs while erasing the file
     */
    public void eraseFile(String filePath, FileErasureSpec erasureSpec) throws IllegalArgumentException, IOException {
        preConfigure();
        erasureSpec.setConf(getConf());

        Path path = new Path(filePath);
        try (FileSystem fs = FileSystem.get(getConf())) {
            // Check that path is a regular file
            Preconditions.checkArgument(fs.exists(path), "File does not exist: " + path);
            Preconditions.checkArgument(fs.getFileStatus(path).isFile(), "Path is not a regular file: " + path);

            erasureSpec.eraseFile(fs, path);
        }
    }

    /**
     * Perform any configuration needed before erasing files.
     */
    private void preConfigure() {
        getConf().setBooleanIfUnset(Conf.LOG_ACTIONS, false);
    }

    /**
     * Utility class for {@link Configuration} constants.
     */
    public static final class Conf {
        /**
         * Whether or not erasure specifications should log their actions.
         */
        public static final String LOG_ACTIONS = "com.nthportal.hdfs-secure-erase.LOG_ACTIONS";

        private Conf() {}
    }
}
