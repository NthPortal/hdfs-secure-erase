package com.nthportal.hadoop.hdfs.erase.core;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
     * Erases files on HDFS matching a glob.
     *
     * @param pathGlob    a {@link Path} containing a
     *                    {@link FileSystem#globStatus(Path) glob pattern}
     * @param erasureSpec the {@link FileErasureSpec} with which to erase the files
     *                    matching the glob
     * @throws IllegalArgumentException if the glob matches no files or non-regular
     *                                  files
     * @throws IOException              if an I/O error occurs while erasing the
     *                                  files matching the glob
     * @see FileSystem#globStatus(Path)
     */
    public void eraseGlob(Path pathGlob, FileErasureSpec erasureSpec) throws IllegalArgumentException, IOException {
        preConfigure();
        erasureSpec.setConf(getConf());

        try (FileSystem fs = FileSystem.get(getConf())) {
            FileStatus[] statuses = fs.globStatus(pathGlob);
            Preconditions.checkArgument(statuses.length > 0, "glob does not match any files: " + pathGlob);

            // Check that statuses refer to regular files
            for (FileStatus status : statuses) {
                Preconditions.checkArgument(status.isFile(), "Path is not a regular file: " + status.getPath());
            }

            // Erase files
            for (FileStatus status : statuses) {
                erasureSpec.eraseFile(fs, status.getPath());
            }
        }
    }

    /**
     * Erases a file on HDFS.
     *
     * @param path        the path to the file (MUST NOT be a glob)
     * @param erasureSpec the {@link FileErasureSpec} with which to erase the file
     * @throws IllegalArgumentException if the path does not exist or does not refer
     *                                  to a regular file
     * @throws IOException              if an I/O error occurs while erasing the file
     */
    public void eraseFile(Path path, FileErasureSpec erasureSpec) throws IllegalArgumentException, IOException {
        preConfigure();
        erasureSpec.setConf(getConf());

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
