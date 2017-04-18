package tmp.namespace.undecided.core;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Static utility for securely erasing files.
 */
public final class SecureErase {
    private SecureErase() {}

    /**
     * Erases a file on HDFS.
     *
     * @param filePath    the string path to the file
     * @param erasureSpec the {@link FileErasureSpec} with which to erase the file
     * @throws IllegalArgumentException if the path does not exist or does not refer
     *                                  to a regular file
     * @throws IOException              if an I/O error occurs while erasing the file
     */
    public static void eraseFile(String filePath, FileErasureSpec erasureSpec) throws IllegalArgumentException, IOException {
        Path path = new Path(filePath);
        try (FileSystem fs = FileSystem.get(new Configuration())) {
            // Check that path is a regular file
            Preconditions.checkArgument(fs.exists(path), "File does not exist: " + path);
            Preconditions.checkArgument(fs.getFileStatus(path).isFile(), "Path is not a regular file: " + path);

            erasureSpec.eraseFile(fs, path);
        }
    }
}
