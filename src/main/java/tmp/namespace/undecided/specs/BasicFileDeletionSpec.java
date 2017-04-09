package tmp.namespace.undecided.specs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tmp.namespace.undecided.FileErasureSpec;

import java.io.IOException;

/**
 * An {@link FileErasureSpec} which deletes a file.
 */
public final class BasicFileDeletionSpec extends FileDeletionSpec {
    @Override
    public void eraseFile(FileSystem fs, Path path) throws IOException {
        fs.delete(path, false);
    }
}
