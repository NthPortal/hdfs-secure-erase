package tmp.namespace.undecided.specs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tmp.namespace.undecided.ErasureSpec;

import java.io.IOException;

/**
 * An {@link ErasureSpec} which deletes a file.
 */
public class BasicDeleteSpec extends ErasureSpec {
    @Override
    public void eraseFile(FileSystem fs, Path path) throws IOException {
        fs.delete(path, false);
    }

    @Override
    public boolean isTerminal() {
        return true;
    }
}
