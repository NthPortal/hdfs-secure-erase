package tmp.namespace.undecided;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tmp.namespace.undecided.specs.BasicFileDeleteSpec;

import java.io.IOException;

public class SecureErase {
    public static void eraseFile(String fileName, FileErasureSpecProvider provider) throws IOException {
        eraseFile(fileName, provider.get());
    }

    public static void eraseFile(String fileName, FileErasureSpec erasureSpec) throws IOException {
        if (erasureSpec.isTerminal()) {
            doEraseFile(fileName, erasureSpec);
        } else {
            // TODO: 3/30/17 add parameter to specify this behavior
            doEraseFile(fileName, erasureSpec.andThen(new BasicFileDeleteSpec()));
        }
    }

    private static void doEraseFile(String fileName, FileErasureSpec erasureSpec) throws IOException {
        Path path = new Path(fileName);
        // TODO: 3/30/17 check that path is a regular file

        FileSystem fs = FileSystem.get(new Configuration());

        erasureSpec.eraseFile(fs, path);
    }
}
