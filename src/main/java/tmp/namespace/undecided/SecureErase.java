package tmp.namespace.undecided;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tmp.namespace.undecided.specs.AdvancedFileDeletionSpec;
import tmp.namespace.undecided.specs.AdvancedFileDeletionSpec.Conf;

import java.io.IOException;

public final class SecureErase {
    public static void eraseFile(String fileName, FileErasureSpecProvider provider) throws IOException {
        eraseFile(fileName, provider.get());
    }

    public static void eraseFile(String fileName, FileErasureSpec erasureSpec) throws IOException {
        if (erasureSpec.isTerminal()) {
            doEraseFile(fileName, erasureSpec);
        } else {
            doEraseFile(fileName, erasureSpec.andThen(new AdvancedFileDeletionSpec(Conf.newBuilder().result())));
        }
    }

    private static void doEraseFile(String fileName, FileErasureSpec erasureSpec) throws IllegalArgumentException, IOException {
        Path path = new Path(fileName);
        try (FileSystem fs = FileSystem.get(new Configuration())) {
            // Check that path is a regular file
            Preconditions.checkArgument(fs.getFileStatus(path).isFile(), "Path is not a regular file: " + path);

            erasureSpec.eraseFile(fs, path);
        }
    }
}
