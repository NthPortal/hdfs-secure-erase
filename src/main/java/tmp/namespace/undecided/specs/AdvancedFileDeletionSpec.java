package tmp.namespace.undecided.specs;

import com.google.common.primitives.Ints;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public final class AdvancedFileDeletionSpec extends FileDeletionSpec {
    private static final int MINIMUM_FILE_TRUNCATIONS = 20;

    private final Conf conf;

    public AdvancedFileDeletionSpec(Conf conf) {
        this.conf = conf;
    }

    @Override
    public void eraseFile(FileSystem fs, Path path) throws IOException {
        if (conf.obfuscateFileSize()) {
            // Truncate file
            long originalSize = fs.getFileStatus(path).getLen();

            // If size is larger than Integer.MAX_VALUE, truncate to Integer.MAX_VALUE
            // so future calculations cannot overflow the size of a double
            int initialSize;
            if (originalSize > Integer.MAX_VALUE) {
                initialSize = Integer.MAX_VALUE;
                fs.truncate(path, initialSize);
            } else {
                initialSize = Ints.checkedCast(originalSize); // should never fail, because smaller than Integer.MAX_VALUE
            }

            // Truncate MINIMUM_FILE_TRUNCATIONS times, unless the file is fewer than
            // that many bytes in length
            int truncations = Math.min(initialSize, MINIMUM_FILE_TRUNCATIONS);
            for (int i = 1; i <= truncations; i++) {
                long newSize = originalSize - (long) (initialSize / (double) truncations * i);
                fs.truncate(path, newSize);
            }
        }

        if (conf.obfuscateFileName()) {
            // Rename file to all '0's of the same length, then reduce size
            // by one character until of size 1 (based on GNU shred utility)
            Path parent = path.getParent();
            String newName = StringUtils.leftPad("", path.getName().length(), '0');
            Path prevPath;
            while (newName.length() > 0) {
                prevPath = path;
                path = new Path(parent, newName);
                fs.rename(prevPath, path);
                newName = newName.substring(0, newName.length() - 1);
            }
        }

        fs.delete(path, false);
    }

    public static final class Conf {
        private final boolean obfuscateFileName;
        private final boolean obfuscateFileSize;

        private Conf(boolean obfuscateFileName, boolean obfuscateFileSize) {
            this.obfuscateFileName = obfuscateFileName;
            this.obfuscateFileSize = obfuscateFileSize;
        }

        public boolean obfuscateFileName() {
            return obfuscateFileName;
        }

        public boolean obfuscateFileSize() {
            return obfuscateFileSize;
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static final class Builder {
            private boolean obfuscateFileName = true;
            private boolean obfuscateFileSize = false;

            private Builder() {}

            public Builder obfuscateFileName(boolean obfuscateFileName) {
                this.obfuscateFileName = obfuscateFileName;
                return this;
            }

            public Builder obfuscateFileSize(boolean obfuscateFileSize) {
                this.obfuscateFileSize = obfuscateFileSize;
                return this;
            }

            public Conf result() {
                return new Conf(obfuscateFileName, obfuscateFileSize);
            }
        }
    }
}
