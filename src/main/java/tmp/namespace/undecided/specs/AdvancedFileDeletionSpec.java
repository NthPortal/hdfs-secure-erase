package tmp.namespace.undecided.specs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

/**
 * Configurable specification for deleting files.
 */
public final class AdvancedFileDeletionSpec extends FileDeletionSpec {
    private static final long SMALL_FILE_THRESHOLD = 64;
    private static final List<Character> FILE_NAME_CHARS;

    static {
        // Create list of possible characters for file names
        ArrayList<Character> list = new ArrayList<>();

        // Digits
        for (int i = 0; i < 10; i++) {
            list.add((char) ('0' + i));
        }
        // Lowercase letters
        for (int i = 0; i < 26; i++) {
            list.add((char) ('a' + i));
        }
        // Uppercase letters
        for (int i = 0; i < 26; i++) {
            list.add((char) ('A' + i));
        }
        // Symbols
        list.add('_');
        list.add('-');

        list.trimToSize();
        FILE_NAME_CHARS = Collections.unmodifiableList(list);
    }

    private final Conf conf;

    public AdvancedFileDeletionSpec(Conf conf) {
        this.conf = conf;
    }

    @Override
    public void eraseFile(FileSystem fs, Path path) throws IOException {
        if (conf.obfuscateFileSize()) {
            truncateFile(fs, path);
        }

        if (conf.obfuscateFileName()) {
            path = obfuscateFileName(fs, path);
        }

        fs.delete(path, false);
    }

    private static void truncateFile(FileSystem fs, Path path) throws IOException {
        long size = fs.getFileStatus(path).getLen();

        if (size > SMALL_FILE_THRESHOLD) {
            // Resize to power of 2 bytes
            size = 1L << (Long.SIZE - 1 - Long.numberOfLeadingZeros(size));
            fs.truncate(path, size);

            // Cut size in half until small
            while (size > SMALL_FILE_THRESHOLD) {
                size >>= 1;
                fs.truncate(path, size);
            }
        }

        truncateSmallFile(fs, path, size);
    }

    private static void truncateSmallFile(FileSystem fs, Path path, long size) throws IOException {
        // shrink size by about 1/8 (however, always at least 1 byte)
        // until size is 0
        while (size > 0) {
            size -= (Math.max(size >> 3, 1));
            fs.truncate(path, size);
        }
    }

    private static Path obfuscateFileName(FileSystem fs, Path path) throws IOException {
        Path parent = path.getParent();
        int currentNameLength = path.getName().length();

        while (currentNameLength > 0) {
            Path newPath = findPathOfLength(fs, parent, currentNameLength);
            if (newPath != null) {
                fs.rename(path, newPath);
                path = newPath;
            }
            currentNameLength--;
        }

        return path;
    }

    private static Path findPathOfLength(FileSystem fs, Path parent, int length) throws IOException {
        Iterator<String> names = namesOfLength(length);

        while (names.hasNext()) {
            Path path = new Path(parent, names.next());
            if (!fs.exists(path)) {
                return path;
            }
        }

        // No file name not in use found
        return null;
    }

    private static Iterator<String> namesOfLength(final int length) {
        return new Iterator<String>() {
            private Iterator<Character> characterIterator = FILE_NAME_CHARS.iterator();
            private final Iterator<String> prefixIterator = (length > 1) ? namesOfLength(length - 1) : null;
            private String prefix = (prefixIterator != null) ? prefixIterator.next() : null;

            @Override
            public boolean hasNext() {
                return characterIterator.hasNext() || (prefixIterator != null && prefixIterator.hasNext());
            }

            @Override
            public String next() {
                if (characterIterator.hasNext()) {
                    if (prefix != null) {
                        return prefix + characterIterator.next();
                    } else {
                        return characterIterator.next().toString();
                    }
                } else {
                    if (prefixIterator != null) {
                        prefix = prefixIterator.next();
                        characterIterator = FILE_NAME_CHARS.iterator();
                        return next();
                    } else {
                        throw new NoSuchElementException("Empty Iterator");
                    }
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
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
