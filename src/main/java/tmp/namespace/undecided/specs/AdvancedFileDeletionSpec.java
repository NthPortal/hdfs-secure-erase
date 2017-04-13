package tmp.namespace.undecided.specs;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Configurable specification for deleting files.
 */
public final class AdvancedFileDeletionSpec extends FileDeletionSpec {
    private static final long SMALL_FILE_THRESHOLD = 64;
    private static final List<Character> FILE_NAME_CHARS;
    private static final long TRUNCATE_WAIT_MILLIS = 20;

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

    /**
     * Creates a specification from the specified configuration.
     *
     * @param conf the configuration for the spec
     */
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

    /**
     * Truncates a file.
     *
     * @param fs   the file system on which the file resides
     * @param path the path to the file
     * @throws IOException if an I/O error occurs while truncating the file
     */
    private static void truncateFile(FileSystem fs, Path path) throws IOException {
        long size = fs.getFileStatus(path).getLen();

        if (size > SMALL_FILE_THRESHOLD) {
            // Resize to power of 2 bytes
            size = 1L << (Long.SIZE - 1 - Long.numberOfLeadingZeros(size));
            truncateToSize(fs, path, size);

            // Cut size in half until small
            while (size > SMALL_FILE_THRESHOLD) {
                size >>= 1;
                truncateToSize(fs, path, size);
            }
        }

        truncateSmallFile(fs, path, size);
    }

    /**
     * Truncates a file smaller than SMALL_FILE_THRESHOLD.
     *
     * @param fs   the file system on which the file resides
     * @param path the path to the file
     * @param size the size of the file in bytes
     * @throws IOException if an I/O error occurs while truncating the file
     */
    private static void truncateSmallFile(FileSystem fs, Path path, long size) throws IOException {
        // shrink size by about 1/8 (however, always at least 1 byte)
        // until size is 0
        while (size > 0) {
            size -= (Math.max(size >> 3, 1));
            truncateToSize(fs, path, size);
        }
    }

    /**
     * Truncates a file to a specified size, waiting if necessary for the truncation
     * to finish.
     *
     * @param fs   the file system on which the file resides
     * @param path the path to the file
     * @param size the desired size of the file in bytes
     * @throws IOException if an I/O error occurs while truncating the file
     */
    private static void truncateToSize(FileSystem fs, Path path, long size) throws IOException {
        boolean res = fs.truncate(path, size);
        if (!res) {
            // loop and wait until file is truncated
            do {
                Uninterruptibles.sleepUninterruptibly(TRUNCATE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } while (fs.getFileStatus(path).getLen() != size);
        }
    }

    /**
     * Obfuscates the name of a file.
     *
     * @param fs   the file system on which the file resides
     * @param path the path to the file
     * @return the new path to the file
     * @throws IOException if an I/O error occurs while renaming the file
     */
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

    /**
     * Finds a path with an obfuscated file name of a specified length
     * which does not already exist on a filesystem.
     *
     * @param fs the file system on which to find the path
     * @param parent the path of the parent directory to contain the file
     * @param length the length of the file name
     * @return an obfuscated path which does not exist on the file system, or
     * null if no path could be found
     * @throws IOException if an I/O error occurs while finding the path
     */
    private static Path findPathOfLength(FileSystem fs, Path parent, int length) throws IOException {
        try {
            Iterator<String> names = namesOfLength(length);

            while (names.hasNext()) {
                Path path = new Path(parent, names.next());
                if (!fs.exists(path)) {
                    return path;
                }
            }
        } catch (StackOverflowError ignored) {
            // In the astronomically unlikely case that the file name
            // is absurdly long, nearly all possible permutations of
            // obfuscation names are already taken as names of other
            // files, and the iterator overflows the stack, this
            // handles it.
        }

        // No file name not in use found
        return null;
    }

    /**
     * Returns an iterator over possible file names of a given length.
     *
     * @param length the length of the file names
     * @return an iterator over possible file names of a given length
     */
    private static Iterator<String> namesOfLength(final int length) {
        return new Iterator<String>() {
            private Iterator<Character> characterIterator = FILE_NAME_CHARS.iterator();
            private Iterator<String> prefixIteratorInstance = null;
            private String prefix = StringUtils.leftPad("", length - 1, '0');

            private Iterator<String> prefixIterator() {
                if (prefixIteratorInstance == null && length > 1) {
                    prefixIteratorInstance = namesOfLength(length - 1);
                    prefixIteratorInstance.next(); // drop prefix matching initial prefix
                }
                return prefixIteratorInstance;
            }

            @Override
            public boolean hasNext() {
                return characterIterator.hasNext() || (prefixIterator() != null && prefixIterator().hasNext());
            }

            @Override
            public String next() {
                if (characterIterator.hasNext()) {
                    return prefix + characterIterator.next();
                } else {
                    if (prefixIterator() != null) {
                        prefix = prefixIterator().next();
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

    /**
     * A configuration for an {@link AdvancedFileDeletionSpec}.
     */
    public static final class Conf {
        private final boolean obfuscateFileName;
        private final boolean obfuscateFileSize;

        private Conf(boolean obfuscateFileName, boolean obfuscateFileSize) {
            this.obfuscateFileName = obfuscateFileName;
            this.obfuscateFileSize = obfuscateFileSize;
        }

        /**
         * Whether or not to obfuscate the name of the file.
         *
         * @return whether or not to obfuscate the name of the file
         */
        public boolean obfuscateFileName() {
            return obfuscateFileName;
        }

        /**
         * Whether or not to obfuscate the size of the file.
         *
         * @return whether or not to obfuscate the size of the file
         */
        public boolean obfuscateFileSize() {
            return obfuscateFileSize;
        }

        /**
         * Returns a new {@link Builder} with the same values as this {@code Conf}.
         *
         * @return a new Builder with the same values as this Conf
         */
        public Builder toBuilder() {
            return newBuilder()
                    .obfuscateFileName(obfuscateFileName)
                    .obfuscateFileSize(obfuscateFileSize);
        }

        /**
         * Returns a new configuration builder.
         *
         * @return a new configuration builder.
         */
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Returns the default {@code Conf}.
         *
         * @return the default Conf.
         */
        public static Conf defaultConf() {
            return newBuilder().result();
        }

        /**
         * A builder for a {@link Conf}.
         */
        public static final class Builder {
            private boolean obfuscateFileName = true;
            private boolean obfuscateFileSize = false;

            private Builder() {}

            /**
             * Sets whether or not to obfuscate the name of the file.
             *
             * @param obfuscateFileName whether or not to obfuscate the name
             *                          of the file
             * @return this builder
             */
            public Builder obfuscateFileName(boolean obfuscateFileName) {
                this.obfuscateFileName = obfuscateFileName;
                return this;
            }

            /**
             * Sets whether or not to obfuscate the size of the file.
             *
             * @param obfuscateFileSize whether or not to obfuscate the size
             *                          of the file
             * @return this builder
             */
            public Builder obfuscateFileSize(boolean obfuscateFileSize) {
                this.obfuscateFileSize = obfuscateFileSize;
                return this;
            }

            /**
             * Returns a configuration from this builder.
             *
             * @return a configuration from this builder
             */
            public Conf result() {
                return new Conf(obfuscateFileName, obfuscateFileSize);
            }
        }
    }
}
