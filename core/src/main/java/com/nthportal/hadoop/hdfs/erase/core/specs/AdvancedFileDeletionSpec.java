package com.nthportal.hadoop.hdfs.erase.core.specs;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

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
    private static final Logger logger = Logger.getLogger(AdvancedFileDeletionSpec.class);

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
        boolean logActions = isLoggingEnabled();
        if (logActions) {
            logger.info("Deleting file: " + path);
        }

        if (conf.obfuscateFileSize()) {
            if (logActions) {
                logger.info("Truncating file: " + path);
            }
            truncateFile(fs, path, logActions);
        }

        if (conf.obfuscateFileName()) {
            if (logActions) {
                logger.info("Obfuscating file name: " + path);
            }
            path = obfuscateFileName(fs, path, logActions);
        }

        if (logActions) {
            logger.info("Removing file: " + path);
        }
        fs.delete(path, false);
    }

    /**
     * Truncates a file.
     *
     * @param fs         the file system on which the file resides
     * @param path       the path to the file
     * @param logActions whether or not to log actions
     * @throws IOException if an I/O error occurs while truncating the file
     */
    private static void truncateFile(FileSystem fs, Path path, boolean logActions) throws IOException {
        long size = fs.getFileStatus(path).getLen();

        if (size > SMALL_FILE_THRESHOLD) {
            // Resize to power of 2 bytes
            size = 1L << (Long.SIZE - 1 - Long.numberOfLeadingZeros(size));
            truncateToSize(fs, path, size, logActions);

            // Cut size in half until small
            while (size > SMALL_FILE_THRESHOLD) {
                size >>= 1;
                truncateToSize(fs, path, size, logActions);
            }
        }

        truncateSmallFile(fs, path, size, logActions);
    }

    /**
     * Truncates a file smaller than SMALL_FILE_THRESHOLD.
     *
     * @param fs         the file system on which the file resides
     * @param path       the path to the file
     * @param size       the size of the file in bytes
     * @param logActions whether or not to log actions
     * @throws IOException if an I/O error occurs while truncating the file
     */
    private static void truncateSmallFile(FileSystem fs, Path path, long size, boolean logActions) throws IOException {
        // shrink size by about 1/8 (however, always at least 1 byte)
        // until size is 0
        while (size > 0) {
            size -= (Math.max(size >> 3, 1));
            truncateToSize(fs, path, size, logActions);
        }
    }

    /**
     * Truncates a file to a specified size, waiting if necessary for the truncation
     * to finish.
     *
     * @param fs         the file system on which the file resides
     * @param path       the path to the file
     * @param size       the desired size of the file in bytes
     * @param logActions whether or not to log actions
     * @throws IOException if an I/O error occurs while truncating the file
     */
    private static void truncateToSize(FileSystem fs, Path path, long size, boolean logActions) throws IOException {
        if (logActions) {
            logger.debug("Truncating to size: " + size + "B");
        }
        boolean res = fs.truncate(path, size);
        if (!res) {
            // loop and wait until file is truncated
            if (logActions) {
                logger.debug("Waiting for file to finish being truncated...");
            }
            do {
                Uninterruptibles.sleepUninterruptibly(TRUNCATE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } while (fs.getFileStatus(path).getLen() != size);
        }
    }

    /**
     * Obfuscates the name of a file.
     *
     * @param fs         the file system on which the file resides
     * @param path       the path to the file
     * @param logActions whether or not to log actions
     * @return the new path to the file
     * @throws IOException if an I/O error occurs while renaming the file
     */
    private static Path obfuscateFileName(FileSystem fs, Path path, boolean logActions) throws IOException {
        return new FileNameObfuscator(fs, path, logActions).obfuscate();
    }

    /**
     * Obfuscates a file's name.
     */
    private static final class FileNameObfuscator {
        private static final int SHORT_NAME_MAX_LENGTH = 4;
        private static final int MEDIUM_NAME_MAX_LENGTH = 10;
        private static final int SHORT_NAME_ATTEMPTS = 4;
        private static final int MEDIUM_NAME_ATTEMPTS = 3;
        private static final int LONG_NAME_ATTEMPTS = 2;
        private static final int SHORT_NAME_MAX_FAILURES = 0;
        private static final int MEDIUM_NAME_MAX_FAILURES = 1;
        private static final int LONG_NAME_MAX_FAILURES = 3;

        private final FileSystem fs;
        private final Path parent;
        private final boolean log;

        private Path path;

        /**
         * Creates a new {@code FileNameObfuscator}.
         *
         * @param fs         the file system on which the file resides
         * @param path       the path to the file
         * @param logActions whether or not to log actions
         */
        FileNameObfuscator(FileSystem fs, Path path, boolean logActions) {
            this.fs = fs;
            this.path = path;
            parent = path.getParent();
            this.log = logActions;
        }

        /**
         * Obfuscates the name of the file.
         *
         * @return the new path to the file
         * @throws IOException if an I/O error occurs while renaming the file
         */
        Path obfuscate() throws IOException {
            int nameLength = path.getName().length();

            if (nameLength <= SHORT_NAME_MAX_LENGTH) {
                obfuscateFileName(nameLength, SHORT_NAME_ATTEMPTS, SHORT_NAME_MAX_FAILURES);
            } else if (nameLength <= MEDIUM_NAME_MAX_LENGTH) {
                obfuscateFileName(nameLength, MEDIUM_NAME_ATTEMPTS, MEDIUM_NAME_MAX_FAILURES);
            } else {
                obfuscateFileName(nameLength, LONG_NAME_ATTEMPTS, LONG_NAME_MAX_FAILURES);
            }

            return path;
        }

        /**
         * Obfuscates the name of the file based on the specified parameters.
         *
         * @param currentNameLength the current length of the file name
         * @param attempts          the maximum number of attempts to obfuscate
         *                          a file to a given length
         * @param maxFailures       the maximum number of failed obfuscations
         *                          (to a given length) allowed
         * @throws IOException if an I/O error occurred and the file name could
         *                     not be fully obfuscated
         */
        private void obfuscateFileName(int currentNameLength, int attempts, int maxFailures) throws IOException {
            int failures = 0;

            while (currentNameLength > 0) {
                if (!obfuscateToLength(currentNameLength, attempts)) {
                    failures += 1;
                }
                if (failures > maxFailures) {
                    throw new IOException("Failed to obfuscate file name for: " + path.toString());
                }
                currentNameLength--;
            }
        }

        /**
         * Renames the file to an obfuscated name of the specified length.
         *
         * @param targetLength the length of the obfuscated name
         * @param attempts     the maximum number of attempts to obfuscate the name
         *                     to the specified length
         * @return true if the file name was obfuscated to the specified length;
         * false otherwise
         * @throws IOException if an I/O error occurred while attempting to
         *                     obfuscate the file name
         */
        private boolean obfuscateToLength(int targetLength, int attempts) throws IOException {
            for (int i = 0; i < attempts; i++) {
                Path newPath = findPathOfLength(fs, parent, targetLength);
                if (newPath != null && rename(path, newPath)) {
                    if (log) {
                        logger.debug("Renamed '" + path + "' to '" + newPath + "'");
                    }
                    path = newPath;
                    return true;
                }
            }
            return false;
        }

        /**
         * Renames {@code src} to {@code dest}.
         *
         * <p>Because the documentation for {@link FileSystem#rename(Path, Path)}
         * is unclear, this method unambiguously returns a {@code boolean}
         * specifying whether or not the renaming was successful, and does not
         * throw an {@link IOException}.
         *
         * @param src  the source path
         * @param dest the destination path
         * @return true if the path was renamed; false otherwise
         */
        private boolean rename(Path src, Path dest) {
            try {
                return fs.rename(src, dest);
            } catch (IOException ignored) {
                if (log) {
                    logger.warn("Failed to rename '" + src + "' to '" + dest + "'");
                }
                return false;
            }
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
            private boolean obfuscateFileSize = true;

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
