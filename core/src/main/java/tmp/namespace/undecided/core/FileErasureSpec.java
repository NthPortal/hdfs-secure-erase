package tmp.namespace.undecided.core;

import com.google.common.primitives.Ints;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.google.common.base.Preconditions.*;

/**
 * A specification for erasing a file.
 */
public abstract class FileErasureSpec extends ErasureSpec {
    private static final long MAX_INT = Integer.MAX_VALUE;

    /**
     * Erases a file.
     *
     * @param fs   the file system on which the file resides
     * @param path the path to the file
     * @throws IOException if an I/O error occurs during the file erasure
     */
    public abstract void eraseFile(FileSystem fs, Path path) throws IOException;

    /**
     * Returns a {@code FileErasureSpec} which erases files by executing
     * {@link #eraseFile(FileSystem, Path)} on this
     * {@code FileErasureSpec} and then on the specified {@code FileErasureSpec}.
     *
     * @param other the FileErasureSpec to use after this one
     * @return a new FileErasureSpec combining this and the other FileErasureSpec
     * @throws NullPointerException          if `other` is null
     * @throws UnsupportedOperationException if this FileErasureSpec is
     *                                       {@link #isTerminal() terminal}
     */
    public final FileErasureSpec andThen(FileErasureSpec other)
            throws NullPointerException, UnsupportedOperationException {
        checkNotNull(other);
        requireNonTerminal();
        return this.equals(other) ? Repeated.apply(this, 2) : new Cons(this, other);
    }

    /**
     * Returns a {@code FileErasureSpec} which erases files by executing
     * {@link #eraseFile(FileSystem, Path)} on this
     * {@code FileErasureSpec} a specified number of times.
     *
     * @param times the number of times to repeat this FileErasureSpec
     * @return a new FileErasureSpec repeating this one the specified number of times
     * @throws IllegalArgumentException      if the number of times to repeat this
     *                                       FileErasureSpec is not positive
     * @throws UnsupportedOperationException if this FileErasureSpec is
     *                                       {@link #isTerminal() terminal}
     */
    public final FileErasureSpec repeated(int times) throws IllegalArgumentException, UnsupportedOperationException {
        checkArgument(times > 0, "must be repeated a positive number of times");
        requireNonTerminal();
        return (times == 1) ? this : Repeated.apply(this, times);
    }

    /**
     * Returns whether or not this {@code FileErasureSpec} is terminal.
     *
     * <p>A terminal {@code FileErasureSpec} is one which modifies a file
     * in such a way that further erasure is impossible or may behave
     * inconsistently. For example, deleting the file from the file system
     * would prevent further operations on the file, and is thus terminal.
     *
     * @return whether or not this FileErasureSpec is terminal
     */
    public abstract boolean isTerminal();

    /**
     * Creates a {@code FileErasureSpec} from a specified
     * {@link OutputStreamErasureSpec}.
     *
     * @param spec the OutputStreamErasureSpec to use
     * @return a FileErasureSpec from the specified OutputStreamErasureSpec
     */
    public static FileErasureSpec from(OutputStreamErasureSpec spec) {
        return new Delegating(spec);
    }

    /**
     * A FileErasureSpec which combines two FileErasureSpecs in order.
     */
    private static final class Cons extends FileErasureSpec {
        private final FileErasureSpec first;
        private final FileErasureSpec last;

        private Cons(FileErasureSpec first, FileErasureSpec last) {
            this.first = first;
            this.last = last;
        }

        @Override
        public void eraseFile(FileSystem fs, Path path) throws IOException {
            first.eraseFile(fs, path);
            last.eraseFile(fs, path);
        }

        @Override
        public boolean isTerminal() {
            return last.isTerminal();
        }
    }

    /**
     * A FileErasureSpec which repeats a FileErasureSpec a specified number of times.
     */
    private static final class Repeated extends FileErasureSpec {
        private final FileErasureSpec spec;
        private final int times;

        private Repeated(FileErasureSpec spec, int times) {
            this.spec = spec;
            this.times = times;
        }

        @Override
        public void eraseFile(FileSystem fs, Path path) throws IOException {
            for (int i = 0; i < times; i++) {
                spec.eraseFile(fs, path);
            }
        }

        @Override
        public boolean isTerminal() {
            return false;
        }

        /** static factory */
        private static Repeated apply(FileErasureSpec spec, int times) {
            if (spec instanceof Repeated) {
                Repeated repeated = (Repeated) spec;
                return new Repeated(repeated.spec, repeated.times * times);
            } else {
                return new Repeated(spec, times);
            }
        }
    }

    /**
     * A FileErasureSpec which delegates erasure to an {@link OutputStreamErasureSpec}.
     */
    private static final class Delegating extends FileErasureSpec {
        private final OutputStreamErasureSpec spec;

        private Delegating(OutputStreamErasureSpec spec) {
            this.spec = spec;
        }

        @Override
        public void eraseFile(final FileSystem fs, final Path path) throws IOException {
            FileStatus fileStatus = fs.getFileStatus(path);
            final long length = fileStatus.getLen();
            int blockSize = intBlockSize(fileStatus.getBlockSize());

            spec.erase(new SizedOutputStreamProvider() {
                @Override
                public SizedOutputStream get() throws IOException {
                    return new SizedOutputStream(fs.create(path), length);
                }
            }, blockSize);
        }

        @Override
        public boolean isTerminal() {
            return spec.isTerminal();
        }

        /**
         * Returns the int value of a long block size, or {@link Integer#MAX_VALUE} if
         * the block size does not fit in an int (unlikely).
         *
         * @param blockSize the long block size
         * @return the block size as an int
         */
        private static int intBlockSize(long blockSize) {
            while (blockSize > MAX_INT) {
                if ((blockSize & 1) == 1) {
                    return -1;
                }
                blockSize >>= 1;
            }
            return Ints.checkedCast(blockSize); // should never fail after of loop
        }
    }
}
