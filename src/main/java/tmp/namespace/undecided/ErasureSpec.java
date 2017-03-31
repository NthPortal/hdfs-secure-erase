package tmp.namespace.undecided;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A specification for erasing a file.
 */
public abstract class ErasureSpec {
    // TODO: 3/29/17 figure out method signature
    public abstract void eraseFile(FileSystem fs, Path path) throws IOException;

    /**
     * Returns a new {@code ErasureSpec} which erases files by executing
     * {@link #eraseFile(FileSystem, Path)} on this
     * {@code ErasureSpec} and then on the specified {@code ErasureSpec}.
     *
     * @param other the ErasureSpec to use after this one
     * @return a new ErasureSpec combining this and the other ErasureSpec
     * @throws NullPointerException if `other` is null
     * @throws UnsupportedOperationException if this ErasureSpec is
     *                                       {@link #isTerminal() terminal}
     */
    public final ErasureSpec andThen(ErasureSpec other) throws NullPointerException, UnsupportedOperationException {
        Preconditions.checkNotNull(other);
        requireNonTerminal();
        return new Cons(this, other);
    }

    /**
     * Returns a new {@code ErasureSpec} which erases files by executing
     * {@link #eraseFile(FileSystem, Path)} on this
     * {@code ErasureSpec} a specified number of times.
     *
     * @param times the number of times to repeat this ErasureSpec
     * @return a new ErasureSpec repeating this one the specified number of times
     * @throws IllegalArgumentException      if the number of times to repeat this
     *                                       ErasureSpec is not positive
     * @throws UnsupportedOperationException if this ErasureSpec is
     *                                       {@link #isTerminal() terminal}
     */
    public final ErasureSpec repeated(int times) throws IllegalArgumentException, UnsupportedOperationException {
        Preconditions.checkArgument(times > 0, "must be repeated a positive number of times");
        requireNonTerminal();
        return (times == 1) ? this : new Repeated(this, times);
    }

    private void requireNonTerminal() throws UnsupportedOperationException {
        if (isTerminal()) {
            throw new UnsupportedOperationException("A terminal ErasureSpec cannot be followed by other erasures");
        }
    }

    /**
     * Returns whether or not this {@code ErasureSpec} is terminal.
     *
     * <p>A terminal {@code ErasureSpec} is one which modifies a file
     * in such a way that further erasure is impossible or may behave
     * inconsistently. For example, deleting the file from the file system
     * would prevent further operations on the file, and is thus terminal.
     *
     * @return whether or not this ErasureSpec is terminal
     */
    public abstract boolean isTerminal();

    /**
     * An ErasureSpec which combines two ErasureSpecs in order.
     */
    private static final class Cons extends ErasureSpec {
        private final ErasureSpec first;
        private final ErasureSpec last;

        private Cons(ErasureSpec first, ErasureSpec last) {
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
     * An ErasureSpec which repeats another ErasureSpec a given number of times
     */
    private static final class Repeated extends ErasureSpec {
        private final ErasureSpec spec;
        private final int times;

        private Repeated(ErasureSpec spec, int times) {
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
    }
}
