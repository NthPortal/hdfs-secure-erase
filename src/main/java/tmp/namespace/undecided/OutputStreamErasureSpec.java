package tmp.namespace.undecided;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * A specification for erasing something which can be written to
 * by an {@link java.io.OutputStream OutputStream}.
 */
public abstract class OutputStreamErasureSpec extends ErasureSpec {
    /**
     * Erases data whose location is referenced by a given {@code OutputStream}.
     *
     * @param provider         a provider for a {@link SizedOutputStream}
     * @param blockSizeIfKnown the block size of the medium being written to; -1
     *                         if the block size is not known
     * @throws IOException if an I/O error occurs during the erasure
     */
    public abstract void erase(SizedOutputStreamProvider provider, int blockSizeIfKnown) throws IOException;

    /**
     * Returns a new {@code OutputStreamErasureSpec} which erases by executing
     * {@link #erase(SizedOutputStreamProvider, int)} on this
     * {@code OutputStreamErasureSpec} and then on the specified
     * {@code OutputStreamErasureSpec}.
     *
     * @param other the OutputStreamErasureSpec to use after this one
     * @return a new OutputStreamErasureSpec combining this and the other
     * OutputStreamErasureSpec
     * @throws NullPointerException          if `other` is null
     * @throws UnsupportedOperationException if this OutputStreamErasureSpec is
     *                                       {@link #isTerminal() terminal}
     */
    public final OutputStreamErasureSpec andThen(OutputStreamErasureSpec other) {
        Preconditions.checkNotNull(other);
        requireNonTerminal();
        return new Cons(this, other);
    }

    /**
     * Returns a new {@code OutputStreamErasureSpec} which erases files by executing
     * {@link #erase(SizedOutputStreamProvider, int)} on this
     * {@code OutputStreamErasureSpec} a specified number of times.
     *
     * @param times the number of times to repeat this OutputStreamErasureSpec
     * @return a new OutputStreamErasureSpec repeating this one the specified number
     * of times
     * @throws IllegalArgumentException      if the number of times to repeat this
     *                                       OutputStreamErasureSpec is not positive
     * @throws UnsupportedOperationException if this OutputStreamErasureSpec is
     *                                       {@link #isTerminal() terminal}
     */
    public final OutputStreamErasureSpec repeated(int times) {
        requireNonTerminal();
        return new Repeated(this, times);
    }

    /**
     * An OutputStreamErasureSpec which combines two OutputStreamErasureSpec in order.
     */
    private static class Cons extends OutputStreamErasureSpec {
        private final OutputStreamErasureSpec first;
        private final OutputStreamErasureSpec last;

        private Cons(OutputStreamErasureSpec first, OutputStreamErasureSpec last) {
            this.first = first;
            this.last = last;
        }

        @Override
        public void erase(SizedOutputStreamProvider provider, int blockSizeIfKnown) throws IOException {
            first.erase(provider, blockSizeIfKnown);
            last.erase(provider, blockSizeIfKnown);
        }

        @Override
        public boolean isTerminal() {
            return last.isTerminal();
        }
    }

    /**
     * An OutputStreamErasureSpec which repeats an OutputStreamErasureSpec
     * a specified number of times.
     */
    private static final class Repeated extends OutputStreamErasureSpec {
        private final OutputStreamErasureSpec spec;
        private final int times;

        private Repeated(OutputStreamErasureSpec spec, int times) {
            this.spec = spec;
            this.times = times;
        }

        @Override
        public void erase(SizedOutputStreamProvider provider, int blockSizeIfKnown) throws IOException {
            for (int i = 0; i < times; i++) {
                spec.erase(provider, blockSizeIfKnown);
            }
        }

        @Override
        public boolean isTerminal() {
            return false;
        }
    }
}