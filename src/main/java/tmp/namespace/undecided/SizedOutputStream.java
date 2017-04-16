package tmp.namespace.undecided;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link OutputStream} which only allows at most a certain number
 * of bytes to be written.
 */
public final class SizedOutputStream extends OutputStream {
    private final OutputStream underlying;
    private final long size;
    private final AtomicLong sizeRemaining;

    /**
     * Creates a {@code SizedOutputStream} from an {@link OutputStream} and
     * a size.
     *
     * @param underlying the underlying OutputStream
     * @param size the writable size of the OutputStream
     */
    public SizedOutputStream(OutputStream underlying, long size) {
        this.underlying = underlying;
        this.size = size;
        sizeRemaining = new AtomicLong(size);
    }

    @Override
    public void write(int b) throws IOException {
        checkRemainingSize(1);
        underlying.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        checkRemainingSize(b.length);
        underlying.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkRemainingSize(len);
        underlying.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        underlying.flush();
    }

    @Override
    public void close() throws IOException {
        underlying.close();
    }

    /**
     * The writable size of this {@code OutputStream} in bytes.
     *
     * @return the writable size of this OutputStream in bytes
     */
    public long size() {
        return size;
    }

    /**
     * Check that there is sufficient remaining size to write the specified
     * number of bytes.
     *
     * @param countToWrite the number of bytes to write
     * @throws IllegalStateException if there is not sufficient remaining size
     *                               to write the specified number of bytes
     */
    private void checkRemainingSize(long countToWrite) throws IllegalStateException {
        long remaining;
        do {
            remaining = sizeRemaining.get();
            if (countToWrite > remaining) {
                throw new IllegalStateException("Insufficient remaining size to write " + countToWrite + " bytes");
            }
        } while (sizeRemaining.compareAndSet(remaining, remaining - countToWrite));
    }
}
