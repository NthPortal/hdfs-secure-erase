package tmp.namespace.undecided;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link OutputStream} which only allows at most a certain number
 * of bytes to be written.
 */
@NotThreadSafe
public class SizedOutputStream extends OutputStream {
    private final OutputStream underlying;
    private final long size;
    private long sizeRemaining;

    /**
     * Creates a {@code SizedOutputStream} from an {@code OutputStream} and
     * a size.
     *
     * @param underlying the underlying OutputStream
     * @param size the writable size of the OutputStream
     */
    public SizedOutputStream(OutputStream underlying, long size) {
        this.underlying = underlying;
        this.size = size;
        sizeRemaining = size;
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

    private void checkRemainingSize(int countToWrite) throws IllegalStateException {
        if (countToWrite > sizeRemaining) {
            throw new IllegalStateException("Insufficient remaining size to write " + countToWrite + " bytes");
        }
        sizeRemaining -= countToWrite;
    }
}
