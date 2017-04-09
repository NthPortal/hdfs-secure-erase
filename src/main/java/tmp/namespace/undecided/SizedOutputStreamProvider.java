package tmp.namespace.undecided;

import java.io.IOException;

/**
 * Provides a {@link SizedOutputStream}.
 */
public interface SizedOutputStreamProvider {
    /**
     * Returns a {@link SizedOutputStream}.
     *
     * @return a SizedOutputStream
     * @throws IOException if an I/O error occurs while generating the OutputStream
     */
    SizedOutputStream get() throws IOException;
}
