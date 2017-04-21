package com.nthportal.hadoop.hdfs.erase.core.specs;

import com.google.common.primitives.Ints;
import com.nthportal.hadoop.hdfs.erase.core.OutputStreamErasureSpec;
import com.nthportal.hadoop.hdfs.erase.core.SizedOutputStream;
import com.nthportal.hadoop.hdfs.erase.core.SizedOutputStreamProvider;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

/**
 * An {@link OutputStreamErasureSpec} which overwrites a region (once).
 */
public final class OverwriteSpec extends OutputStreamErasureSpec {
    private static final int DEFAULT_BLOCK_SIZE = 512;
    private static final Set<ByteProvider.Reusability> exactSizeReusable =
            EnumSet.of(ByteProvider.Reusability.EXACT, ByteProvider.Reusability.PREFIX_UNLIMITED);
    private static final byte[] emptyByteArray = new byte[0];
    private static final Logger logger = Logger.getLogger(OverwriteSpec.class);

    private final ByteProvider byteProvider;

    /**
     * Creates an OverwriteSpec from the specified {@link ByteProvider}.
     *
     * @param byteProvider the {@link ByteProvider provider} to use to
     *                     generate bytes with which to overwrite files
     */
    public OverwriteSpec(ByteProvider byteProvider) {
        this.byteProvider = byteProvider;
    }

    @Override
    public void erase(SizedOutputStreamProvider provider, int blockSizeIfKnown) throws IOException {
        if (isLoggingEnabled()) {
            logger.info("Overwriting with " + byteProvider.description());
        }

        int blockSize = (blockSizeIfKnown > 0) ? blockSizeIfKnown : DEFAULT_BLOCK_SIZE;

        try (SizedOutputStream outputStream = provider.get()) {
            long size = outputStream.size();
            long fullBlocks = size / blockSizeIfKnown;
            int remainder = Ints.checkedCast(size % blockSize); // should never fail, because blockSize is an int

            ByteWriter writer = new ByteWriter(outputStream);
            for (long i = 0; i < fullBlocks; i++) {
                writer.writeBytes(blockSize);
            }
            if (remainder != 0) {
                writer.writeBytes(remainder);
            }

            outputStream.flush();
        }
    }

    @Override
    public final boolean isTerminal() {
        return false;
    }

    /**
     * Writes bytes to an {@link OutputStream output stream}.
     */
    private class ByteWriter {
        private final OutputStream outputStream;
        private ByteProvider.Reusability reusability = ByteProvider.Reusability.NONE;
        private ByteProvider.State state = ByteProvider.State.empty();
        private byte[] bytes = emptyByteArray;

        private ByteWriter(OutputStream outputStream) {this.outputStream = outputStream;}

        /**
         * Write the specified number of bytes to the specified
         * {@link OutputStream output stream}, using the {@link #byteProvider}
         * (from the enclosing {@link OverwriteSpec}.
         *
         * @param count        the number of bytes to write
         * @throws IOException if an I/O error occurs while writing to the output stream
         */
        void writeBytes(int count) throws IOException {
            if (count < bytes.length && reusability == ByteProvider.Reusability.PREFIX_UNLIMITED) {
                bytes = Arrays.copyOfRange(bytes, 0, count);
            } else if (count != bytes.length || !exactSizeReusable.contains(reusability)) {
                bytes = new byte[count];
                reusability = byteProvider.nextBytes(bytes, state);
            }

            outputStream.write(bytes);
        }
    }
}
