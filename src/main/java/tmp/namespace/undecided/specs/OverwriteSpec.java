package tmp.namespace.undecided.specs;

import com.google.common.primitives.Ints;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tmp.namespace.undecided.ErasureSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

/**
 * An {@link ErasureSpec} which overwrites a file (once).
 */
public final class OverwriteSpec extends ErasureSpec {
    private static final Set<ByteProvider.Reusability> exactSizeReusable =
            EnumSet.of(ByteProvider.Reusability.EXACT, ByteProvider.Reusability.PREFIX_UNLIMITED);
    private static final byte[] emptyByteArray = new byte[0];

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
    public void eraseFile(FileSystem fs, Path path) throws IOException {
        FileStatus fileStatus = fs.getFileStatus(path);

        // Calculate byte counts to write
        long length = fileStatus.getLen();
        int blockSize = intBlockSize(fileStatus.getBlockSize());

        long fullBlocks = length / blockSize;
        int remainder = Ints.checkedCast(length % blockSize); // should never fail, because blockSize is an int

        // Write bytes
        try (FSDataOutputStream outputStream = fs.create(path)) {
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
     * Returns the int value of a long block size, or {@link Integer#MAX_VALUE} if
     * the block size does not fit in an int (unlikely).
     *
     * @param blockSize the long block size
     * @return the block size as an int
     */
    private static int intBlockSize(long blockSize) {
        try {
            return Ints.checkedCast(blockSize);
        } catch (IllegalArgumentException ignored) {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * Writes bytes to an {@link FSDataOutputStream output stream}.
     */
    private class ByteWriter {
        private final FSDataOutputStream outputStream;
        private ByteProvider.Reusability reusability = ByteProvider.Reusability.NONE;
        private ByteProvider.State state = ByteProvider.State.empty();
        private byte[] bytes = emptyByteArray;

        private ByteWriter(FSDataOutputStream outputStream) {this.outputStream = outputStream;}

        /**
         * Write the specified number of bytes to the specified
         * {@link FSDataOutputStream output stream}, using the {@link #byteProvider}
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
