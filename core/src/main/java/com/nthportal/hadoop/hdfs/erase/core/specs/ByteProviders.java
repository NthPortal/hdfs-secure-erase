package com.nthportal.hadoop.hdfs.erase.core.specs;

import com.google.common.base.Preconditions;

import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Various pre-implemented {@link ByteProvider}s.
 *
 * NOTE: This class will be deprecated upon updating to Java 8.
 */
public final class ByteProviders {
    private ByteProviders() {}

    /**
     * Returns a ByteProvider which generates random bytes using a {@link SecureRandom}.
     *
     * @return a ByteProvider which generates random bytes using a SecureRandom
     */
    public static ByteProvider randomBytes() {
        return randomBytes(new SecureRandom());
    }

    /**
     * Returns a ByteProvider which generates random bytes using
     * the specified {@link SecureRandom}.
     *
     * @param random the SecureRandom to use to generate bytes
     * @return a ByteProvider which generates random bytes using
     * the specified SecureRandom
     * @throws NullPointerException if the SecureRandom is null
     */
    public static ByteProvider randomBytes(final SecureRandom random) throws NullPointerException {
        Preconditions.checkNotNull(random);

        return new ByteProvider() {
            @Override
            public Reusability nextBytes(byte[] bytes, State state) {
                random.nextBytes(bytes);
                return Reusability.NONE;
            }
        };
    }

    /**
     * Returns a ByteProvider which provides {@code 0}-value bytes (bytes with the value {@code 0}).
     *
     * @return a ByteProvider which provides 0-value bytes
     */
    public static ByteProvider zeros() {
        return new ByteProvider() {
            @Override
            public Reusability nextBytes(byte[] bytes, State state) {
                // Abuse the fact that `bytes` is specified to be filled with zeros
                return Reusability.PREFIX_UNLIMITED;
            }
        };
    }

    /**
     * Returns a ByteProvider which provides bytes with a specified value.
     *
     * @param b the byte value to provide
     * @return a ByteProvider which provides bytes with the specified value
     */
    public static ByteProvider repeatedBytes(final byte b) {
        if (b == 0) {
            return zeros();
        } else {
            return new ByteProvider() {
                @Override
                public Reusability nextBytes(byte[] bytes, State state) {
                    Arrays.fill(bytes, b);
                    return Reusability.PREFIX_UNLIMITED;
                }
            };
        }
    }

    /**
     * Returns a ByteProvider which provides a repeated pattern of bytes.
     *
     * @param toRepeat the bytes to repeat
     * @return a ByteProvider which provides a repeated pattern of bytes
     * @throws NullPointerException if the byte array is null
     * @throws IllegalArgumentException if the byte array is empty
     */
    public static ByteProvider repeatedBytes(final byte[] toRepeat)
            throws NullPointerException, IllegalArgumentException {
        Preconditions.checkNotNull(toRepeat);
        Preconditions.checkArgument(toRepeat.length > 0, "Byte array must be non-empty");

        return new ByteProvider() {
            @Override
            public Reusability nextBytes(byte[] bytes, State state) {
                // Handle offset from previous operation
                int offsetIndex = state.getInt();
                int initialSrcLen = toRepeat.length - offsetIndex;

                // Bytes to provide are less than the remaining bytes in toRepeat
                if (bytes.length < initialSrcLen) {
                    System.arraycopy(toRepeat, offsetIndex, bytes, 0, bytes.length);
                    state.setInt(offsetIndex + bytes.length);
                    return Reusability.NONE;
                }

                // Copy remnant of toRepeat from previous operation
                System.arraycopy(toRepeat, offsetIndex, bytes, 0, initialSrcLen);

                int effectiveDestLen = bytes.length - offsetIndex;

                // Repeat toRepeat as needed to fill the rest of bytes
                if (effectiveDestLen < toRepeat.length) {
                    System.arraycopy(toRepeat, 0, bytes, offsetIndex, effectiveDestLen);
                    state.setInt(effectiveDestLen);
                    return (effectiveDestLen == offsetIndex) ? Reusability.EXACT : Reusability.NONE;
                } else {
                    System.arraycopy(toRepeat, 0, bytes, offsetIndex, toRepeat.length);

                    int currentLength = toRepeat.length;
                    while (currentLength < effectiveDestLen) {
                        System.arraycopy(bytes, offsetIndex, bytes, offsetIndex + currentLength, Math.min(currentLength, effectiveDestLen - currentLength));
                        currentLength *= 2;
                    }

                    int newIndex = effectiveDestLen % toRepeat.length;
                    state.setInt(newIndex);
                    return (newIndex == offsetIndex) ? Reusability.EXACT : Reusability.NONE;
                }
            }
        };
    }
}
