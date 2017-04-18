package com.nthportal.hadoop.hdfs.erase.core;

/**
 * A specification for erasing something.
 */
abstract class ErasureSpec {
    /**
     * Returns whether or not this {@code ErasureSpec} is terminal.
     *
     * <p>A terminal {@code ErasureSpec} is one which performs some action
     * such that further erasure is impossible or may behave inconsistently.
     *
     * @return whether or not this ErasureSpec is terminal
     */
    public abstract boolean isTerminal();

    /**
     * Requires that this {@code ErasureSpec} is not terminal.
     *
     * @throws UnsupportedOperationException if this ErasureSpec is terminal
     * @see #isTerminal()
     */
    protected final void requireNonTerminal() throws UnsupportedOperationException {
        if (isTerminal()) {
            throw new UnsupportedOperationException("A terminal ErasureSpec cannot be followed by other erasures");
        }
    }
}