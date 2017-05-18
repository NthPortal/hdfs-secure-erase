package com.nthportal.hadoop.hdfs.erase.core;

import org.apache.hadoop.conf.Configuration;

/**
 * A specification for erasing something.
 */
abstract class ErasureSpec extends NonNullConfigured {
    private boolean loggingEnabled = false;

    /**
     * Returns whether or not this {@code ErasureSpec} is terminal.
     *
     * <p>A terminal {@code ErasureSpec} is one which performs some action
     * such that further erasure is impossible or may behave inconsistently.
     *
     * @return whether or not this ErasureSpec is terminal
     */
    public abstract boolean isTerminal();

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        loggingEnabled = conf.getBoolean(SecureErase.Conf.LOG_ACTIONS, false);
    }

    /**
     * Returns {@code true} if logging is enabled; {@code false}
     * if it is not enabled and this erasure specification should not
     * log its actions.
     *
     * @return true if logging is enabled; false otherwise
     */
    protected final boolean isLoggingEnabled() {
        return loggingEnabled;
    }

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
