package com.nthportal.hadoop.hdfs.erase.core;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * A {@link Configured} which does not allow {@code null}
 * {@link Configuration}s.
 */
class NonNullConfigured extends Configured {
    /**
     * Creates a new {@code NonNullConfigured} with an empty {@link Configuration}.
     */
    NonNullConfigured() {
        this(new Configuration());
    }

    /**
     * Creates a new {@code NonNullConfigured} with the specified {@link Configuration}.
     *
     * @param conf the Configuration to use
     * @throws NullPointerException if the specified Configuration is null
     */
    NonNullConfigured(Configuration conf) throws NullPointerException {
        super(Preconditions.checkNotNull(conf));
    }

    /**
     * Sets the {@link Configuration} to be used.
     *
     * @param conf the Configuration to use
     * @throws NullPointerException if the Configuration is null
     */
    @Override
    public void setConf(Configuration conf) throws NullPointerException {
        super.setConf(Preconditions.checkNotNull(conf));
    }
}
