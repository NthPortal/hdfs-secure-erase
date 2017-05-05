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
     * Because {@link Configured} calls
     * {@link Configured#setConf(Configuration) setConf(Configuration)} from
     * inside its constructor, it will call an inherited {@code setConf(Configuration)}
     * method before the inheriting object is initialized. Because this object also
     * inherits from {@code Configured}, the value of this variable will not be
     * initialized to {@code true} until after {@code Configured}'s constructor
     * returns. Thus, this (not-actually-)final variable can be used as a safety check
     * before accessing possibly uninitialized fields.
     *
     * <p>...don't call methods which can be overridden from a constructor.
     */
    protected final boolean initialized;

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
        initialized = true;
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
