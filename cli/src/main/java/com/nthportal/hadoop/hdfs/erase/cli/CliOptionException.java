package com.nthportal.hadoop.hdfs.erase.cli;

public final class CliOptionException extends Exception {
    CliOptionException(String message, Throwable cause) {
        super(message, cause);
    }

    CliOptionException(String message) {
        super(message);
    }
}
