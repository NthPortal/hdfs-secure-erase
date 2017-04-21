package com.nthportal.hadoop.hdfs.erase.cli;

/**
 * An exception thrown when command-line options are invalid.
 */
final class CliOptionException extends Exception {
    CliOptionException(String message, Throwable cause) {
        super(message, cause);
    }

    CliOptionException(String message) {
        super(message);
    }
}
