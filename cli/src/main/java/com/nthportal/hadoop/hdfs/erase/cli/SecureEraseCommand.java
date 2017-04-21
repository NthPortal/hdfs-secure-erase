package com.nthportal.hadoop.hdfs.erase.cli;

/**
 * Entry point for secure erase command line interface.
 */
public final class SecureEraseCommand {
    public static void main(String[] args) throws Exception {
        new OptionProcessor(ArgParser.parse(args)).processOptions();
    }
}
