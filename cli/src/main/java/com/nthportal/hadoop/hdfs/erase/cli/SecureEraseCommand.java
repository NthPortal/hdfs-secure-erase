package com.nthportal.hadoop.hdfs.erase.cli;

public final class SecureEraseCommand {
    public static void main(String[] args) throws Exception {
        new OptionHandler(ArgParser.parse(args)).handleOptions();
    }
}
