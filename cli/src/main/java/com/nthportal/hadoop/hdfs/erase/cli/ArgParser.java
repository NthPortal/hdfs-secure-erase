package com.nthportal.hadoop.hdfs.erase.cli;

import com.nthportal.hadoop.hdfs.erase.core.FileErasureSpec;
import org.apache.commons.cli.*;

final class ArgParser {
    private ArgParser() {}

    private static class Holder {
        private static final Options options = makeOptions();
    }

    static CommandLine parse(String[] args) throws ParseException {
        return new GnuParser().parse(Holder.options, args);
    }

    private static Options makeOptions() {
        Options options = new Options();

        options.addOption(new Option(
                Opts.HELP_SHORT,
                Opts.HELP,
                false,
                "shows this help message"));

        options.addOption(new Option(
                Opts.VERSION_SHORT,
                Opts.VERSION,
                false,
                "shows version information"));

        options.addOption(new Option(
                Opts.ITERATIONS_SHORT,
                Opts.ITERATIONS,
                true,
                "the number of times to overwrite the file(s) with random data "
                        + "(default " + OptionHandler.DEFAULT_ITERATIONS + ")"));

        options.addOption(new Option(
                Opts.BYTE_PATTERNS_SHORT,
                Opts.BYTE_PATTERNS,
                true,
                "a comma-separated sequence of byte patterns (represented in hex)"));

        options.addOption(new Option(
                Opts.REMOVE_SHORT,
                Opts.REMOVE,
                false,
                "truncate and remove the file(s) after overwriting"));

        options.addOption(new Option(
                Opts.SPEC_SHORT,
                Opts.SPEC,
                true,
                "the name of a `" + FileErasureSpec.class.getSimpleName() + "` "
                        + "to use (must have a default constructor"));

        options.addOption(new Option(
                Opts.PROVIDER_SHORT,
                Opts.PROVIDER,
                true,
                "the name of a `" + FileErasureSpecProvider.class.getSimpleName()+ "` "
                        + "to use (must have a default constructor"));

        return options;
    }

    static void printHelp() {
        new HelpFormatter().printHelp("", Holder.options);
    }

    static void printVersion() {
        // TODO: 4/20/17 impl
    }

    static class Opts {
        static String HELP_SHORT = "h";
        static String HELP = "help";
        static String VERSION_SHORT = "v";
        static String VERSION = "version";
        static String ITERATIONS_SHORT = "n";
        static String ITERATIONS = "iterations";
        static String BYTE_PATTERNS_SHORT = "b";
        static String BYTE_PATTERNS = "byte-patterns";
        static String REMOVE_SHORT = "u";
        static String REMOVE = "remove";
        static String SPEC_SHORT = "s";
        static String SPEC = "erasure-spec";
        static String PROVIDER_SHORT = "p";
        static String PROVIDER = "erasure-spec-provider";
    }
}
