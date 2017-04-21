package com.nthportal.hadoop.hdfs.erase.cli;

import com.nthportal.hadoop.hdfs.erase.core.FileErasureSpec;
import org.apache.commons.cli.*;

/**
 * Utility class for managing command-line options.
 */
final class ArgParser {
    private ArgParser() {}

    /**
     * Lazy initialization for options.
     */
    private static class Holder {
        private static final Options options = makeOptions();
    }

    /**
     * Parse the specified command line arguments.
     *
     * @param args the arguments to parse
     * @return a {@link CommandLine} containing the parsed arguments
     * @throws ParseException if an error occurs while parsing the arguments
     */
    static CommandLine parse(String[] args) throws ParseException {
        return new GnuParser().parse(Holder.options, args);
    }

    /**
     * Returns an {@link Options} object with which to parse
     * command-line arguments.
     *
     * @return an Options object with which to parse command-line
     * arguments
     */
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
                        + "(default " + OptionProcessor.DEFAULT_ITERATIONS + ")"));

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

    /**
     * Prints a help message to stdout.
     */
    static void printHelp() {
        printVersion();
        new HelpFormatter().printHelp("", Holder.options, true);
    }

    /**
     * Prints version information to stdout.
     */
    static void printVersion() {
        String version = ArgParser.class.getPackage().getImplementationVersion();
        System.out.println("hdfs-secure-erase version " + ((version != null) ? version : "UNKNOWN"));
    }

    /**
     * Command-line options
     */
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
