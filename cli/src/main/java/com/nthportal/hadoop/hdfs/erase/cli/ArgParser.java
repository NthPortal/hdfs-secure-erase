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
        Option opt;

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

        opt = new Option(
                Opts.ITERATIONS_SHORT,
                Opts.ITERATIONS,
                true,
                "the number of times to overwrite the file(s) with random data "
                        + "(default " + OptionProcessor.DEFAULT_ITERATIONS + ")");
        opt.setArgName(Names.ITERATIONS_ARG);
        options.addOption(opt);

        opt = new Option(
                Opts.BYTE_PATTERNS_SHORT,
                Opts.BYTE_PATTERNS,
                true,
                "a comma-separated sequence of byte patterns (represented in hex)");
        opt.setArgName(Names.PATTERNS_ARG);
        options.addOption(opt);

        options.addOption(new Option(
                Opts.REMOVE_SHORT,
                Opts.REMOVE,
                false,
                "truncate and remove the file(s) after overwriting"));

        opt = new Option(
                Opts.SPEC_SHORT,
                Opts.SPEC,
                true,
                "the name of a `" + FileErasureSpec.class.getSimpleName() + "` "
                        + "to use (must have a default constructor");
        opt.setArgName(Names.CLASS_ARG);
        options.addOption(opt);

        opt = new Option(
                Opts.PROVIDER_SHORT,
                Opts.PROVIDER,
                true,
                "the name of a `" + FileErasureSpecProvider.class.getSimpleName()+ "` "
                        + "to use (must have a default constructor");
        opt.setArgName(Names.CLASS_ARG);
        options.addOption(opt);

        options.addOption(new Option(
                Opts.VERBOSE_SHORT,
                Opts.VERBOSE,
                false,
                "show verbose erasure information"));

        return options;
    }

    /**
     * Prints a help message to stdout.
     */
    static void printHelp() {
        printVersion();
        new HelpFormatter().printHelp(Names.COMMAND_NAME + " [OPTIONS] FILE [FILES...]", Holder.options);
    }

    /**
     * Prints version information to stdout.
     */
    static void printVersion() {
        String version = ArgParser.class.getPackage().getImplementationVersion();
        System.out.println(Names.COMMAND_NAME + " version " + ((version != null) ? version : "UNKNOWN"));
        System.out.println();
    }

    /**
     * Command-line options
     */
    static final class Opts {
        private Opts() {}

        static String HELP_SHORT = "h";
        static String HELP = "help";
        static String VERSION_SHORT = "V";
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
        static String VERBOSE_SHORT = "v";
        static String VERBOSE = "verbose";
    }

    private static class Names {
        private static String COMMAND_NAME = "hdfs-secure-erase";

        private static String ITERATIONS_ARG = "COUNT";
        private static String PATTERNS_ARG = "PATTERNS";
        private static String CLASS_ARG = "CLASS";
    }
}
