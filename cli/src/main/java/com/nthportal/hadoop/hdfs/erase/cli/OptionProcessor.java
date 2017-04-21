package com.nthportal.hadoop.hdfs.erase.cli;

import com.nthportal.hadoop.hdfs.erase.cli.ArgParser.Opts;
import com.nthportal.hadoop.hdfs.erase.core.FileErasureSpec;
import com.nthportal.hadoop.hdfs.erase.core.SecureErase;
import com.nthportal.hadoop.hdfs.erase.core.specs.AdvancedFileDeletionSpec;
import com.nthportal.hadoop.hdfs.erase.core.specs.ByteProviders;
import com.nthportal.hadoop.hdfs.erase.core.specs.OverwriteSpec;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;

/**
 * Processes command-line options and erases files based on
 * the specified options.
 */
final class OptionProcessor {
    static final int DEFAULT_ITERATIONS = 3;

    private final CommandLine cmd;

    /**
     * Creates an {@code ErasureOptionHandler} from the specified
     * {@link CommandLine}.
     *
     * @param cmd the CommandLine with which to process options
     */
    OptionProcessor(CommandLine cmd) {
        this.cmd = cmd;
    }

    /**
     * Alias for {@link CommandLine#hasOption(String)}.
     */
    private boolean has(String opt) {
        return cmd.hasOption(opt);
    }

    void processOptions() throws CliOptionException, IOException {
        if (!handleHelpAndVersion()) {
            checkForEmptyFilesList();
            checkForMutuallyExclusiveOptions();
            handleRemainingOptions();
        }
    }

    /**
     * Handles help and version options if present.
     *
     * @return true if help or version options were present; false otherwise
     */
    private boolean handleHelpAndVersion() {
        if (has(Opts.HELP)) {
            ArgParser.printHelp();
            return true;
        } else if (has(Opts.VERSION)) {
            ArgParser.printVersion();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Checks if no files to erase were specified.
     *
     * @throws CliOptionException if no files to erase were specified
     */
    private void checkForEmptyFilesList() throws CliOptionException {
        if (cmd.getArgs().length == 0) {
            throw new CliOptionException("No files specified");
        }
    }

    /**
     * Checks for mutually exclusive options.
     *
     * @throws CliOptionException if mutually exclusive options were used
     */
    private void checkForMutuallyExclusiveOptions() throws CliOptionException {
        checkForMutuallyExclusiveOptions(Opts.ITERATIONS, Opts.BYTE_PATTERNS);
        checkForMutuallyExclusiveOptions(Opts.ITERATIONS, Opts.SPEC);
        checkForMutuallyExclusiveOptions(Opts.ITERATIONS, Opts.PROVIDER);
        checkForMutuallyExclusiveOptions(Opts.BYTE_PATTERNS, Opts.SPEC);
        checkForMutuallyExclusiveOptions(Opts.BYTE_PATTERNS, Opts.PROVIDER);
        checkForMutuallyExclusiveOptions(Opts.REMOVE, Opts.SPEC);
        checkForMutuallyExclusiveOptions(Opts.REMOVE, Opts.PROVIDER);
        checkForMutuallyExclusiveOptions(Opts.SPEC, Opts.PROVIDER);
    }

    /**
     * Checks if two mutually exclusive are present.
     *
     * @param opt1 the first of the mutually exclusive options
     * @param opt2 the second of the mutually exclusive options
     * @throws CliOptionException if both options are present
     */
    private void checkForMutuallyExclusiveOptions(String opt1, String opt2) throws CliOptionException {
        if (has(opt1) && has(opt2)) {
            throw new CliOptionException("Mutually exclusive options `" + opt1 + "` and `" + opt2 + "`");
        }
    }

    /**
     * Handles all options other than help and version options.
     *
     * @throws CliOptionException if one or more options are invalid
     * @throws IOException if an error occurred while erasing files
     */
    private void handleRemainingOptions() throws CliOptionException, IOException {
        if (!has(Opts.ITERATIONS)
                && !has(Opts.BYTE_PATTERNS)
                && !has(Opts.REMOVE)
                && !has(Opts.SPEC)
                && !has(Opts.PROVIDER)) {
            defaultEraseFiles();
        } else if (has(Opts.ITERATIONS)) {
            String iterationsStr = cmd.getOptionValue(Opts.ITERATIONS);
            try {
                int iterations = Integer.parseInt(iterationsStr);
                FileErasureSpec spec = FileErasureSpec.from(new OverwriteSpec(ByteProviders.randomBytes()).repeated(iterations));
                eraseAndPossiblyRemove(spec);
            } catch (NumberFormatException e) {
                throw new CliOptionException("Invalid iteration count: " + iterationsStr, e);
            }
        } else if (has(Opts.BYTE_PATTERNS)) {
            FileErasureSpec spec = parseBytePatterns();
            eraseAndPossiblyRemove(spec);
        } else if (has(Opts.SPEC)) {
            eraseFiles((FileErasureSpec) ReflectionHelper.instantiate(cmd.getOptionValue(Opts.SPEC)));
        } else if (has(Opts.PROVIDER)) {
            eraseFiles(((FileErasureSpecProvider) ReflectionHelper.instantiate(cmd.getOptionValue(Opts.PROVIDER))).get());
        }
    }

    /**
     * Perform the default erasure.
     *
     * @throws IOException if an error occurred while erasing files
     */
    private void defaultEraseFiles() throws IOException {
        eraseWithRemoval(FileErasureSpec.from(new OverwriteSpec(ByteProviders.randomBytes()).repeated(DEFAULT_ITERATIONS)));
    }

    /**
     * Parses list of byte patterns provided as the argument to the
     * {@code byte-patterns} option.
     *
     * @return A {@link FileErasureSpec} which erases according to the
     * specified byte patterns
     * @throws CliOptionException if one or more of the byte patterns
     * are invalid
     */
    private FileErasureSpec parseBytePatterns() throws CliOptionException {
        String patternsStr = cmd.getOptionValue(Opts.BYTE_PATTERNS);
        String[] patterns = patternsStr.split(",");
        FileErasureSpec spec = null;
        boolean first = true;

        try {
            for (String pattern : patterns) {
                if (first) {
                    spec = parseBytePattern(pattern);
                    first = false;
                } else {
                    spec = spec.andThen(parseBytePattern(pattern));
                }
            }

            return spec;
        } catch (CliOptionException e) {
            throw new CliOptionException("Invalid byte patterns string: " + patternsStr, e);
        }
    }

    /**
     * Parses a byte pattern from a string.
     *
     * @param pattern the pattern to parse
     * @return a {@link FileErasureSpec} which erases according to the
     * specified byte pattern
     * @throws CliOptionException if the byte pattern is invalid
     */
    private static FileErasureSpec parseBytePattern(String pattern) throws CliOptionException {
        return FileErasureSpec.from(new OverwriteSpec(ByteProviders.repeatedBytes(bytesFromString(pattern))));
    }

    /**
     * Parses a byte array from a hex string.
     *
     * @param hex the hex string to parse
     * @return the byte array represented by the hex string
     * @throws CliOptionException if the byte string is invalid
     */
    private static byte[] bytesFromString(String hex) throws CliOptionException {
        if (hex.length() == 0) {
            throw new CliOptionException("Byte pattern cannot be empty");
        }

        try {
            return Hex.decodeHex(hex.toCharArray());
        } catch (DecoderException e) {
            throw new CliOptionException("Invalid byte pattern: " + hex, e);
        }
    }

    /**
     * Erases files according to the specified {@link FileErasureSpec},
     * and deletes the files afterward if the option to do so was specified.
     *
     * @param spec the specification with which to erase the files
     * @throws IOException if an error occurred while erasing files
     */
    private void eraseAndPossiblyRemove(FileErasureSpec spec) throws IOException {
        if (has(Opts.REMOVE)) {
            eraseWithRemoval(spec);
        } else {
            eraseFiles(spec);
        }
    }

    /**
     * Erases files according to the specified {@link FileErasureSpec},
     * and deletes the files afterward.
     *
     * @param spec the specification with which to erase the files
     * @throws IOException if an error occurred while erasing files
     */
    private void eraseWithRemoval(FileErasureSpec spec) throws IOException {
        eraseFiles(spec.andThen(new AdvancedFileDeletionSpec(AdvancedFileDeletionSpec.Conf.defaultConf())));
    }

    /**
     * Erases files according to the specified {@link FileErasureSpec}.
     *
     * @param spec the specification with which to erase the files
     * @throws IOException if an error occurred while erasing files
     */
    private void eraseFiles(FileErasureSpec spec) throws IOException {
        // TODO: 4/20/17 maybe handle globbing?
        for (String arg : cmd.getArgs()) {
            SecureErase.eraseFile(arg, spec);
        }
    }
}
