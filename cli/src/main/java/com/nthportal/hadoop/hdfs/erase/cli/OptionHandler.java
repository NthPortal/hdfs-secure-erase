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

final class OptionHandler {
    static final int DEFAULT_ITERATIONS = 3;

    private final CommandLine cmd;

    OptionHandler(CommandLine cmd) {
        this.cmd = cmd;
    }

    private boolean has(String opt) {
        return cmd.hasOption(opt);
    }

    void handleOptions() throws CliOptionException, IOException {
        if (!handleHelpAndVersion()) {
            checkForMutuallyExclusiveOptions();
            handleRemainingOptions();
        }
    }

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

    private void checkForMutuallyExclusiveOptions(String opt1, String opt2) throws CliOptionException {
        if (has(opt1) && has(opt2)) {
            throw new CliOptionException("Mutually exclusive options `" + opt1 + "` and `" + opt2 + "`");
        }
    }

    private void handleRemainingOptions() throws CliOptionException, IOException {
        if (!has(Opts.ITERATIONS)
                && !has(Opts.BYTE_PATTERNS)
                && !has(Opts.REMOVE)
                && !has(Opts.SPEC)
                && !has(Opts.PROVIDER)) {
            eraseWithRemoval(FileErasureSpec.from(new OverwriteSpec(ByteProviders.randomBytes()).repeated(DEFAULT_ITERATIONS)));
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
            FileErasureSpec spec = parseBytePattern();
            eraseAndPossiblyRemove(spec);
        } else if (has(Opts.SPEC)) {
            eraseFiles((FileErasureSpec) ReflectionHelper.instantiate(cmd.getOptionValue(Opts.SPEC)));
        } else if (has(Opts.PROVIDER)) {
            eraseFiles(((FileErasureSpecProvider) ReflectionHelper.instantiate(cmd.getOptionValue(Opts.PROVIDER))).get());
        }
    }

    private FileErasureSpec parseBytePattern() throws CliOptionException {
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

    private static FileErasureSpec parseBytePattern(String pattern) throws CliOptionException {
        return FileErasureSpec.from(new OverwriteSpec(ByteProviders.repeatedBytes(bytesFromString(pattern))));
    }

    private static byte[] bytesFromString(String s) throws CliOptionException {
        if (s.length() == 0) {
            throw new CliOptionException("Byte pattern cannot be empty");
        }

        try {
            return Hex.decodeHex(s.toCharArray());
        } catch (DecoderException e) {
            throw new CliOptionException("Invalid byte pattern: " + s, e);
        }
    }

    private void eraseAndPossiblyRemove(FileErasureSpec spec) throws IOException {
        if (has(Opts.REMOVE)) {
            eraseWithRemoval(spec);
        } else {
            eraseFiles(spec);
        }
    }

    private void eraseWithRemoval(FileErasureSpec spec) throws IOException {
        eraseFiles(spec.andThen(new AdvancedFileDeletionSpec(AdvancedFileDeletionSpec.Conf.defaultConf())));
    }

    private void eraseFiles(FileErasureSpec spec) throws IOException {
        // TODO: 4/20/17 maybe handle globbing?
        for (String arg : cmd.getArgs()) {
            // TODO: 4/20/17 maybe handle errors?
            SecureErase.eraseFile(arg, spec);
        }
    }
}
