package com.geedgenetworks.bootstrap.utils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.geedgenetworks.bootstrap.command.CommandArgs;
import com.geedgenetworks.bootstrap.command.UsageFormatter;
import com.geedgenetworks.common.Constants;

public final class CommandLineUtils {
    private CommandLineUtils() {
        throw new UnsupportedOperationException(
                "CommandLineUtils is a utility class and cannot be instantiated");
    }

    public static <T extends CommandArgs> T parse(String[] args, T obj) {
        return parse(args, obj, null, false);
    }

    public static <T extends CommandArgs> T parse(
            String[] args, T obj, String programName, boolean acceptUnknownOptions) {
        JCommander jCommander =
                JCommander.newBuilder()
                        .programName(programName)
                        .addObject(obj)
                        .acceptUnknownOptions(acceptUnknownOptions)
                        .build();
        try {
            jCommander.parse(args);
            obj.setOriginalParameters(jCommander.getUnknownOptions());
        } catch (ParameterException e) {
            System.err.println(e.getLocalizedMessage());
            exit(jCommander);
        }

        if (obj.isHelp()) {
            exit(jCommander);
        }
        return obj;
    }

    private static void exit(JCommander jCommander) {
        jCommander.setUsageFormatter(new UsageFormatter(jCommander));
        jCommander.usage();
        System.exit(Constants.SYSTEM_EXIT_CODE);
    }


}

