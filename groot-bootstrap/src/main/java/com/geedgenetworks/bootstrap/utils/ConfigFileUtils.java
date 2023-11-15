package com.geedgenetworks.bootstrap.utils;

import com.geedgenetworks.bootstrap.command.CommandArgs;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.utils.FileUtils;
import lombok.NonNull;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ConfigFileUtils {
    private ConfigFileUtils() {
        throw new UnsupportedOperationException("FileUtils is a utility class and cannot be instantiated");
    }
    public static Path getConfigPath(@NonNull CommandArgs args) {
        switch (args.getDeployMode()) {
            case RUN:
            case CLIENT:
                return Paths.get(args.getConfigFile());
            case RUN_APPLICATION:
                return Paths.get(FileUtils.getFileName(args.getConfigFile()));
            default:
                throw new IllegalArgumentException(
                        "Unsupported deploy mode: " + args.getDeployMode());
        }
    }

    public static void checkConfigExist(Path configFile) {
        if (!configFile.toFile().exists()) {
            String message = "Can't find config file: " + configFile;
            throw new GrootStreamRuntimeException(CommonErrorCode.FILE_OPERATION_ERROR, message);
        }
    }





}
