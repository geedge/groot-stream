package com.geedgenetworks.bootstrap.utils;

import com.geedgenetworks.bootstrap.command.CommandArgs;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import lombok.NonNull;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {
    private FileUtils() {
        throw new UnsupportedOperationException("FileUtils is a utility class and cannot be instantiated");
    }
    public static Path getConfigPath(@NonNull CommandArgs args) {
        switch (args.getDeployMode()) {
            case RUN:
            case CLIENT:
                return Paths.get(args.getConfigFile());
            case RUN_APPLICATION:
                return Paths.get(getFileName(args.getConfigFile()));
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

    private static String getFileName(@NonNull String filePath) {
        return filePath.substring(filePath.lastIndexOf(File.separatorChar) + 1);
    }

    public static String getFileExtension(@NonNull String fullName) {
        String fileName = new File(fullName).getName();
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? "" : fileName.substring(dotIndex + 1);
    }

}
