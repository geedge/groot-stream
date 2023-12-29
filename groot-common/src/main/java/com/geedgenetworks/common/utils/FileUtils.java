package com.geedgenetworks.common.utils;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtils {

    public static List<URL> searchJarFiles(@NonNull Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory, FileVisitOption.FOLLOW_LINKS)) {
            return paths.filter(path -> path.toString().endsWith(".jar"))
                    .map(
                            path -> {
                                try {
                                    return path.toUri().toURL();
                                } catch (MalformedURLException e) {
                                    throw new GrootStreamRuntimeException(
                                            CommonErrorCode
                                                    .UNSUPPORTED_OPERATION,
                                            e);
                                }
                            })
                    .collect(Collectors.toList());
        }
    }

    public static String readFileToStr(Path path) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            return new String(bytes);
        } catch (IOException e) {
            throw new GrootStreamRuntimeException(
                    CommonErrorCode
                            .UNSUPPORTED_OPERATION, ExceptionUtils.getMessage(e));
        }
    }

    public static void writeStringToFile(String filePath, String str) {
        PrintStream ps = null;
        try {
            File file = new File(filePath);
            ps = new PrintStream(new FileOutputStream(file));
            ps.println(str);
        } catch (FileNotFoundException e) {
            throw new GrootStreamRuntimeException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    ExceptionUtils.getMessage(e),
                    e);
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    public static void createParentFile(File file) {
        File parentFile = file.getParentFile();
        if (null != parentFile && !parentFile.exists()) {
            parentFile.mkdirs();
        }
    }


    /**
     * create a new file, delete the old one if it is exists.
     *
     * @param filePath filePath
     */
    public static void createNewFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }

        if (!file.getParentFile().exists()) {
            createParentFile(file);
        }
        file.createNewFile();
    }

    /**
     * return the line number of file
     *
     * @param filePath The file need be read
     * @return The file line number
     */
    public static Long getFileLineNumber(@NonNull String filePath) {
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            return lines.count();
        } catch (IOException e) {
            throw new GrootStreamRuntimeException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format("get file[%s] line error", filePath),
                    e);
        }
    }

    /**
     * return the line number of all files in the dirPath
     *
     * @param dirPath dirPath
     * @return The file line number of dirPath
     */
    public static Long getFileLineNumberFromDir(@NonNull String dirPath) {
        File file = new File(dirPath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null) {
                return 0L;
            }
            return Arrays.stream(files)
                    .map(
                            currFile -> {
                                if (currFile.isDirectory()) {
                                    return getFileLineNumberFromDir(currFile.getPath());
                                } else {
                                    return getFileLineNumber(currFile.getPath());
                                }
                            })
                    .mapToLong(Long::longValue)
                    .sum();
        }
        return getFileLineNumber(file.getPath());
    }

    /**
     * create a dir, if the dir exists, clear the files and sub dirs in the dir.
     *
     * @param dirPath dirPath
     */
    public static void createNewDir(@NonNull String dirPath) {
        deleteFile(dirPath);
        File file = new File(dirPath);
        file.mkdirs();
    }

    /**
     * clear dir and the sub dir
     *
     * @param filePath filePath
     */
    public static void deleteFile(@NonNull String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.isDirectory()) {
                deleteFiles(file);
            }
            file.delete();
        }
    }

    private static void deleteFiles(@NonNull File file) {
        try {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                File thisFile = files[i];
                if (thisFile.isDirectory()) {
                    deleteFiles(thisFile);
                }
                thisFile.delete();
            }
            file.delete();

        } catch (Exception e) {
            String errorMsg = String.format("Delete file [%s] failed", file.getPath());
            throw new GrootStreamRuntimeException(
                    CommonErrorCode.UNSUPPORTED_OPERATION, errorMsg, e);
        }
    }
    public static String getFileExtension(@NonNull String fullName) {
        String fileName = new File(fullName).getName();
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? "" : fileName.substring(dotIndex + 1);
    }

    public static String getFileName(@NonNull String filePath) {
        return filePath.substring(filePath.lastIndexOf(File.separatorChar) + 1);
    }
}
