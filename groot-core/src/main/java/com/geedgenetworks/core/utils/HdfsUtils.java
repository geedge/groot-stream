package com.geedgenetworks.core.utils;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import java.io.*;
import org.apache.hadoop.fs.FileSystem;



@Slf4j
public class HdfsUtils {

    public static byte[] readBytes(Path hdfsPath) {

        try {
            FileSystem fs = hdfsPath.getFileSystem(new Configuration());
            if (fs.exists(hdfsPath)) {
                try (FSDataInputStream inputStream = fs.open(hdfsPath);
                     ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                    return outputStream.toByteArray();
                } catch (RuntimeException e) {
                    String errorMsg = String.format("get hdfs file [%s] failed", hdfsPath);
                    throw new GrootStreamRuntimeException(
                            CommonErrorCode.FILE_OPERATION_ERROR, errorMsg, e);
                }
            }
            throw new GrootStreamRuntimeException(
                    CommonErrorCode.FILE_OPERATION_ERROR, String.format("hdfs filepath [%s] is not exists", hdfsPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}