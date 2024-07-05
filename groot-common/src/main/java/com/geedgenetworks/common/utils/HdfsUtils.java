package com.geedgenetworks.common.utils;

import com.alibaba.fastjson2.JSONArray;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.shaded.com.google.common.base.Splitter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class HdfsUtils {

    private static FileSystem fs;
    private static HdfsUtils hdfsInstance;

    private HdfsUtils(Map<String, String> hdfsProp)  {
        Configuration config = new Configuration();
        String paths = hdfsProp.getOrDefault("dfs.namenodes","192.168.44.12");
        String replication = hdfsProp.getOrDefault("dfs.replication","1");
        String port = hdfsProp.getOrDefault("dfs.port","9000");
        String name = hdfsProp.getOrDefault("dfs.user","root");
        List<String> hadoopNameNodes = new ArrayList<>();
        String defaultFS = "";
        if (!paths.isEmpty()) {
            try {
                hadoopNameNodes= Splitter.on(",").trimResults().splitToList(paths);
            } catch (RuntimeException e) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The param of hadoop.namenodes is illegal");
            }
        }
        if (hadoopNameNodes.size() == 1) {
            defaultFS = "hdfs://" + hadoopNameNodes.get(0) + ":" + port;
            config.set("fs.defaultFS", defaultFS);
            config.set("dfs.replication", replication);
        } else if (hadoopNameNodes.size() > 1) {
            String nameservices = hdfsProp.getOrDefault("dfs.nameservices","ns1");
            String ns1 = hdfsProp.getOrDefault("dfs.ha.namenodes.ns1","nn1,nn2");
            defaultFS = "hdfs://" + nameservices;
            config.set("fs.defaultFS", defaultFS);
            config.set("dfs.nameservices", nameservices);
            config.set("dfs.ha.namenodes.ns1", ns1);
            config.set("dfs.namenode.rpc-address.ns1.nn1", hadoopNameNodes.get(0) + ":" + port);
            config.set("dfs.namenode.rpc-address.ns1.nn2", hadoopNameNodes.get(1) + ":" + port);
            config.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            config.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
            config.set("dfs.replication", replication);
        }
        try {
            fs = FileSystem.get(new URI(defaultFS), config, name);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "create hdfs fileSystem error",e);
        }
    }

    public static HdfsUtils getHdfsUtilInstance(Map<String, String> hdfsProp)  {
        if (hdfsInstance == null) {
            hdfsInstance = new HdfsUtils(hdfsProp);
        }
        return hdfsInstance;
    }

    public byte[] readBytes(Path hdfsPath) {

        try {
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