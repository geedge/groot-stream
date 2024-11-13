package com.geedgenetworks.core.udf.udtf;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


public class UnrollTest {
    PathUnroll pathUnroll;
    Event event;

    @Test
    public void explodePathWithNoFileField() {
        init("path", "out_path", ".");
        Map<String, Object> fields = event.getExtractedFields();
        fields.put("path", "ETHERNET.IPv4.TCP.ssl");
        String[] excepted = new String[]{"ETHERNET","ETHERNET.IPv4","ETHERNET.IPv4.TCP","ETHERNET.IPv4.TCP.ssl"};
        String[] outPaths = pathUnroll.evaluate(event).stream().map(x -> (String)x.getExtractedFields().get("out_path")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        assertArrayEquals(outPaths, excepted);
        // 忽略结尾的分隔符
        fields.put("path", "ETHERNET.IPv4.TCP.ssl.");
        outPaths = pathUnroll.evaluate(event).stream().map(x -> (String)x.getExtractedFields().get("out_path")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        assertArrayEquals(outPaths, excepted);
        // 空路径不输出
        fields.put("path", "");
        outPaths = pathUnroll.evaluate(event).stream().map(x -> (String)x.getExtractedFields().get("out_path")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        assertTrue(outPaths.length == 0);

        init("path", "out_path", "/");
        fields = event.getExtractedFields();
        fields.put("path", "ETHERNET/IPv4/TCP/ssl");
        excepted = new String[]{"ETHERNET","ETHERNET/IPv4","ETHERNET/IPv4/TCP","ETHERNET/IPv4/TCP/ssl"};
        outPaths = pathUnroll.evaluate(event).stream().map(x -> (String)x.getExtractedFields().get("out_path")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        assertArrayEquals(outPaths, excepted);
        // 忽略结尾的分隔符
        fields.put("path", "ETHERNET/IPv4/TCP/ssl/");
        outPaths = pathUnroll.evaluate(event).stream().map(x -> (String)x.getExtractedFields().get("out_path")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        assertArrayEquals(outPaths, excepted);
    }

    @Test
    public void explodePathWithFileField() {
        init("path", "file", "out_path", "out_file", ".");
        Map<String, Object> fields = event.getExtractedFields();
        fields.put("path", "ETHERNET.IPv4.TCP.ssl");
        fields.put("file", "ssl");
        String[] excepted = new String[]{"ETHERNET", "ETHERNET.IPv4", "ETHERNET.IPv4.TCP", "ETHERNET.IPv4.TCP.ssl"};
        String[] exceptedFile = new String[]{null, null, null, "ssl"};
        String[] outPaths = pathUnroll.evaluate(event).stream().map(x -> (String) x.getExtractedFields().get("out_path")).toArray(String[]::new);
        String[] outFiles = pathUnroll.evaluate(event).stream().map(x -> (String) x.getExtractedFields().get("out_file")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        System.out.println(JSON.toJSONString(outFiles));
        assertArrayEquals(outPaths, excepted);
        assertArrayEquals(outFiles, exceptedFile);
        // 忽略结尾的分隔符
        fields.put("path", "ETHERNET.IPv4.TCP.ssl.");
        fields.put("file", "ssl");
        outPaths = pathUnroll.evaluate(event).stream().map(x -> (String) x.getExtractedFields().get("out_path")).toArray(String[]::new);
        outFiles = pathUnroll.evaluate(event).stream().map(x -> (String) x.getExtractedFields().get("out_file")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        System.out.println(JSON.toJSONString(outFiles));
        assertArrayEquals(outPaths, excepted);
        assertArrayEquals(outFiles, exceptedFile);

        fields.put("path", "ETHERNET.IPv4.TCP.ssl");
        fields.put("file", "ssl.aa");
        excepted = new String[]{"ETHERNET", "ETHERNET.IPv4", "ETHERNET.IPv4.TCP", "ETHERNET.IPv4.TCP.ssl", "ETHERNET.IPv4.TCP.ssl.ssl.aa"};
        exceptedFile = new String[]{null, null, null, null,"ssl.aa"};
        outPaths = pathUnroll.evaluate(event).stream().map(x -> (String) x.getExtractedFields().get("out_path")).toArray(String[]::new);
        outFiles = pathUnroll.evaluate(event).stream().map(x -> (String) x.getExtractedFields().get("out_file")).toArray(String[]::new);
        System.out.println(JSON.toJSONString(outPaths));
        System.out.println(JSON.toJSONString(outFiles));
        assertArrayEquals(outPaths, excepted);
        assertArrayEquals(outFiles, exceptedFile);
    }

    private void init(String pathField, String outputPathField, String separator){
        init(pathField, null, outputPathField, null, separator);
    }

    private void init(String pathField, String fileField, String outputPathField, String outputFileField, String separator){
        pathUnroll = new PathUnroll();
        UDFContext c = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("separator", separator);
        c.setParameters(parameters);
        c.setLookupFields(Arrays.asList(pathField, fileField).stream().filter(x -> x != null).collect(Collectors.toList()));
        c.setOutputFields(Arrays.asList(outputPathField, outputFileField).stream().filter(x -> x != null).collect(Collectors.toList()));

        pathUnroll.open(null, c);
        event = new Event();
        Map<String, Object> fields = new HashMap<>();
        event.setExtractedFields(fields);
    }
}