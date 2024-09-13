package com.geedgenetworks.core.udf.udtf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.TableFunction;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Preconditions;

import java.util.*;

public class ExplodeAppAndProtocol implements TableFunction {
    private String appField;
    private String decodedPathField;
    private String outputAppField;
    private String outputProtocolField;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext c) {
        Preconditions.checkArgument(c.getLookup_fields().size() == 2, "input fields requested for app and decodedPath");
        Preconditions.checkArgument(c.getOutput_fields().size() == 2, "output fields requested for app and decodedPath");
        appField = c.getLookup_fields().get(0);
        decodedPathField = c.getLookup_fields().get(1);
        outputAppField = c.getOutput_fields().get(0);
        outputProtocolField = c.getOutput_fields().get(1);
    }

    @Override
    public List<Event> evaluate(Event event) {
        Map<String, Object> map = event.getExtractedFields();
        String appFullPath = (String) map.get(appField);
        String decodedPath = (String) map.get(decodedPathField);

        if (StringUtils.isBlank(decodedPath)) {
            return Collections.emptyList();
        }

        // decoded_path和app进行拼接，格式化
        String app;
        if (StringUtils.isBlank(appFullPath)) {
            app = null;
        } else {
            String[] appSplits = appFullPath.split("\\.");
            app = appSplits[appSplits.length - 1];
            String firstAppProtocol = appSplits[0];
            String endProtocol = decodedPath.substring(decodedPath.lastIndexOf(".") + 1);
            if (endProtocol.equals(firstAppProtocol)) {
                if (appSplits.length > 1) {
                    decodedPath = decodedPath + appFullPath.substring(appFullPath.indexOf("."));
                }
            } else {
                decodedPath = decodedPath + "." + appFullPath;
            }
        }

        List<Event> events = new ArrayList<>();
        Event e;
        Map<String, Object> fields;

        // 拆分decodedPath
        int index = decodedPath.indexOf('.');
        String subDecodedPath;
        while (index > 0) {
            subDecodedPath = decodedPath.substring(0, index);
            e = new Event();
            fields = new HashMap<>(map);
            fields.put(outputAppField, null);
            fields.put(outputProtocolField, subDecodedPath);
            e.setExtractedFields(fields);
            events.add(e);
            index = decodedPath.indexOf('.', index + 1);
        }

        e = new Event();
        fields = new HashMap<>(map);
        fields.put(outputAppField, app);
        fields.put(outputProtocolField, decodedPath);
        e.setExtractedFields(fields);
        events.add(e);

        return events;
    }

    @Override
    public void close() {}

    @Override
    public String functionName() {
        return "EXPLODE_APP_AND_PROTOCOL";
    }
}
