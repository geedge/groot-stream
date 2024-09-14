package com.geedgenetworks.core.udf.udtf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.TableFunction;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Preconditions;

import java.util.*;

public class PathUnroll implements TableFunction {
    private String pathField;
    private String fileField;
    private char separator;

    private String outputPathField;
    private String outputFileField;
    private List<Event> events;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext c) {
        Preconditions.checkArgument(c.getLookup_fields().size() >= 1, "input fields requested one path param at least");
        Preconditions.checkArgument(CollectionUtils.isEmpty(c.getOutput_fields()) || c.getOutput_fields().size() == c.getOutput_fields().size(), "output fields requested same count param with input fields");
        pathField = c.getLookup_fields().get(0);
        fileField = c.getLookup_fields().size() == 1? null: c.getLookup_fields().get(1);

        outputPathField = CollectionUtils.isEmpty(c.getOutput_fields())? pathField : c.getOutput_fields().get(0);
        outputFileField = CollectionUtils.isEmpty(c.getOutput_fields()) || c.getLookup_fields().size() == 1 ? fileField : c.getOutput_fields().get(1);
        Map<String, Object> params = c.getParameters() == null? Collections.EMPTY_MAP:c.getParameters();
        String sep = params.getOrDefault("separator", "/").toString();
        Preconditions.checkArgument(sep.length() == 1, "separator mush has one char");
        separator = sep.charAt(0);
        events = new ArrayList<>();
    }

    @Override
    public List<Event> evaluate(Event event) {
        Map<String, Object> map = event.getExtractedFields();
        String p = (String) map.get(pathField);
        // 去除path结尾的分隔符
        final String path = StringUtils.isBlank(p)? null: (separator != p.charAt(p.length() - 1) ? p: p.substring(0, p.length() - 1));
        final String fileName = fileField == null? null: (String) map.get(fileField);

        if (StringUtils.isBlank(path)) {
            return Collections.emptyList();
        }

        if(events.size() > 100){
            events = new ArrayList<>();
        }else if(events.size() > 0){
            events.clear();
        }
        Event e;
        Map<String, Object> fields;

        // 拆分path
        int index = path.indexOf(separator);
        String subPath;
        while (index > 0) {
            subPath = path.substring(0, index);
            e = new Event();
            fields = new HashMap<>(map);
            fields.put(outputPathField, subPath);
            if(outputFileField != null){
                fields.put(outputFileField, null);
            }
            e.setExtractedFields(fields);
            events.add(e);
            index = path.indexOf(separator, index + 1);
        }
        boolean hasFile = StringUtils.isNotBlank(fileName);
        boolean pathContainsFile = hasFile && path.endsWith(fileName);

        if(!hasFile){
            e = new Event();
            fields = new HashMap<>(map);
            fields.put(outputPathField, path);
            if(outputFileField != null){
                fields.put(outputFileField, null);
            }
            e.setExtractedFields(fields);
            events.add(e);
        }else{
            e = new Event();
            fields = new HashMap<>(map);
            fields.put(outputPathField, path);
            if(outputFileField != null){
                fields.put(outputFileField, pathContainsFile? fileName:null);
            }
            e.setExtractedFields(fields);
            events.add(e);

            // 输出path + file
            if(!pathContainsFile){
                e = new Event();
                fields = new HashMap<>(map);
                fields.put(outputPathField, path + separator + fileName);
                if(outputFileField != null){
                    fields.put(outputFileField, fileName);
                }
                e.setExtractedFields(fields);
                events.add(e);
            }
        }

        return events;
    }

    @Override
    public void close() {}

    @Override
    public String functionName() {
        return "PATH_UNROLL";
    }
}
