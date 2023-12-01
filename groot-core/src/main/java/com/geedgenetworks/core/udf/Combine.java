package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

public class Combine implements UDF {

    private UDFContext udfContext;

    private StringBuilder stringBuilder;


    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        this.udfContext = udfContext;
        if (udfContext.getParameters() != null
                && udfContext.getParameters().size() != 0) {
            ArrayList<String> keys = new ArrayList<>();
            for (Map.Entry<String, Object> entry : udfContext.getParameters().entrySet()) {
                String key = entry.getKey();
                keys.add(key);
            }
            keys.sort(new NaturalNumberStringComparator());
            stringBuilder = new StringBuilder();
            for (String key : keys) {
                stringBuilder.append(udfContext.getParameters().get(key));
            }
        }
    }

    @Override
    public Event evaluate(Event event) {

        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {
            stringBuilder.append(
                    event.getExtractedFields().get(udfContext.getLookup_fields().get(0)));
            event.getExtractedFields()
                    .put(udfContext.getOutput_fields().get(0), stringBuilder.toString());
        }
        return event;
    }

    @Override
    public String functionName() {
        return "COMBINE";
    }

    @Override
    public void close() {

    }

    static class NaturalNumberStringComparator implements Comparator<String> {
        @Override
        public int compare(String s1, String s2) {
            String[] s1Array = s1.split("path");
            String[] s2Array = s2.split("path");
            Integer n1 = Integer.parseInt(s1Array[1]);
            Integer n2 = Integer.parseInt(s2Array[1]);
            return n1.compareTo(n2);
        }
    }

}
