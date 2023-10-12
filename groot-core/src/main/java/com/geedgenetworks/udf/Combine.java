package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

public class Combine implements Udf {


    private ProjectionFunction projection_function;

    private StringBuilder stringBuilder;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
        if(projection_function.getParameters()!=null && projection_function.getParameters().size()!=0){
            ArrayList<String> keys = new ArrayList<>();
            for (Map.Entry<String, Object> entry : projection_function.getParameters().entrySet()) {
                String key = entry.getKey();
                keys.add(key);
            }
            keys.sort(new NaturalNumberStringComparator());
            stringBuilder =new StringBuilder();
            for(String key: keys){
                stringBuilder.append(projection_function.getParameters().get(key));
            }
        }
    }

    @Override
    public Event evaluate(Event event) {

        if (event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))) {
            stringBuilder.append(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)));
            event.getExtractedFields().put(projection_function.getOutput_fields().get(0), stringBuilder.toString());
        }
        return event;
    }


    @Override
    public String functionName() {
        return "COMBINE";
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