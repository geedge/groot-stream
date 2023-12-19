package com.geedgenetworks.common.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnUtil {

    public static Map<String, Object> columnSelector(

            Map<String, Object> extractedFields, List<String> output_fields) {
        Map<String, Object> newExtractedFields = new HashMap<>();
        if (output_fields != null && !output_fields.isEmpty()) {

            for (String field : output_fields) {

                if (extractedFields.containsKey(field)) {
                    newExtractedFields.put(field, extractedFields.get(field));
                }
            }
            return newExtractedFields;
        } else {
            return extractedFields;
        }
    }

    public static Map<String, Object> columnRemover(
            Map<String, Object> extractedFields, List<String> remove_fields) {
        if (remove_fields != null && !remove_fields.isEmpty()) {

            for (String field : remove_fields) {

                extractedFields.remove(field);
            }
        }
        return extractedFields;
    }
}
