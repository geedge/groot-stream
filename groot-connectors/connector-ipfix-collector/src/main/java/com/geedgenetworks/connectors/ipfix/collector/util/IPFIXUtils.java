package com.geedgenetworks.connectors.ipfix.collector.util;

import cn.hutool.json.JSONArray;
import com.geedgenetworks.connectors.ipfix.collector.IPFixSourceProvider;
import com.geedgenetworks.core.pojo.ipfix.Record;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import scala.xml.Node;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IPFIXUtils {
    private static final Log logger = LogFactory.get();

    public static ArrayList<Node> getElements() {
        ArrayList<Node> nodes = new ArrayList<>();
        try {
            JSONObject jsonObject = JSONUtil.readJSONObject(new File(IPFIXUtils.class.getClassLoader().getResource("ipfix.json").getPath()), StandardCharsets.UTF_8);
            int penNumber = (int) jsonObject.get("PEN_number");
            for (Object jsonValue : jsonObject.values()) {
                if (jsonValue instanceof JSONArray) {
                    for (Object object : (JSONArray) jsonValue) {
                        JSONObject elementJson = (JSONObject) object;
                        if (elementJson.containsKey("element_name") && elementJson.containsKey("element_type") && elementJson.containsKey("element_id")) {
                            Record record = new Record(penNumber, elementJson.getStr("element_name"), elementJson.getStr("element_type"), elementJson.getInt("element_id"));
                            JSONObject jsonRecord = JSONUtil.parseObj(record);
                            String xmlStr = cn.hutool.json.XML.toXml(jsonRecord, "record");
                            Node node = (Node) scala.xml.XML.loadString(xmlStr);
                            nodes.add(node);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("load ipfix.json error.", e);
        }
        return nodes;
    }

    public static Map<String, Object> dataConverter(Map<String, Object> extractedFields, Map<String, IPFixSourceProvider.ValueConverter> filedConverters) throws Exception {
        Map<String, Object> newExtractedFields = new HashMap<>();
        if (filedConverters != null && filedConverters.size() > 0) {
            for (String field : filedConverters.keySet()) {
                if (extractedFields.containsKey(field)) {
                    newExtractedFields.put(field, filedConverters.get(field).convert(extractedFields.get(field)));
                }
            }
            return newExtractedFields;
        } else {
            return extractedFields;
        }
    }
}
