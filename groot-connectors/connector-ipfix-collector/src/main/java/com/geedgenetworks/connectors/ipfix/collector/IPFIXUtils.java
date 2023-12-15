package com.geedgenetworks.connectors.ipfix.collector;

import cn.hutool.json.JSONArray;
import com.geedgenetworks.core.pojo.ipfix.Record;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import scala.xml.Node;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class IPFIXUtils {
    private static final Log logger = LogFactory.get();

    public static ArrayList<Node> getElements() {
        ArrayList<Node> nodes = new ArrayList<>();
        try {
            JSONObject jsonObject = JSONUtil.readJSONObject(new File(IPFIXUtils.class.getClassLoader().getResource("ipfix_schema.json").getPath()), StandardCharsets.UTF_8);
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
            logger.error("load ipfix_schema.json error.", e);
        }
        return nodes;
    }
}
