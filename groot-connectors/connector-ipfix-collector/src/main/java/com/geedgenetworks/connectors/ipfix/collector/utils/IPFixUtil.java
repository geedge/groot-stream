package com.geedgenetworks.connectors.ipfix.collector.utils;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.connectors.ipfix.collector.pojo.Element;
import scala.xml.Node;

import java.util.ArrayList;

public class IPFixUtil {
    private static final String ELEMENT_ROOT_NODE = "record";
    private static final String ENTERPRISE_ID_NODE = "PEN_number";

    public static ArrayList<Node> getElements() {
        ArrayList<Node> elements = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(IPFixUtil.class.getClassLoader().getResource("ipfix_schema.json"));
        int enterpriseId = (int) jsonObject.get(ENTERPRISE_ID_NODE);
        for (Object jsonValue : jsonObject.values()) {
            if (jsonValue instanceof JSONArray) {
                for (Object object : (JSONArray) jsonValue) {
                    JSONObject elementJson = (JSONObject) object;
                    Element element = JSON.parseObject(elementJson.toString(), Element.class);
                    if (element.getName() != null && element.getDataType() != null) {
                        element.setEnterpriseId(enterpriseId);
                        String recordXmlStr = cn.hutool.json.XML.toXml(JSONUtil.parseObj(element), ELEMENT_ROOT_NODE);
                        elements.add((Node) scala.xml.XML.loadString(recordXmlStr));
                    }
                }
            }
        }
        return elements;
    }

}
