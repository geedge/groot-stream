package com.geedgenetworks.utils;

import cn.hutool.core.io.IoUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.ipfix.Element;
import com.geedgenetworks.pojo.ipfix.Elements;
import com.geedgenetworks.pojo.ipfix.Record;
import scala.xml.Node;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class IPFIXUtils {
    private static final Log logger = LogFactory.get();

    public static ArrayList<Node> getElements() {
        ArrayList<Node> nodes = new ArrayList<>();
        try {
            InputStream jsonStream = IPFIXUtils.class.getClassLoader().getResourceAsStream("ipfix.json");
            if (jsonStream != null) {
                String jsonStr = IoUtil.read(jsonStream, StandardCharsets.UTF_8);
                Elements elements = JSONUtil.toBean(jsonStr, Elements.class, true);
                int pen_number = elements.getPEN_number();
                ArrayList<Element> elementList = new ArrayList<>();
                elementList.addAll(elements.getBASE_elements());
                elementList.addAll(elements.getSSL_elements());
                elementList.addAll(elements.getHTTP_elements());
                elementList.addAll(elements.getMAIL_elements());
                elementList.addAll(elements.getDNS_elements());
                elementList.addAll(elements.getDTLS_elements());
                elementList.addAll(elements.getQUIC_elements());
                elementList.addAll(elements.getFTP_elements());
                elementList.addAll(elements.getSIP_elements());
                elementList.addAll(elements.getRTP_elements());
                elementList.addAll(elements.getSSH_elements());
                elementList.addAll(elements.getBGP_elements());
                elementList.addAll(elements.getRDP_elements());
                elementList.addAll(elements.getTunnels_elements());
                elementList.addAll(elements.getStratum_elements());
                Record record = new Record();
                for (Element element : elementList) {
                    record.setEnterpriseId(pen_number);
                    record.setElementId(element.getElement_id());
                    record.setName(element.getElement_name());
                    record.setDataType(element.getElement_type());
                    JSONObject jsonRecord = JSONUtil.parseObj(record);
                    String xmlStr = cn.hutool.json.XML.toXml(jsonRecord, "record");
                    Node node = (Node) scala.xml.XML.loadString(xmlStr);
                    nodes.add(node);
                }
            } else {
                logger.error("load ipfix.json error.");
            }
        } catch (Exception e) {
            logger.error("load ipfix.json error.", e);
        }
        return nodes;
    }
}
