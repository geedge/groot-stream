package com.geedgenetworks.connectors.ipfix.util;

import com.geedgenetworks.pojo.ipfix.Element;
import com.geedgenetworks.pojo.ipfix.Elements;
import com.geedgenetworks.pojo.ipfix.Record;

import cn.hutool.core.io.IoUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import scala.xml.Node;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class IPFIXUtils {
    private static final Log logger = LogFactory.get();

    public static ArrayList<Node> getElements() {
        ArrayList<Node> nodes = new ArrayList<>();
        try {
            InputStream jsonStream =
                    IPFIXUtils.class.getClassLoader().getResourceAsStream("ipfix.json");
            if (jsonStream != null) {
                String jsonStr = IoUtil.read(jsonStream, StandardCharsets.UTF_8);
                Elements elements = JSONUtil.toBean(jsonStr, Elements.class, true);
                int pen_number = elements.getPEN_number();
                ArrayList<Element> elementList = new ArrayList<>();
                if (elements.getBASE_elements() != null)
                    elementList.addAll(elements.getBASE_elements());
                if (elements.getSSL_elements() != null)
                    elementList.addAll(elements.getSSL_elements());
                if (elements.getHTTP_elements() != null)
                    elementList.addAll(elements.getHTTP_elements());
                if (elements.getMAIL_elements() != null)
                    elementList.addAll(elements.getMAIL_elements());
                if (elements.getDNS_elements() != null)
                    elementList.addAll(elements.getDNS_elements());
                if (elements.getDTLS_elements() != null)
                    elementList.addAll(elements.getDTLS_elements());
                if (elements.getQUIC_elements() != null)
                    elementList.addAll(elements.getQUIC_elements());
                if (elements.getFTP_elements() != null)
                    elementList.addAll(elements.getFTP_elements());
                if (elements.getSIP_elements() != null)
                    elementList.addAll(elements.getSIP_elements());
                if (elements.getRTP_elements() != null)
                    elementList.addAll(elements.getRTP_elements());
                if (elements.getSSH_elements() != null)
                    elementList.addAll(elements.getSSH_elements());
                if (elements.getBGP_elements() != null)
                    elementList.addAll(elements.getBGP_elements());
                if (elements.getRDP_elements() != null)
                    elementList.addAll(elements.getRDP_elements());
                if (elements.getTunnels_elements() != null)
                    elementList.addAll(elements.getTunnels_elements());
                if (elements.getStratum_elements() != null)
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
