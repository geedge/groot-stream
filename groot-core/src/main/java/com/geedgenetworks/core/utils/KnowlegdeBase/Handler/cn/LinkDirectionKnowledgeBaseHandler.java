package com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn;

import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/17 15:06
 */
public class LinkDirectionKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(LinkDirectionKnowledgeBaseHandler.class);

    private HashMap<Long, String> linkMap = new HashMap<>();

    private LinkDirectionKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final LinkDirectionKnowledgeBaseHandler instance = new LinkDirectionKnowledgeBaseHandler();
    }

    public static LinkDirectionKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    public Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("peer_city");
            needColumns.add("link_id");
            byte[] content = downloadFile(knowledgeMetedataCache.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<Long, String> newLinkMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String peerCity = line.get("peer_city");
                    String linkIdStr = line.get("link_id");
                    long linkId = Long.parseLong("".equals(linkIdStr) ? "0" : linkIdStr);

                    newLinkMap.put(linkId, peerCity);
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            linkMap = newLinkMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public String lookup(String linkId) {
        try {
            long id = Long.parseLong(linkId);
            return linkMap.get(id);
        } catch (Exception e) {
            logger.warn("invalid link id: " + linkId);
        }
        return null;
    }
}
