package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import lombok.Data;
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
 * @date 2024/8/26 16:27
 */
public class BaseStationKnowledgeBaseHandler extends AbstractSingleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(BaseStationKnowledgeBaseHandler.class);

    private HashMap<String, BaseStationLocation> baseStationLocationHashMap = new HashMap<>();

    private BaseStationKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final BaseStationKnowledgeBaseHandler instance = new BaseStationKnowledgeBaseHandler();
    }

    public static BaseStationKnowledgeBaseHandler getInstance() {
        return BaseStationKnowledgeBaseHandler.InstanceHolder.instance;
    }

    @Override
    public Boolean buildKnowledgeBase() {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("cell_id");
            needColumns.add("longitude");
            needColumns.add("latitude");
            byte[] content = downloadFile();
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HashMap<String, BaseStationLocation> newBaseStationLocationHashMap = new HashMap<>((int) (highCsvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String cellId = line.get("cell_id");
                    BaseStationLocation baseStationLocation = new BaseStationLocation();
                    baseStationLocation.setLongitude(parseDouble(line.get("longitude")));
                    baseStationLocation.setLatitude(parseDouble(line.get("latitude")));
                    newBaseStationLocationHashMap.put(cellId, baseStationLocation);
                } catch (Exception lineException) {
                    logger.error(this.getClass().getSimpleName() + " line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            baseStationLocationHashMap = newBaseStationLocationHashMap;
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
            return false;
        }
        return true;
    }

    public BaseStationLocation lookup(String ecgi) {
        return baseStationLocationHashMap.get(ecgi);
    }

    public void close() {
        baseStationLocationHashMap.clear();
        baseStationLocationHashMap = null;
    }

    private double parseDouble(String str) {
        try {
            return Double.parseDouble(str);
        } catch (Exception e) {
            return 0;
        }
    }

    @Data
    public static final class BaseStationLocation {
        private double longitude;
        private double latitude;
    }
}