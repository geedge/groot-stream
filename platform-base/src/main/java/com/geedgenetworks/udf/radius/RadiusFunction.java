package com.geedgenetworks.udf.radius;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.config.commonConfig;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.UdfFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RadiusFunction implements UdfFunction {

    public static  String DATA_RELATIONSHIP_MODEL;
    private static final Log logger = LogFactory.get();
    private ProjectionFunction projection_function;

    private static Map<String, String> radiusMap = new ConcurrentHashMap<>(16);
    private static Map<String, HashMap<String, Object>> gtpcMap = new ConcurrentHashMap<>(16);
    private static Connection connection;
    private static Long time;
    private static int HBASE_TICK_TUPLE_FREQ_SECS;

    public RadiusFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
        DATA_RELATIONSHIP_MODEL= (String) projection_function.getParameters().get("data_relationship_model");
        getConnection();
    }


    @Override
    public void udffunction(Event event) {
        if ("vsys".equals(DATA_RELATIONSHIP_MODEL)) {
            int vsysId = Integer.parseInt(event.getExtractedFields().get("common_vsys_id").toString());
            event.getExtractedFields().put(projection_function.getOutput_fields().get(0), HBaseUtils.getAccount( projection_function.getLookup_fields().get(0)+ vsysId));
        } else {
            event.getExtractedFields().put(projection_function.getOutput_fields().get(0),HBaseUtils.getAccount( projection_function.getLookup_fields().get(0)));
        }
    }


    private  void getConnection() {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", commonConfig.SNOWID_ZK);
            configuration.set("hbase.client.retries.number", "1");
            configuration.set("hbase.client.pause", "100");
            configuration.set("hbase.rpc.timeout", commonConfig.HBASE_RPC_TIMEOUT);
            configuration.set("zookeeper.recovery.retry", "1");
            configuration.set("zookeeper.recovery.retry.intervalmill", "200");
            connection = ConnectionFactory.createConnection(configuration);
            time = System.currentTimeMillis();
            logger.warn("HBaseUtils get HBase connection,now to getAll().");
        } catch (IOException ioe) {
            logger.error("HBaseUtils getHbaseConn() IOException===>{" + ioe + "}<===");
        } catch (RuntimeException e) {
            logger.error("HBaseUtils getHbaseConn() Exception===>{" + e + "}<===");
        }
    }
}
