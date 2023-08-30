package com.geedgenetworks.udf.lookup;

import com.geedgenetworks.config.commonConfig;
import com.geedgenetworks.udf.io.HBaseClient;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.io.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.geedgenetworks.config.commonConfig.HBASE_GTPC_SCAN_MAX_ROWS;
import static com.geedgenetworks.config.commonConfig.HBASE_GTPC_TABLE_NAME;

public class GtpCLookup extends HbaseLookup<String, GtpCLookup.GtpC> {

    private static final Logger LOG = LoggerFactory.getLogger(GtpCLookup.class);

    private final Function<HBaseClient, Map<String, GtpC>> update;

    /**
     * Constructs a new HbaseLookup instance with the given configuration parameters.
     *
     * @param parameters The configuration parameters for HBase connection.
     */
    public GtpCLookup(Configuration parameters) {
        super(parameters);

        this.update = client -> {
            Map<String, GtpC> tmp = new HashMap<>();
            try {
                client.scan(HBASE_GTPC_TABLE_NAME,
                        HBASE_GTPC_SCAN_MAX_ROWS > 0 ? HBASE_GTPC_SCAN_MAX_ROWS : Integer.MAX_VALUE,
                        TimeRange.allTime(), ret -> {
                            String upLinkTeId = ret.getString(commonConfig.GTPC_FAMILY_NAME, "uplink_teid");
                            String downLinkTeId = ret.getString(commonConfig.GTPC_FAMILY_NAME, "downlink_teid");
                            String imsi = ret.getString(commonConfig.GTPC_FAMILY_NAME, "imsi");
                            String imei = ret.getString(commonConfig.GTPC_FAMILY_NAME, "imei");
                            String phoneNumber = ret.getString(commonConfig.GTPC_FAMILY_NAME, "phone_number");
                            GtpC gtpC = new GtpC(phoneNumber, imsi, imei);
                            tmp.put(upLinkTeId, gtpC);
                            tmp.put(downLinkTeId, gtpC);
                        });
            } catch (IOException e) {
                LOG.warn("Error while caching GTP-C relations.", e);
            }
            return tmp;
        };
    }

    @Override
    protected Function<HBaseClient, Map<String, GtpC>> getUpdate() {
        return this.update;
    }

    public static final class GtpC {
        public String phoneNumber;
        public String imsi;
        public String imei;

        public GtpC(String phoneNumber, String imsi, String imei) {
            this.phoneNumber = phoneNumber;
            this.imsi = imsi;
            this.imei = imei;
        }
    }
}
