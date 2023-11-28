package com.geedgenetworks.core.udf.gtpc;

import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.core.utils.hbase.HBaseClient;
import com.geedgenetworks.core.utils.hbase.HbaseLookup;
import com.geedgenetworks.core.utils.hbase.async.state.Cached;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.io.TimeRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.BiConsumer;

public class GtpCLookupUtil extends HbaseLookup<String, GtpCLookupUtil.GtpC> {

    private static final Logger LOG = LoggerFactory.getLogger(GtpCLookupUtil.class);

    private static final int DEFAULT_HBASE_GTPC_SCAN_MAX_ROWS = 10000;

    private static final String FIELD_NAME_UP_LINK_TE_ID = "uplink_teid";
    private static final String FIELD_NAME_DOWN_LINK_TE_ID = "downlink_teid";
    private static final String FIELD_NAME_IMSI = "imsi";
    private static final String FIELD_NAME_IMEI = "imei";
    private static final String FIELD_NAME_PHONE_NUMBER = "phone_number";
    private static final String FIELD_NAME_MSG_TYPE = "msg_type";

    /**
     * Constructs a new HbaseLookup instance with the given configuration parameters.
     *
     * @param parameters The configuration parameters for HBase connection.
     */
    public GtpCLookupUtil(Configuration parameters) {
        super(parameters);
    }

    @Override
    protected BiConsumer<HBaseClient, Cached<String, GtpC>> getUpdate() {
        return (client, cached) -> {
            try {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                client.scan(
                        CommonConfig.HBASE_GTPC_TABLE_NAME,
                        CommonConfig.HBASE_GTPC_SCAN_MAX_ROWS > 0
                                ? CommonConfig.HBASE_GTPC_SCAN_MAX_ROWS
                                : DEFAULT_HBASE_GTPC_SCAN_MAX_ROWS,
                        TimeRange.allTime(),
                        ret -> {
                            String upLinkTeId =
                                    ret.getString(CommonConfig.GTPC_FAMILY_NAME, FIELD_NAME_UP_LINK_TE_ID);
                            String downLinkTeId =
                                    ret.getString(CommonConfig.GTPC_FAMILY_NAME, FIELD_NAME_DOWN_LINK_TE_ID);
                            String imsi = ret.getString(CommonConfig.GTPC_FAMILY_NAME, FIELD_NAME_IMSI);
                            String imei = ret.getString(CommonConfig.GTPC_FAMILY_NAME, FIELD_NAME_IMEI);
                            String phoneNumber =
                                    ret.getString(CommonConfig.GTPC_FAMILY_NAME, FIELD_NAME_PHONE_NUMBER);
                            MsgType msgType =
                                    MsgType.of(ret.getInt(CommonConfig.GTPC_FAMILY_NAME, FIELD_NAME_MSG_TYPE));
                            switch (msgType) {
                                case CREATE:
                                    GtpC gtpC = new GtpC(phoneNumber, imsi, imei);
                                    cached.set(upLinkTeId, gtpC);
                                    cached.set(downLinkTeId, gtpC);
                                case DELETE:
                                    cached.remove(upLinkTeId);
                                    cached.remove(downLinkTeId);
                                default:
                            }
                        });
                stopWatch.stop();
                LOG.info(
                        "Updating the GTPC relationship data took a total of {} ms.",
                        stopWatch.getTime());
            } catch (IOException e) {
                LOG.warn("Error while caching GTP-C relations.", e);
            }
        };
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

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private enum MsgType {
        CREATE(1),
        DELETE(2),
        UNKNOWN(0);
        private final int code;

        MsgType(int code) {
            this.code = code;
        }

        public static MsgType of(int code) {
            MsgType[] types = MsgType.values();
            for (MsgType type : types) {
                if (type.code == code) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }
}
