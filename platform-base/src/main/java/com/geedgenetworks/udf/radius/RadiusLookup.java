package com.geedgenetworks.udf.radius;

import com.geedgenetworks.utils.hbase.HBaseClient;
import com.geedgenetworks.utils.hbase.HbaseLookup;
import com.geedgenetworks.utils.hbase.async.state.Cached;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.io.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.BiConsumer;

import static com.geedgenetworks.config.commonConfig.*;

/**
 * This class provides methods to interact with HBase for fetching radius-related data.
 *
 * @author chaoc
 * @since 1.0
 */
public class RadiusLookup extends HbaseLookup<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(RadiusLookup.class);

    private static final int DEFAULT_HBASE_RADIUS_SCAN_MAX_ROWS = 10000;
    private static final String FIELD_NAME_FRAMED_IP = "framed_ip";
    private static final String FIELD_NAME_ACCOUNT = "account";
    private static final String FIELD_NAME_ACCT_STATUS_TYPE = "acct_status_type";

    public RadiusLookup(Configuration parameters) {
        super(parameters);
    }

    @Override
    protected BiConsumer<HBaseClient, Cached<String, String>> getUpdate() {
        return (client, cache) -> {
            try {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                client.scan(HBASE_RADIUS_TABLE_NAME,
                        HBASE_RADIUS_SCAN_MAX_ROWS > 0 ? HBASE_RADIUS_SCAN_MAX_ROWS : DEFAULT_HBASE_RADIUS_SCAN_MAX_ROWS,
                        TimeRange.allTime(),
                        ret -> {
                            String framedIp = ret.getString(RADIUS_FAMILY_NAME, FIELD_NAME_FRAMED_IP);
                            String account = ret.getString(RADIUS_FAMILY_NAME, FIELD_NAME_ACCOUNT);
                            AcctStatusType type = AcctStatusType.of(ret.getInt(RADIUS_FAMILY_NAME, FIELD_NAME_ACCT_STATUS_TYPE));
                            switch (type) {
                                case STOP:
                                    cache.remove(framedIp);
                                    break;
                                case START:
                                    cache.set(framedIp, account);
                                    break;
                                default:
                            }
                        });
                stopWatch.stop();
                LOG.info("Updating the Radius relationship data took a total of {} ms.", stopWatch.getTime());
            } catch (IOException e) {
                LOG.warn("Error while caching radius relations.", e);
            }
        };
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private enum AcctStatusType {
        START(1), STOP(2), UNKNOWN(0);
        private final int code;

        AcctStatusType(int code) {
            this.code = code;
        }

        public static AcctStatusType of(int code) {
            AcctStatusType[] values = AcctStatusType.values();
            for (AcctStatusType value : values) {
                if (value.code == code) {
                    return value;
                }
            }
            return UNKNOWN;
        }
    }
}
