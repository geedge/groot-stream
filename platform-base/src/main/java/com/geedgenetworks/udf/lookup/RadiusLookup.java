package com.geedgenetworks.udf.lookup;

import com.geedgenetworks.udf.io.HBaseClient;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.io.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * This class provides methods to interact with HBase for fetching radius-related data.
 *
 * @author chaoc
 * @since 1.0
 */
public class RadiusLookup extends HbaseLookup<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(RadiusLookup.class);
    // Fill in the actual table name
    private static final String HBASE_RADIUS_TABLE_NAME = "";
    // Fill in the actual family name
    private static final String RADIUS_FAMILY_NAME = "";

    private final Function<HBaseClient, Map<String, String>> update;

    public RadiusLookup(Configuration parameters) {
        super(parameters);

        this.update = client -> {
            final Map<String, String> tmp = new HashMap<>();
            try {
                // TODO: Read limit and time range from parameters
                client.scan(HBASE_RADIUS_TABLE_NAME, 0, TimeRange.allTime(), ret -> {
                    String framedIp = ret.getString(RADIUS_FAMILY_NAME, "framed_ip");
                    String account = ret.getString(RADIUS_FAMILY_NAME, "account");
                    tmp.put(framedIp, account);
                });
            } catch (IOException e) {
                LOG.warn("Error while caching radius relations.", e);
            }
            return tmp;
        };
    }

    @Override
    protected Function<HBaseClient, Map<String, String>> getUpdate() {
        return this.update;
    }

}
