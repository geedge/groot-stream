package com.geedgenetworks.spi.table.schema;

import com.geedgenetworks.common.utils.HttpClientPoolUtil;
import com.geedgenetworks.shaded.org.apache.http.Header;
import com.geedgenetworks.shaded.org.apache.http.message.BasicHeader;
import org.apache.flink.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class HttpDynamicSchema extends DynamicSchema {
    static final Logger LOG = LoggerFactory.getLogger(HttpDynamicSchema.class);
    private static final Header header =  new BasicHeader("Content-Type", "application/x-www-form-urlencoded");
    private final String url;
    private final String key;
    public HttpDynamicSchema(String url, SchemaParser.Parser parser, long intervalMs) {
        super(parser, intervalMs);
        checkNotNull(url);
        this.url = url;
        this.key = String.format("%s_%s", url, TimeUtils.formatWithHighestUnit(Duration.ofMillis(intervalMs)));
        parseDataType(getDataTypeContent());
    }

    @Override
    public String getCacheKey() {
        return key;
    }

    @Override
    protected String getDataTypeContent() {
        try {
            String response = HttpClientPoolUtil.getInstance().httpGet(URI.create(url), header);
            return response;
        } catch (Exception e) {
            LOG.error("request " + url + " error", e);
            return null;
        }
    }
}
