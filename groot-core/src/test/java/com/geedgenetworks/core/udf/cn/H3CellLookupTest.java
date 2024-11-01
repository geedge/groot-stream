package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/9/6 16:15
 */
public class H3CellLookupTest {


    private static H3CellLookup h3CellLookup;

    @BeforeAll
    static void setUp() {
        /**
         * Create a UDFContext object and set the parameters
         * e.g.:
         *      - function: BASE_STATION_LOOKUP
         *         lookup_fields: [ subscriber_longitude,subscriber_latitude ]
         *         output_fields: [ first_location ]
         *         parameters:
         *           resolution: 9
         */
        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(Arrays.asList("subscriber_longitude", "subscriber_latitude"));
        udfContext.setOutputFields(Arrays.asList("first_location"));
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("resolution", 9);
        udfContext.setParameters(parameters);

        RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);

        /*String content = "cell_id,longitude,latitude\n460-11-630947-1,93.9290001,42.66884";
        mockKnowledgeBaseHandler(content);*/

        h3CellLookup = new H3CellLookup();
        h3CellLookup.open(runtimeContext, udfContext);
    }

    @Test
    void testValueExpression() throws IOException {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("subscriber_longitude", 116.390249);
        fields.put("subscriber_latitude", 39.905392);
        event.setExtractedFields(fields);
        Event evaluate = h3CellLookup.evaluate(event);
        assertEquals("8931aa42853ffff", evaluate.getExtractedFields().get("first_location"));

    }
}
