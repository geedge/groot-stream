package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/9/2 15:20
 */
class BaseStationLookupTest {

    private static BaseStationLookup baseStationLookup;

    @BeforeAll
    static void setUp() {
        /**
         * Create a UDFContext object and set the parameters
         * e.g.:
         *      - function: BASE_STATION_LOOKUP
         *         lookup_fields: [ cell_id ]
         *         output_fields: [ subscriber_longitude,subscriber_latitude ]
         *         parameters:
         *           kb_name: base_station_location
         */
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Arrays.asList("cell_id"));
        udfContext.setOutput_fields(Arrays.asList("subscriber_longitude", "subscriber_latitude"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "cell_id,longitude,latitude\n460-11-630947-1,93.9290001,42.66884";
        mockKnowledgeBaseHandler(content);

        baseStationLookup = new BaseStationLookup();
        baseStationLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("cell_id", "460-11-630947-1");
        fields.put("cell_type", 1L);
        event.setExtractedFields(fields);
        Event evaluate = baseStationLookup.evaluate(event);
        assertEquals(93.9290001, evaluate.getExtractedFields().get("subscriber_longitude"));
        assertEquals(42.66884, evaluate.getExtractedFields().get("subscriber_latitude"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}
