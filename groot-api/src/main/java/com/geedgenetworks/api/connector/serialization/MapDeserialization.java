package com.geedgenetworks.api.connector.serialization;

import java.io.IOException;
import java.util.Map;

public interface MapDeserialization {
    Map<String, Object> deserializeToMap(byte[] bytes) throws IOException;
}
