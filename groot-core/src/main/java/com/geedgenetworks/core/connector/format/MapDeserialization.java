package com.geedgenetworks.core.connector.format;

import java.io.IOException;
import java.util.Map;

public interface MapDeserialization {
    Map<String, Object> deserializeToMap(byte[] bytes) throws IOException;
}
