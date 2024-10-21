package com.geedgenetworks.core.udf.uuid;

import com.geedgenetworks.common.exception.GrootStreamRuntimeException;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.geedgenetworks.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;

public enum UUIDNameSpace {

    NAMESPACE_IP(UUID.fromString("6ba7b890-9dad-11d1-80b4-00c04fd430c8")),
    NAMESPACE_DOMAIN(UUID.fromString("6ba7b891-9dad-11d1-80b4-00c04fd430c8")),
    NAMESPACE_APP(UUID.fromString("6ba7b892-9dad-11d1-80b4-00c04fd430c8")),
    NAMESPACE_SUBSCRIBER(UUID.fromString("6ba7b893-9dad-11d1-80b4-00c04fd430c8"));

    private final UUID uuid;

    // Static map to hold the mapping from name to UUID
    private static final Map<String, UUID> NAME_TO_UUID_MAP = new HashMap<>();

    // Static block to populate the map
    static {
        for (UUIDNameSpace namespace : UUIDNameSpace.values()) {
            NAME_TO_UUID_MAP.put(namespace.name(), namespace.uuid);
        }
    }

    UUIDNameSpace(UUID uuid) {
        this.uuid = uuid;
    }

    public static UUID getUUID(String name) {
        UUID uuid = NAME_TO_UUID_MAP.get(name);
        if (uuid == null) {
            throw new GrootStreamRuntimeException(ILLEGAL_ARGUMENT,"No enum constant " + UUIDNameSpace.class.getCanonicalName() + "." + name);
        }
        return uuid;
    }


}
