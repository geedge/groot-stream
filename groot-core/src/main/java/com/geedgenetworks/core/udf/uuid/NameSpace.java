package com.geedgenetworks.core.udf.uuid;

import com.geedgenetworks.common.exception.GrootStreamRuntimeException;

import java.util.UUID;

import static com.geedgenetworks.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;

public enum NameSpace {

    NAMESPACE_IP("NAMESPACE_IP",UUID.fromString("6ba7b890-9dad-11d1-80b4-00c04fd430c8")),
    NAMESPACE_DOMAIN("NAMESPACE_DOMAIN", UUID.fromString("6ba7b891-9dad-11d1-80b4-00c04fd430c8")),
    NAMESPACE_APP("NAMESPACE_APP", UUID.fromString("6ba7b892-9dad-11d1-80b4-00c04fd430c8")),
    NAMESPACE_SUBSCRIBER("NAMESPACE_SUBSCRIBER", UUID.fromString("6ba7b893-9dad-11d1-80b4-00c04fd430c8"));
    private final String name;
    private final UUID uuid;
    NameSpace(String name, UUID uuid) {
        this.name = name;
        this.uuid = uuid;
    }
    public static UUID getUuidByName(String name) {
        for (NameSpace nameSpace : NameSpace.values()) {
            if (nameSpace.name.equals(name)) {
                return nameSpace.uuid;
            }
        }
        throw new GrootStreamRuntimeException(ILLEGAL_ARGUMENT,"No enum constant " + NameSpace.class.getCanonicalName() + "." + name);
    }

}
