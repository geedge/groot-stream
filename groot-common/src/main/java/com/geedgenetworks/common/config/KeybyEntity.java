package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@Data
public class KeybyEntity implements Serializable {
    private Map<String,Object> keys;
    private String keysToString;

    public KeybyEntity(Map<String, Object> keys) {
        this.keys = keys;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keysToString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeybyEntity other = (KeybyEntity) o;
        return Objects.equals(keysToString, other.keysToString);
    }
}
