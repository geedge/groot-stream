package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SnowflakeConfig implements Serializable {
    private String dataCenterId = ServerConfigOptions.SNOWFLAKE_DATA_CENTER_ID.defaultValue();
}
