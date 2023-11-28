package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class AsnLookupConfig implements Serializable {
    private String vendorId = ServerConfigOptions.ASN_LOOKUP_VENDOR_ID.defaultValue();
    private String defaultMMDBFile = ServerConfigOptions.ASN_LOOKUP_DEFAULT_MMDB_FILE.defaultValue();
    private String userDefinedMMDBFile = ServerConfigOptions.ASN_LOOKUP_USER_DEFINED_MMDB_FILE.defaultValue();


}
