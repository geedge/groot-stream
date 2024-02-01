package com.geedgenetworks.core.utils.cn.common;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;
import lombok.ToString;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/18 17:35
 */
@Data
@ToString
public class KnowledgeMetadata {

    @JSONField(name = "kb_id")
    private String id;

    private String name;

    private String sha256;

    private String format;

    private String path;

    private String category;

    @JSONField(name = "is_valid")
    private int isValid = 1;

    private String version;
}
