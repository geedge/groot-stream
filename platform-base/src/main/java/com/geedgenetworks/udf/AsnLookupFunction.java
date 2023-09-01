package com.geedgenetworks.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.utils.IPUtils;
import com.geedgenetworks.utils.StringUtil;

public class AsnLookupFunction implements UdfFunction {

    private ProjectionFunction projection_function;
    private static final Log logger = LogFactory.get();

    private final String vender ;


    public AsnLookupFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
        this.vender =  projection_function.getParameters().get("kb_vender").toString();

    }


    @Override
    public void udffunction(Event event) {
        if(StringUtil.isNotEmpty(vender)) {
            event.getExtractedFields().put(projection_function.getOutput_fields().get(0),IPUtils.venderWithAsnLookup.get(vender).asnLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
        }
        else {
            logger.error("知识库id为空");

        }

    }
}
