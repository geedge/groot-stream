package com.zdjizhi.base.udf;

import com.zdjizhi.base.pojo.ProjectionFunction;
import com.zdjizhi.utils.FormatUtils;

import java.util.Map;

public class DomainFunction implements UdfFunction {



    @Override
    public void udffunction(Map<String, Object> AuxiliaryFieldsAndValue, ProjectionFunction projection_function) {


        String  topDomain = FormatUtils.getTopPrivateDomain((String) AuxiliaryFieldsAndValue.get(projection_function.getInput_fields().get(0)));
        AuxiliaryFieldsAndValue.put(projection_function.getOutput_fields().get(0),topDomain);
    }
}
