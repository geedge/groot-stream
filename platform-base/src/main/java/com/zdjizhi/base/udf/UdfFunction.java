package com.zdjizhi.base.udf;


import com.zdjizhi.base.pojo.ProjectionFunction;

import java.util.Map;

public interface UdfFunction {

      void udffunction(Map<String, Object> AuxiliaryFieldsAndValue, ProjectionFunction projection_function);


    }
