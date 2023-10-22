package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.core.udf.UDF;

import com.googlecode.aviator.Expression;

import java.io.Serializable;

public class UdfEntity implements Serializable {
    private UDF udf;
    private Expression compiledExp;
    private String name;
    private String className;

    public UDF getUdfFunction() {
        return udf;
    }

    public void setUdfFunction(UDF udf) {
        this.udf = udf;
    }

    public Expression getCompiledExp() {
        return compiledExp;
    }

    public void setCompiledExp(Expression compiledExp) {
        this.compiledExp = compiledExp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }
}
