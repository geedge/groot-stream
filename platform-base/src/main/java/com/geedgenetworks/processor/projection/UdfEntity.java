package com.geedgenetworks.processor.projection;

import com.geedgenetworks.udf.UdfFunction;
import com.googlecode.aviator.Expression;

import java.io.Serializable;

public class UdfEntity implements Serializable {
    private UdfFunction udfFunction;
    private Expression compiledExp;

    public UdfFunction getUdfFunction() {
        return udfFunction;
    }

    public void setUdfFunction(UdfFunction udfFunction) {
        this.udfFunction = udfFunction;
    }

    public Expression getCompiledExp() {
        return compiledExp;
    }

    public void setCompiledExp(Expression compiledExp) {
        this.compiledExp = compiledExp;
    }
}
