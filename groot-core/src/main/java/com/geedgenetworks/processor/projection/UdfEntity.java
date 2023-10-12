package com.geedgenetworks.processor.projection;

import com.geedgenetworks.udf.Udf;
import com.googlecode.aviator.Expression;

import java.io.Serializable;

public class UdfEntity implements Serializable {
    private Udf udf;
    private Expression compiledExp;
    private String name;
    private String className;

    public Udf getUdfFunction() {
        return udf;
    }

    public void setUdfFunction(Udf udf) {
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
