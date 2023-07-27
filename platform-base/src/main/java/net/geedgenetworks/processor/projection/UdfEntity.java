package net.geedgenetworks.processor.projection;

import com.googlecode.aviator.Expression;
import net.geedgenetworks.udf.UdfFunction;

public class UdfEntity {
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
