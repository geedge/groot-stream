package com.geedgenetworks.expressions.codegen;

import com.geedgenetworks.expressions.Calc;
import com.geedgenetworks.expressions.EvalExpressionResolver;
import com.geedgenetworks.expressions.Expression;
import com.geedgenetworks.expressions.InvalidProgramException;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Converting a mathematical expression into executable code for evaluation.
 *
 * @author chaoc
 * @since 1.0
 */
public class CalcCodeConvertor {

    // The SimpleCompiler used for dynamic code compilation.
    private static final SimpleCompiler COMPILER = new SimpleCompiler();

    private static final AtomicLong COUNTER = new AtomicLong(0);

    static {
        COMPILER.setParentClassLoader(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Converts a mathematical expression into an executable Calc object.
     *
     * @param expr The mathematical expression to convert.
     * @return A Calc object capable of evaluating the expression.
     */
    public static Calc convert(final String expr) {
        EvalExpressionResolver expressionResolver = new EvalExpressionResolver();
        Expression expression = expressionResolver.resolve(expr);
        CodeGeneratorContext ctx = new CodeGeneratorContext();
        expression.accept(new EvalCodeGenerator(ctx));
        String className = "Calc$" + COUNTER.incrementAndGet();
        return compile(className, evalCode(ctx, className));
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private static String evalCode(final CodeGeneratorContext ctx, final String className) {
        return "public class " + className + " implements " + Calc.class.getCanonicalName() + " {\n" +
                "\n" +
                "       " + ctx.reuseMemberCode() + "\n" +
                "\n" +
                "        @Override\n" +
                "        public Object eval(java.util.Map<String, Object> data) {\n" +
                "           " + ctx.reuseLocalVariableCode() + "\n" +
                "           " + ctx.evalCode() + "\n" +
                "            return " + ctx.resultTerm() + ";\n" +
                "        }\n" +
                "    }";
    }

    private static Calc compile(final String className, final String code) {
        try {
            COMPILER.cook(code);
            return COMPILER.getClassLoader().loadClass(className)
                    .asSubclass(Calc.class).getConstructor().newInstance();
        } catch (CompileException e) {
            System.err.println(code);
            throw new InvalidProgramException("Calc program cannot be compiled. Please check expression syntax.");
        } catch (ClassNotFoundException |
                 InvocationTargetException | InstantiationException |
                 IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
