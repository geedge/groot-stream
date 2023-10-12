package com.geedgenetworks.expressions.codegen;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a context for generating code and managing reusable variables and statements.
 *
 * @author chaoc
 * @since 1.0
 */
public class CodeGeneratorContext {

    public static final String PARAM_NAME = "data";

    private static final String FIELD_PREFIX = "value";

    private final AtomicLong nameCounter = new AtomicLong(0);

    private final Map<Object, String> reusableMember = new HashMap<>();

    private final List<String> reusableMemberStatement = new ArrayList<>();

    private final Map<Object, String> reusableLocalVariable = new HashMap<>();

    private final List<String> reusableLocalVariableStatements = new ArrayList<>();

    private final List<String> evalCodeStatements = new ArrayList<>();

    /**
     * Adds a local variable statement to the context.
     *
     * @param statement The statement to add.
     */
    public void addLocalVariable(final String statement) {
        this.reusableLocalVariableStatements.add(statement);
    }

    /**
     * Adds a reusable constant to the context and returns its name.
     *
     * @param value The constant value to add.
     * @return The name of the added constant.
     */
    public String addReusableConstant(final Object value) {
        if (reusableMember.containsKey(value)) {
            return reusableMember.get(value);
        } else {
            String newName = newName();
            reusableMember.put(value, newName);
            String code = String.format("private final %s %s = %s;",
                    value.getClass().getCanonicalName(), newName, encodingValue(value));
            reusableMemberStatement.add(code);
            return newName;
        }
    }

    /**
     * Adds a reusable local variable to the context and returns its name.
     *
     * @param value The local variable value to add.
     * @return The name of the added local variable.
     */
    public String addReusableLocalVariable(final String value) {
        if (reusableLocalVariable.containsKey(value)) {
            return reusableLocalVariable.get(value);
        } else {
            String newName = newName();
            reusableLocalVariable.put(value, newName);
            addLocalVariable("final "
                    + Object.class.getCanonicalName() + " " + newName + ";");
            return newName;
        }
    }

    /**
     * Returns the code for reusing local variables.
     *
     * @return The code for reusing local variables.
     */
    public String reuseLocalVariableCode() {
        return String.join("\n", reusableLocalVariableStatements);
    }

    /**
     * Returns the code for reusing member variables.
     *
     * @return The code for reusing member variables.
     */
    public String reuseMemberCode() {
        return String.join("\n", reusableMemberStatement);
    }

    /**
     * Returns the code for evaluating expressions.
     *
     * @return The code for evaluating expressions.
     */
    public String evalCode() {
        return String.join("\n", evalCodeStatements);
    }


    /**
     * Adds evaluation code statements to the context.
     *
     * @param statement The evaluation code statement to add.
     */
    public void addEvalCodeStatements(final String statement) {
        this.evalCodeStatements.add(statement);
    }

    /**
     * Create a new name for variables.
     *
     * @param prefix The prefix used for field name.
     * @return The field name.
     */
    public String newName(String prefix) {
        return prefix + nameCounter.incrementAndGet();
    }

    /**
     * Create a new name for variables.
     *
     * @return The field name.
     */
    public String newName() {
        return newName(FIELD_PREFIX);
    }

    /**
     * Get result field name.
     *
     * @return The name be used at last time.
     */
    public String resultTerm() {
        return FIELD_PREFIX + nameCounter.get();
    }

    public String addLocalBoolVariable() {
        return this.addLocalBoolVariable(false);
    }

    public String addLocalBoolVariable(boolean isFinal) {
        return this.addLocalVariable(isFinal, Boolean.class, false);
    }

    public String addLocalObjectVariable() {
        return this.addLocalObjectVariable(false);
    }

    public String addLocalObjectVariable(boolean isFinal) {
        return this.addLocalVariable(isFinal, Object.class, null);
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private String encodingValue(final Object value) {
        if (value instanceof String) {
            return String.format("\"%s\"", value);
        } else if (value instanceof Integer) {
            return String.format(String.format("java.lang.Integer.valueOf(%s)", value));
        } else if (value instanceof Long) {
            return String.format(String.format("java.lang.Long.valueOf(%sL)", value));
        } else if (value instanceof Double) {
            return String.format(String.format("java.lang.Double.valueOf(%s)", value));
        } else if (value instanceof BigDecimal) {
            return String.format(String.format("java.math.BigDecimal.valueOf(%s)", value));
        } else if (value instanceof Boolean) {
            return String.format(String.format("java.lang.Boolean.valueOf(%s)", value));
        } else {
            return value.toString();
        }
    }

    private String addLocalVariable(boolean isFinal, Class<?> type, @Nullable Object initValue) {
        String name = newName();
        String code = isFinal ? "final " : "";
        code += type.getCanonicalName();
        code += " " + name;
        if (initValue != null) {
            code += " = " + initValue;
        }
        this.addLocalVariable(code + ";");
        return name;
    }
}
