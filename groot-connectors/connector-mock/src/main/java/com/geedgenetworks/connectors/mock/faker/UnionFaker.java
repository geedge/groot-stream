package com.geedgenetworks.connectors.mock.faker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class UnionFaker extends Faker<UnionFaker.FieldValuesAndComputeFieldFakers> {
    private final FieldsFaker[] fieldsFakers;
    private final boolean random;
    private final FieldValue[] fieldValuesTmp;
    private final List<FieldValue> fieldValues;
    private final FieldValuesAndComputeFieldFakers fieldValuesAndComputeFieldFakers;
    private final int[] weights;
    private final int weightMax;
    private Random r;
    private int index = 0;

    public UnionFaker(FieldsFaker[] fieldsFakers, boolean random) {
        this.fieldsFakers = fieldsFakers;
        this.random = random;
        this.fieldValuesTmp = new FieldValue[Arrays.stream(fieldsFakers).mapToInt(x -> x.fields.length).max().getAsInt()];
        this.fieldValues = new ArrayList<>();
        this.fieldValuesAndComputeFieldFakers = new FieldValuesAndComputeFieldFakers();
        this.weights = new int[fieldsFakers.length];
        weights[0] = fieldsFakers[0].weight;
        for (int i = 1; i < fieldsFakers.length; i++) {
            weights[i] = fieldsFakers[i].weight + weights[i - 1];
        }
        this.weightMax = weights[weights.length - 1];
        for (int i = 0; i < fieldValuesTmp.length; i++) {
            fieldValuesTmp[i] = new FieldValue();
        }
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        r = new Random();
        for (FieldsFaker fieldsFaker : fieldsFakers) {
            for (ObjectFaker.FieldFaker field : fieldsFaker.fields) {
                field.faker.init(instanceCount, instanceIndex);
            }
            for (ObjectFaker.ComputeFieldFaker field : fieldsFaker.computeFields) {
                field.faker.init(instanceCount, instanceIndex);
            }
        }
    }

    @Override
    public FieldValuesAndComputeFieldFakers geneValue() throws Exception {
        fieldValues.clear();

        FieldsFaker fieldsFaker;
        ObjectFaker.FieldFaker[] fakers;
        ObjectFaker.FieldFaker fieldFaker;
        Object value;
        FieldValue fieldValue;

        if (!random) {
            fieldsFaker = fieldsFakers[index];
            if (fieldsFaker.weightIndex == fieldsFaker.weight) {
                fieldsFaker.weightIndex = 0;
                index++;
                if (index == fieldsFakers.length) {
                    index = 0;
                }
                fieldsFaker = fieldsFakers[index];
            }
            fieldsFaker.weightIndex++;
        } else {
            int key = r.nextInt(weightMax) + 1;
            int index = Arrays.binarySearch(weights, key);
            if (index < 0) {
                index = -index - 1;
            }
            fieldsFaker = fieldsFakers[index];
        }

        fakers = fieldsFaker.fields;
        for (int i = 0; i < fakers.length; i++) {
            fieldFaker = fakers[i];
            value = fieldFaker.faker.geneValue();
            if (value != null) {
                fieldValue = fieldValuesTmp[i];
                fieldValue.name = fieldFaker.name;
                fieldValue.value = value;
                fieldValues.add(fieldValue);
            }
        }

        fieldValuesAndComputeFieldFakers.fieldValues = fieldValues;
        fieldValuesAndComputeFieldFakers.computeFields = fieldsFaker.computeFields;

        return fieldValuesAndComputeFieldFakers;
    }

    @Override
    public void destroy() throws Exception {
        for (FieldsFaker fieldsFaker : fieldsFakers) {
            for (ObjectFaker.FieldFaker field : fieldsFaker.fields) {
                field.faker.destroy();
            }
            for (ObjectFaker.ComputeFieldFaker field : fieldsFaker.computeFields) {
                field.faker.destroy();
            }
        }
    }

    @Override
    public boolean isUnionFaker() {
        return true;
    }

    public static class FieldValue implements Serializable {
        public String name;
        public Object value;
    }

    public static class FieldValuesAndComputeFieldFakers implements Serializable {
        public List<FieldValue> fieldValues;
        public ObjectFaker.ComputeFieldFaker[] computeFields;

    }

    public static class FieldsFaker implements Serializable {
        final ObjectFaker.FieldFaker[] fields;
        final ObjectFaker.ComputeFieldFaker[] computeFields;
        final int weight;
        int weightIndex = 0;

        public FieldsFaker(ObjectFaker.FieldFaker[] fields, int weight) {
            this.fields = Arrays.stream(fields).filter(x -> !x.faker.isComputeFaker()).toArray(ObjectFaker.FieldFaker[]::new);
            this.computeFields = Arrays.stream(fields).filter(x -> x.faker.isComputeFaker()).map(x -> new ObjectFaker.ComputeFieldFaker(x.name, (EvalFaker)x.faker)).toArray(ObjectFaker.ComputeFieldFaker[]::new);
            this.weight = weight;
        }
    }

}
