package com.geedgenetworks.connectors.mock.faker;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class ObjectFaker extends Faker<Map<String, Object>> {
    private final FieldFaker[] fields;
    private final ComputeFieldFaker[] computeFields;
    private final int initialCapacity;

    public ObjectFaker(FieldFaker[] fields) {
        this.fields = Arrays.stream(fields).filter(x -> !x.faker.isComputeFaker()).toArray(FieldFaker[]::new);
        this.computeFields = Arrays.stream(fields).filter(x -> x.faker.isComputeFaker()).map(x -> new ComputeFieldFaker(x.name, (EvalFaker)x.faker)).toArray(ComputeFieldFaker[]::new);
        this.initialCapacity = (int) (fields.length / 0.75);
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        for (FieldFaker field : fields) {
            field.faker.init(instanceCount, instanceIndex);
        }
        for (ComputeFieldFaker computeField : computeFields) {
            computeField.faker.init(instanceCount, instanceIndex);
        }
    }

    @Override
    public Map<String, Object> geneValue() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>(initialCapacity);
        FieldFaker fieldFaker;
        ComputeFieldFaker computeFieldFaker;
        UnionFaker.FieldValuesAndComputeFieldFakers fieldValuesAndComputeFieldFakers;
        Object value;
        int len = fields.length;
        for (int i = 0; i < len; i++) {
            fieldFaker = fields[i];
            value = fieldFaker.faker.geneValue();
            if(value == null){
                continue;
            }
            if (!fieldFaker.faker.isUnionFaker()) {
                map.put(fieldFaker.name, value);
            } else {
                fieldValuesAndComputeFieldFakers = (UnionFaker.FieldValuesAndComputeFieldFakers) value;
                for (UnionFaker.FieldValue fieldValue : fieldValuesAndComputeFieldFakers.fieldValues) {
                    map.put(fieldValue.name, fieldValue.value);
                }

                final int length = fieldValuesAndComputeFieldFakers.computeFields.length;
                for (int j = 0; j < length; j++) {
                    computeFieldFaker = fieldValuesAndComputeFieldFakers.computeFields[j];
                    value = computeFieldFaker.faker.geneValue(map);
                    if(value == null){
                        continue;
                    }
                    map.put(computeFieldFaker.name, value);
                }
            }
        }

        len = computeFields.length;
        for (int i = 0; i < len; i++) {
            computeFieldFaker = computeFields[i];
            value = computeFieldFaker.faker.geneValue(map);
            if(value == null){
                continue;
            }
            map.put(computeFieldFaker.name, value);
        }
        return map;
    }

    @Override
    public void destroy() throws Exception {
        for (FieldFaker field : fields) {
            field.faker.destroy();
        }
        for (ComputeFieldFaker computeField : computeFields) {
            computeField.faker.destroy();
        }
    }

    public static final class FieldFaker implements Serializable {
        public final String name;
        public final Faker<?> faker;

        public FieldFaker(String name, Faker<?> faker) {
            this.name = name;
            this.faker = faker;
        }
    }

    public static final class ComputeFieldFaker implements Serializable {
        public final String name;
        public final EvalFaker faker;

        public ComputeFieldFaker(String name, EvalFaker faker) {
            this.name = name;
            this.faker = faker;
        }
    }
}
