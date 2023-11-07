package com.geedgenetworks.bootstrap.command;

import com.beust.jcommander.converters.IParameterSplitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ParameterSplitter implements IParameterSplitter {
    @Override
    public List<String> split(String value) {

        if (!value.contains(",")) {
            return Collections.singletonList(value);
        }

        List<String> result = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideBrackets = false;

        for (char c : value.toCharArray()) {
            if (c == '[') {
                insideBrackets = true;
            } else if (c == ']') {
                insideBrackets = false;
            }

            if (c == ',' && !insideBrackets) {
                result.add(currentToken.toString().trim());
                currentToken = new StringBuilder();
            } else {
                currentToken.append(c);
            }
        }

        if (currentToken.length() > 0) {
            result.add(currentToken.toString().trim());
        }

        return result;
    }
}
