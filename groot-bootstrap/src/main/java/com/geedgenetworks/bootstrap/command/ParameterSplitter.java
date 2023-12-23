
package com.geedgenetworks.bootstrap.command;

import com.beust.jcommander.converters.IParameterSplitter;

import java.util.ArrayList;
import java.util.List;

public class ParameterSplitter implements IParameterSplitter {

    @Override
    public List<String> split(String value) {

        List<String> result = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideBrackets = false;
        boolean insideQuotes = false;

        for (char c : value.toCharArray()) {

            if (c == '[') {
                insideBrackets = true;
            } else if (c == ']') {
                insideBrackets = false;
            } else if (c == '"') {
                insideQuotes = !insideQuotes;
            }

            if (c == ',' && !insideQuotes && !insideBrackets) {
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
