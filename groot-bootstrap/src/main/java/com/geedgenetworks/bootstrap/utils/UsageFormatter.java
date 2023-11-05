package com.geedgenetworks.bootstrap.utils;

import com.beust.jcommander.*;

import java.util.List;

public class UsageFormatter extends UnixStyleUsageFormatter {
    private static final int INDENT = 3;

    public UsageFormatter(JCommander commander) {
        super(commander);
    }

    @Override
    public void appendAllParametersDetails(
            StringBuilder out,
            int indentCount,
            String indent,
            List<ParameterDescription> sortedParameters) {
        if (sortedParameters.size() > 0) {
            out.append(indent).append("  Options:\n");
        }

        // Calculate prefix indent
        int prefixIndent = 0;

        for (ParameterDescription pd : sortedParameters) {
            WrappedParameter parameter = pd.getParameter();
            String prefix = (parameter.required() ? "* " : "  ") + pd.getNames();

            if (prefix.length() > prefixIndent) {
                prefixIndent = prefix.length();
            }
        }

        // Append parameters
        for (ParameterDescription pd : sortedParameters) {
            WrappedParameter parameter = pd.getParameter();

            String prefix = (parameter.required() ? "* " : "  ") + pd.getNames();
            out.append(indent)
                    .append("  ")
                    .append(prefix)
                    .append(s(prefixIndent - prefix.length()))
                    .append(" ");
            final int initialLinePrefixLength = indent.length() + prefixIndent + 3;

            // Generate description
            String description = pd.getDescription();
            Object def = pd.getDefault();

            if (pd.isDynamicParameter()) {
                String syntax =
                        "(syntax: "
                                + parameter.names()[0]
                                + "key"
                                + parameter.getAssignment()
                                + "value)";
                description += (description.length() == 0 ? "" : " ") + syntax;
            }
            Class<?> type = pd.getParameterized().getType();
            if (def != null && !pd.isHelp()) {
                String displayText = type.isEnum() ? def.toString().toLowerCase() : def.toString();
                String displayedDef =
                        Strings.isStringEmpty(displayText) ? "<empty string>" : displayText;
                String defaultText =
                        "(default: " + (parameter.password() ? "********" : displayedDef) + ")";
                description += (description.length() == 0 ? "" : " ") + defaultText;
            }
            wrapDescription(
                    out, indentCount + prefixIndent - INDENT, initialLinePrefixLength, description);
            out.append("\n");
        }
    }


}
