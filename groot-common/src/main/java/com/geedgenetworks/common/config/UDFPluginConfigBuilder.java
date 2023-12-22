package com.geedgenetworks.common.config;

import com.hazelcast.config.*;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.Preconditions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UDFPluginConfigBuilder extends AbstractConfigBuilder {
    private final InputStream in;
    private Properties properties = System.getProperties();

    public UDFPluginConfigBuilder() {
        this((UDFPluginConfigLocator) null);
    }

    public UDFPluginConfigBuilder(UDFPluginConfigLocator locator) {
        if (locator == null) {
            locator = new UDFPluginConfigLocator();
            locator.locateEverywhere();
        }
        this.in = locator.getIn();
    }
    public UDFPluginConfigBuilder(InputStream inputStream) {
        Preconditions.checkTrue(inputStream != null, "inputStream can't be null");
        this.in = inputStream;
    }
    public List<String> build() {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(this.in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            IOUtil.closeResource(this.in);
        }


        return lines;
    }

    public InputStream getIn() {
        return in;
    }

    public UDFPluginConfigBuilder setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }


}
