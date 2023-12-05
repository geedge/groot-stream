package com.geedgenetworks.connectors.ipfix.collector;

import com.geedgenetworks.connectors.ipfix.collector.util.IPFIXUtils;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.factories.FactoryUtil;
import com.geedgenetworks.core.factories.SourceTableFactory;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.cert.netsa.io.ipfix.InfoModel;
import org.cert.netsa.io.ipfix.SessionGroup;
import scala.xml.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


public class IPFixTableFactory implements SourceTableFactory {
    public static final String IDENTIFIER = "ipfix";

    @Override
    public SourceProvider getSourceProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        StructType physicalDataType = context.getPhysicalDataType();
        ReadableConfig config = context.getConfiguration();
        InfoModel model = InfoModel.getStandardInfoModel(true);
        SessionGroup sessionGroup = new SessionGroup(model, null, false);
        ArrayList<Node> elements = IPFIXUtils.getElements();
        for (Node element : elements) {
            model.importElements(element);
        }
        return new IPFixSourceProvider(physicalDataType, config, sessionGroup);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IPFixConnectorOptions.INITIAL_PORT);
        options.add(IPFixConnectorOptions.SERVICE_DISCOVERY_SERVICE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IPFixConnectorOptions.BUFFER_SIZE);
        options.add(IPFixConnectorOptions.MAX_ATTEMPTS);
        options.add(IPFixConnectorOptions.PORT_NUMBER);
        options.add(IPFixConnectorOptions.RECEIVE_BUFFER);
        options.add(IPFixConnectorOptions.SERVICE_DISCOVERY_REGISTER_MODE);
        options.add(IPFixConnectorOptions.SERVICE_DISCOVERY_HEALTH_CHECK_INTERVAL);
        return options;
    }
}
