package com.geedgenetworks.api.connector.source;

import com.geedgenetworks.api.factory.ConnectorFactory;

public interface SourceTableFactory extends ConnectorFactory {
    SourceProvider getSourceProvider(Context context);
}
