package com.geedgenetworks.api.factory;

import com.geedgenetworks.api.connector.source.SourceProvider;

public interface SourceFactory extends ConnectorFactory {
    SourceProvider getSourceProvider(Context context);
}
