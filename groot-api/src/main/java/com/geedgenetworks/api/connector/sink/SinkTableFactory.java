package com.geedgenetworks.api.connector.sink;


import com.geedgenetworks.api.factory.ConnectorFactory;

public interface SinkTableFactory extends ConnectorFactory {
    SinkProvider getSinkProvider(Context context);
}
