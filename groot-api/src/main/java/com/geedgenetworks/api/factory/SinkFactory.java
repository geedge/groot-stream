package com.geedgenetworks.api.factory;


import com.geedgenetworks.api.connector.sink.SinkProvider;

public interface SinkFactory extends ConnectorFactory {
    SinkProvider getSinkProvider(Context context);
}
