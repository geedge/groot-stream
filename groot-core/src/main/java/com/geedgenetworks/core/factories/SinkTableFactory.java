package com.geedgenetworks.core.factories;

import com.geedgenetworks.core.connector.sink.SinkProvider;

public interface SinkTableFactory extends TableFactory {
    SinkProvider getSinkProvider(Context context);
}
