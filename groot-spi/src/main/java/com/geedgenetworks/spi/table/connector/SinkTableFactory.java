package com.geedgenetworks.spi.table.connector;


import com.geedgenetworks.spi.table.factory.TableFactory;

public interface SinkTableFactory extends TableFactory {
    SinkProvider getSinkProvider(Context context);
}
