package com.geedgenetworks.spi.sink;


import com.geedgenetworks.spi.table.factory.TableFactory;

public interface SinkTableFactory extends TableFactory {
    SinkProvider getSinkProvider(Context context);
}
