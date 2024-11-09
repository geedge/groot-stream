package com.geedgenetworks.spi.table.connector;

import com.geedgenetworks.spi.table.factory.TableFactory;

public interface SourceTableFactory extends TableFactory {
    SourceProvider getSourceProvider(Context context);
}
