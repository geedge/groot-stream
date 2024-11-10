package com.geedgenetworks.spi.source;

import com.geedgenetworks.spi.table.factory.TableFactory;

public interface SourceTableFactory extends TableFactory {
    SourceProvider getSourceProvider(Context context);
}
