package com.geedgenetworks.core.factories;

import com.geedgenetworks.core.connector.source.SourceProvider;

public interface SourceTableFactory extends TableFactory {
    SourceProvider getSourceProvider(Context context);
}
