package com.geedgenetworks.common.persistence;

import com.geedgenetworks.common.exception.IMapStorageException;

import java.util.Map;

public interface IMapStorageFactory {
    String factoryIdentifier();

    IMapStorage create(Map<String, Object> configuration) throws IMapStorageException;
}
