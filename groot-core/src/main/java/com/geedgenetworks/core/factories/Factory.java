package com.geedgenetworks.core.factories;

import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

/**
 * 用于注册source、 sink、format factory的基础接口，Factory从key、value配置创建实例。
 * 可用的factory列表使用java Service Provider Interfaces (SPI)发现，把实现类添加到META_INF/services/com.geedgenetworks.core.factories.Factory即可。
 * factory实现和配置参考flink sql
 */
public interface Factory {
    // 返回Factory的唯一标识
    String factoryIdentifier();

    // 必须配置的参数
    Set<ConfigOption<?>> requiredOptions();

    // 可选的参数
    Set<ConfigOption<?>> optionalOptions();
}
