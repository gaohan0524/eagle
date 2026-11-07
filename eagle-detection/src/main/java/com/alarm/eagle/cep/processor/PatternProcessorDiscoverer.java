package com.alarm.eagle.cep.processor;

import java.io.Closeable;

/**
 * Rule discoverer interface — responsible for detecting and fetching latest rules from external data sources.
 *
 * <p>业务含义：规则发现器的抽象，屏蔽规则来源差异（HTTP API / JDBC / 内存），
 * 由具体实现周期性检测规则变更并回调 {@link PatternProcessorManager}。
 *
 * @param <T> event type
 */
public interface PatternProcessorDiscoverer<T> extends Closeable {

    /**
     * Start rule discovery and register a callback manager.
     *
     * @param manager callback manager that receives rule updates
     */
    void discoverPatternProcessorUpdates(PatternProcessorManager<T> manager);
}
