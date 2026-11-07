package com.alarm.eagle.cep.processor;

import java.io.Serializable;

/**
 * Rule discoverer factory interface.
 *
 * <p>Called on each Operator creation to produce a new Discoverer instance.
 * Must implement {@link Serializable}, as it needs to be serialized with the JobGraph to the JM.
 *
 * <p>业务含义：工厂必须可序列化（随 JobGraph 下发到 JM），
 * 由协调器在启动时调用以创建具体的发现器实例。
 *
 * @param <T> event type
 */
public interface PatternProcessorDiscovererFactory<T>
        extends Serializable {

    /**
     * Create a rule discoverer instance.
     *
     * @param userCodeClassLoader user code ClassLoader
     * @return new Discoverer instance
     */
    PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception;
}
