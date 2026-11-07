package com.alarm.eagle.cep.processor;

import java.util.List;

/**
 * Rule manager interface.
 *
 * <p>In Coordinator mode, the JM-side Coordinator implements this interface,
 * receives update callbacks from the Discoverer, and distributes them to all TMs via OperatorEvent.
 *
 * <p>业务含义：在协调器模式下，JM 侧 {@code DynamicCepCoordinator} 实现此接口，
 * 作为发现器的回调接收者，接收最新规则列表并负责下发给所有 TM。
 *
 * @param <T> event type
 */
public interface PatternProcessorManager<T> {

    /**
     * ★ Rule update callback.
     *
     * @param patternProcessors the latest complete rule list
     */
    void onPatternProcessorsUpdated(
            List<PatternProcessor<T>> patternProcessors);
}
