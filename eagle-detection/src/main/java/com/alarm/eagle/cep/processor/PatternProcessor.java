package com.alarm.eagle.cep.processor;

import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;

import java.io.Serializable;

/**
 * Complete abstraction of a single CEP rule.
 *
 * <p>Each rule is uniquely identified by id + version. Contains:
 * <ul>
 *   <li>Rule identifier (id + version)</li>
 *   <li>Pattern definition (as JSON string, deserialized at runtime)</li>
 *   <li>Callback function on match (PatternProcessFunction)</li>
 * </ul>
 *
 * <p>在实时流中的定位：一条 CEP 规则的"可序列化抽象"。规则以 JSON 字符串承载，
 * 通过 OperatorEvent 在 JM→TM 之间传输，运行时才反序列化成 Flink {@link Pattern}。
 *
 * @param <T> event type（本模块为 {@code Transaction} 交易事件）
 */
public interface PatternProcessor<T> extends Serializable {

    /** Unique rule identifier */
    String getId();

    /** Rule version number (incrementing) */
    int getVersion();

    /**
     * ★ Get the Pattern object.
     * Deserialized from JSON on every call to prevent internal Pattern state from being shared across versions.
     *
     * @param classLoader user code ClassLoader, for loading Class-type conditions
     * @return 从规则 JSON 反序列化出的 Flink Pattern（每次调用都全新构建，避免跨版本状态污染）
     */
    Pattern<T, ?> getPattern(ClassLoader classLoader);

    /** Callback function invoked on match */
    PatternProcessFunction<T, ?> getPatternProcessFunction();

    /** Pattern JSON string */
    String getPatternJSON();
}
