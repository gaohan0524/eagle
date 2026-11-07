package com.alarm.eagle.cep;

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;

/**
 * Wrapper class that exposes Pattern's protected constructor.
 *
 * <p>Flink's {@link Pattern} constructor is {@code protected},
 * callable by subclasses in any package via {@code super()}.
 * This wrapper exposes it as a public constructor for JSON → Pattern deserialization.
 *
 * <p>业务含义：{@link NodeSpec} 在把 JSON 还原成 Pattern 节点时，
 * 需要调用 Pattern 的受保护构造器；通过子类 {@code super()} 将其暴露为公共构造器，
 * 避免反射的脆弱性，是"绕过访问限制"的桥接工具类。
 */
public class PatternWrapper<T, F extends T> extends Pattern<T, F> {

    public PatternWrapper(
            String name,
            Pattern<T, ? extends T> previous,
            Quantifier.ConsumingStrategy consumingStrategy,
            AfterMatchSkipStrategy afterMatchSkipStrategy) {
        super(name, previous, consumingStrategy, afterMatchSkipStrategy);
    }
}
