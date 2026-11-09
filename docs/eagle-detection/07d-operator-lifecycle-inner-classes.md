# eagle-detection 代码精读 · 07d 执行引擎：生命周期控制与内部类

> 本篇精读 `DynamicCepOperator` 的生命周期控制（PAUSE/RESUME/DRAIN/DELETE）与内部类（`RuleContext`、`RuleKey`、`UpdatePatternProcessorEvent`、`ContextFunctionImpl`、`TimerServiceImpl`）。
> **读完后应能回答**：暂停和排空有什么区别？生命周期控制真正在哪生效？内部上下文类分别做什么？

---

## 1. 生命周期控制全景

```
RuleLifecycleEvent(action, ruleId)
        │
        ▼
handleLifecycleEvent()
        │
        ├─ PAUSE  → pausedRuleIds.add(id)
        ├─ RESUME → paused/draining.remove(id)
        ├─ DRAIN  → drainingRuleIds.add(id)
        └─ DELETE → 关闭该 id 所有版本 + 标记清理
        │
        ▼
processElementForRule() 开头
        └─ shouldSkipRule(id) → 跳过暂停/排空规则
```

**核心设计：控制动作只改集合，不改状态；真正的拦截在事件处理入口。**

---

## 2. `handleLifecycleEvent()` 精读

```java
private void handleLifecycleEvent(RuleLifecycleEvent event) {
    String ruleId = event.getRuleId();

    switch (event.getAction()) {
        case PAUSE:
            pausedRuleIds.add(ruleId);
            LOG.info("[CEP] Rule paused: " + ruleId);
            break;

        case RESUME:
            pausedRuleIds.remove(ruleId);
            drainingRuleIds.remove(ruleId);
            LOG.info("[CEP] Rule resumed: " + ruleId);
            break;

        case DRAIN:
            drainingRuleIds.add(ruleId);
            LOG.info("[CEP] Rule draining: " + ruleId);
            break;

        case DELETE:
            pausedRuleIds.remove(ruleId);
            drainingRuleIds.remove(ruleId);

            List<RuleKey> toRemove = new ArrayList<>();
            for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry : ruleContexts.entrySet()) {
                if (entry.getKey().id.equals(ruleId)) {
                    pendingStateClear.add(entry.getKey());
                    closeRuleContext(entry.getValue());
                    toRemove.add(entry.getKey());
                }
            }
            for (RuleKey key : toRemove) ruleContexts.remove(key);

            LOG.info("[CEP] Rule cleaned up: " + ruleId + " (" + toRemove.size() + " version(s))");
            break;
    }
}
```

### 2.1 四种动作的语义

| 动作 | 集合变化 | NFA/状态 | 是否处理新事件 | 可恢复 |
|---|---|---|---|---|
| `PAUSE` | 加入 `pausedRuleIds` | 完整保留 | ❌ | ✅ `RESUME` |
| `RESUME` | 从两个集合移除 | 保留 | ✅ | — |
| `DRAIN` | 加入 `drainingRuleIds` | 保留 | ❌ | ✅ `RESUME` |
| `DELETE` | 两个集合移除 | 关闭 + 标记清理 | ❌ | ❌（需重新创建） |

**PAUSE 与 DRAIN 的实现差异**：当前代码里二者行为几乎相同（都是跳过新事件、保留状态）。语义上的区别是：

- **PAUSE**：人为暂停，预期之后恢复；
- **DRAIN**：准备下线，预期让已有部分匹配自然超时后再删除。

但当前 `shouldSkipRule` 对两者一视同仁，**DRAIN 并不会"等待部分匹配超时"**——它只是停止接收新事件。已有部分匹配能否超时，取决于是否还有定时器触发（EventTime 下若水位不再推进，就不会触发）。

> ⚠️ 这是一个实现与语义不完全匹配的点，学习时需注意。

### 2.2 DELETE 的处理

- 遍历 `ruleContexts`，删除**该 id 下所有版本**（因为按 id 匹配，不区分 version）；
- 先收集 `toRemove` 再删除，避免遍历中修改 Map；
- 状态清理同样走 `pendingStateClear`。

### 2.3 未接通的现实

`RuleLifecycleEvent` 目前**没有下发入口**：

- Coordinator 的 `handleEventFromOperator` 为空（TM → JM 无事件）；
- 没有从 REST API/外部系统构造并发送 `RuleLifecycleEvent` 的代码。

因此这套逻辑是"已实现但未启用"。学习时理解设计意图即可，不要以为线上能通过 API 暂停规则【待确认业务】。

---

## 3. `shouldSkipRule()`

```java
private boolean shouldSkipRule(String ruleId) {
    return pausedRuleIds.contains(ruleId) || drainingRuleIds.contains(ruleId);
}
```

调用位置：`processElementForRule` 的第一行。

```java
private void processElementForRule(StreamRecord<IN> element, RuleKey ruleKey,
                                   RuleContext<IN, OUT> rc) throws Exception {
    if (shouldSkipRule(ruleKey.id)) {
        return;
    }
    ...
}
```

**这是生命周期的真正生效点。**

**注意**：只拦截**新事件**；定时器回调 `onEventTime` 仍会遍历所有规则并 `processTimedEvents`，所以暂停规则的已缓冲事件仍可能被处理（取决于水位是否推进）。如果希望"暂停后完全不处理"，需要在 `processTimedEvents` 也加判断——当前没有。

---

## 4. 内部类逐个精读

### 4.1 `RuleContext`

```java
private static class RuleContext<IN, OUT> {
    final PatternProcessor<IN> processor;
    final NFA<IN> nfa;
    final PatternProcessFunction<IN, OUT> userFunction;
    final ValueState<NFAState> computationStates;
    final MapState<Long, List<IN>> elementQueueState;
    final InternalTimerService<VoidNamespace> timerService;
    final TimerService cepTimerService;
    final Counter numLateRecordsDropped;
    final Counter patternMatchedTimes;
}
```

- **static**：不持有外部类引用，避免序列化/内存问题；
- **所有字段 final**：上下文一旦创建就不再替换（规则更新是"换整个 context"，不是改字段）；
- 已在 07a 详述，此处不重复。

### 4.2 `RuleKey`

```java
static class RuleKey {
    final String id;
    final int version;

    @Override public boolean equals(Object o) {
        if (!(o instanceof RuleKey)) return false;
        RuleKey r = (RuleKey) o;
        return version == r.version && id.equals(r.id);
    }

    @Override public int hashCode() {
        return 31 * id.hashCode() + version;
    }

    @Override public String toString() {
        return id + "-v" + version;
    }
}
```

**要点：**

1. **作为 `Map` 的 key**，`equals`/`hashCode` 必须正确实现——这里基于 `id + version`。
2. 版本进入身份，是"升版即新规则"的基础。
3. `toString` 用于日志，格式 `id-vN`。

**易错点**：如果误把 `equals` 写成只比较 id，升版时新规则会命中旧 key，导致状态混用。

### 4.3 `UpdatePatternProcessorEvent`

```java
public static class UpdatePatternProcessorEvent<IN> implements OperatorEvent {
    private static final long serialVersionUID = 1L;
    private final List<PatternProcessor<IN>> processors;

    public UpdatePatternProcessorEvent(List<PatternProcessor<IN>> processors) {
        this.processors = new ArrayList<>(processors);
    }

    public List<PatternProcessor<IN>> getProcessors() { return processors; }
}
```

- `OperatorEvent`：JM → TM 传输；
- 构造时 `new ArrayList<>` 防御性拷贝；
- 放在算子类内部而非独立文件，是历史实现选择。

### 4.4 `ContextFunctionImpl`

```java
private class ContextFunctionImpl implements PatternProcessFunction.Context {
    private Long timestamp;

    @Override
    public <X> void output(OutputTag<X> outputTag, X value) {
        StreamRecord<X> record = isProcessingTime
                ? new StreamRecord<>(value)
                : new StreamRecord<>(value, timestamp());
        output.collect(outputTag, record);
    }

    void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public long timestamp() { return timestamp; }

    @Override
    public long currentProcessingTime() {
        return ruleContexts.isEmpty() ? 0L
                : ruleContexts.values().iterator().next().timerService.currentProcessingTime();
    }
}
```

**作用**：交给用户 `PatternProcessFunction` 的上下文。

| 方法 | 用途 |
|---|---|
| `output(tag, value)` | 侧输出（如把超时/异常事件分流） |
| `timestamp()` | 当前匹配时间戳 |
| `currentProcessingTime()` | 当前处理时间 |

**细节：**

- `isProcessingTime` 为 true 时，输出记录不带时间戳（`new StreamRecord<>(value)`），因为处理时间语义下不需要。
- `currentProcessingTime()` 取"第一个规则"的 TimerService 时钟——所有规则共享物理时钟，取任意一个都可以；但 `ruleContexts` 为空时返回 0，需注意。

### 4.5 `TimerServiceImpl`

```java
private static class TimerServiceImpl implements TimerService {
    private final InternalTimerService<VoidNamespace> timerService;

    TimerServiceImpl(InternalTimerService<VoidNamespace> timerService) {
        this.timerService = timerService;
    }

    @Override
    public long currentProcessingTime() {
        return timerService.currentProcessingTime();
    }
}
```

**作用**：适配器。`org.apache.flink.cep.time.TimerService` 只有一个方法 `currentProcessingTime()`，NFA 用它读取时钟；这里把 Flink 的 `InternalTimerService` 包装成该接口。

**为什么需要适配？** NFA 的 `process` 方法签名要求 CEP 的 `TimerService`，而算子持有的是 Flink 的 `InternalTimerService`，两者类型不同。

---

## 5. 生命周期与热更新的关系

| 事件 | 作用对象 | 是否改变 `ruleContexts` | 是否改状态 |
|---|---|---|---|
| 热更新（全量/增量） | 规则集合 | ✅ | 通过 `pendingStateClear` 延迟 |
| `PAUSE` / `DRAIN` | 单条规则 | ❌ | ❌ |
| `RESUME` | 单条规则 | ❌ | ❌ |
| `DELETE` | 单条规则所有版本 | ✅ | ✅ 延迟清理 |

**关键区分：**

- 热更新是"规则定义变化"；
- 生命周期是"规则运行状态变化"，不改变定义。

---

## 6. 设计要点与陷阱

| 点 | 说明 |
|---|---|
| 控制与执行分离 | 集合只标记，拦截在 `shouldSkipRule` |
| PAUSE 与 DRAIN 实现趋同 | 当前行为几乎一致 |
| DRAIN 不真正排空 | 只停新事件，不保证已有匹配超时 |
| DELETE 按 id 删所有版本 | 版本维度被忽略 |
| 生命周期事件未接通 | 无下发入口 |
| `shouldSkipRule` 只拦新事件 | 定时器仍可能处理缓冲事件 |
| `ContextFunctionImpl` 的时间戳可能为 null | 仅在 `setTimestamp` 后有效 |

---

## 7. 本篇小结

### 7.1 一句话总结

生命周期控制用三个集合（暂停/排空/待清理）实现"软控制"：PAUSE/DRAIN 只标记不销毁，DELETE 关闭上下文并延迟清理状态；真正的拦截在 `processElementForRule` 开头的 `shouldSkipRule`。内部类则分别承担单规则上下文、规则身份、全量事件、用户上下文与时钟适配。

### 7.2 自测问题

1. PAUSE 与 DRAIN 的实现差异是什么？语义差异是什么？
2. `shouldSkipRule` 在哪调用？为什么它是"真正生效点"？
3. DELETE 为什么删除该 id 下所有版本？
4. 生命周期控制为什么不直接修改 `ruleContexts` 的状态？
5. `ContextFunctionImpl.output` 在两种时间语义下有何不同？
6. `TimerServiceImpl` 为什么需要存在？
7. `RuleKey.equals` 如果只比较 id 会发生什么？

### 7.3 下一篇

→ `08-end-to-end-and-testing.md`：把 01–07 串成完整链路，结合 `CepTestDemo` 做端到端分析。
