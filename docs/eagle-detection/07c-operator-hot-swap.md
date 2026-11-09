# eagle-detection 代码精读 · 07c 执行引擎：规则热更新

> 本篇聚焦 `DynamicCepOperator` 的热更新链路：三类 OperatorEvent、全量替换的三种分支、`createRuleContext` 的 7 步、原子替换与延迟清理。
> **读完后应能回答**：规则如何在不重启作业的前提下被增删改？为什么升版后状态从零开始？

---

## 1. 热更新全景

```
JM Coordinator
   │
   ├─ 日常变更 → PatternProcessorChangeEvent（增量）
   │                └ handleIncrementalChange：删 → 改 → 增
   │
   ├─ TM 重启/就绪 → UpdatePatternProcessorEvent（全量）
   │                └ handleFullUpdate → handleUpdatePatternProcessorEvent
   │
   └─ 生命周期 → RuleLifecycleEvent
                    └ handleLifecycleEvent（见 07d）

TM DynamicCepOperator
   └─ handleOperatorEvent(event)   ← RPC 线程调用，无 key 上下文
```

---

## 2. `handleOperatorEvent()`：总入口

```java
@Override
public void handleOperatorEvent(OperatorEvent event) {
    try {
        if (event instanceof UpdatePatternProcessorEvent) {
            handleFullUpdate((UpdatePatternProcessorEvent<IN>) event);
        } else if (event instanceof PatternProcessorChangeEvent) {
            handleIncrementalChange((PatternProcessorChangeEvent<IN>) event);
        } else if (event instanceof RuleLifecycleEvent) {
            handleLifecycleEvent((RuleLifecycleEvent) event);
        } else {
            LOG.warn("Received unexpected operator event: {}", event);
        }
    } catch (Exception e) {
        LOG.error("[CEP] ★★★ handleOperatorEvent Exception!★★★");
        e.printStackTrace(System.err);
        throw new RuntimeException("Failed to update pattern processors.", e);
    }
}
```

**关键约束：本方法由 RPC 线程调用。**

这带来三个后果：

| 后果 | 说明 |
|---|---|
| 无 key 上下文 | 不能直接 `ValueState.clear()` → 只能登记 `pendingStateClear` |
| 与算子线程并发 | `ruleContexts` 的替换要么整体引用赋值，要么需同步 |
| 异常会抛回 Flink | 整个规则更新批次失败，可能导致作业异常 |

**为什么异常要重新抛出？**
规则更新失败是严重问题（TM 与 JM 状态不一致），应让 Flink 感知并触发恢复，而不是静默吞掉。

---

## 3. 全量更新：`handleFullUpdate`

```java
private void handleFullUpdate(UpdatePatternProcessorEvent<IN> event) throws Exception {
    List<PatternProcessor<IN>> processors = event.getProcessors();

    if (processors == null || processors.isEmpty()) {
        LOG.info("[CEP] Received empty rule list, clearing all NFAs");
        for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry : ruleContexts.entrySet()) {
            pendingStateClear.add(entry.getKey());
            closeRuleContext(entry.getValue());
        }
        ruleContexts.clear();
        return;
    }

    handleUpdatePatternProcessorEvent(event);
}
```

**空列表 = 清空所有规则。**
注意这里是 `ruleContexts.clear()`（原地清空），不是原子替换；因为已经是"全部删除"，没有新旧并存问题。

---

## 4. 全量替换核心：`handleUpdatePatternProcessorEvent`

这是整个类最核心的方法。

```java
private void handleUpdatePatternProcessorEvent(UpdatePatternProcessorEvent<IN> event) throws Exception {
    List<PatternProcessor<IN>> processors = event.getProcessors();

    // Case A: 空列表 → 清空
    if (processors == null || processors.isEmpty()) { ... return; }

    // Case B: 构建新 Map
    Map<RuleKey, RuleContext<IN, OUT>> newContexts = new HashMap<>(processors.size());

    for (PatternProcessor<IN> processor : processors) {
        RuleKey ruleKey = new RuleKey(processor.getId(), processor.getVersion());

        // ① 同 id + version：复用
        RuleContext<IN, OUT> existing = ruleContexts.get(ruleKey);
        if (existing != null) {
            newContexts.put(ruleKey, existing);
            LOG.info("[CEP] Rule kept: id=" + ruleKey.id + ", version=" + ruleKey.version);
            continue;
        }

        // ② 找同 id 的旧版本，准备清理
        RuleKey oldKey = findExistingRuleById(processor.getId());
        if (oldKey != null) {
            RuleContext<IN, OUT> oldRc = ruleContexts.get(oldKey);
            pendingStateClear.add(oldKey);
            closeRuleContext(oldRc);
            LOG.info("[CEP] Rule version update: id=" + ruleKey.id
                    + ", v" + oldKey.version + " → v" + ruleKey.version);
        } else {
            LOG.info("[CEP] New rule online: id=" + ruleKey.id + ", version=" + ruleKey.version);
        }

        // ③ 创建新 RuleContext
        RuleContext<IN, OUT> rc = createRuleContext(processor, ruleKey);
        newContexts.put(ruleKey, rc);
    }

    // ④ 清理新列表中不存在的旧规则
    for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry : ruleContexts.entrySet()) {
        if (!newContexts.containsKey(entry.getKey())) {
            pendingStateClear.add(entry.getKey());
            closeRuleContext(entry.getValue());
        }
    }

    // ⑤ 原子替换
    this.ruleContexts = newContexts;
}
```

### 4.1 三种分支

| 情况 | 判定 | 处理 | 状态影响 |
|---|---|---|---|
| **未变更** | `ruleContexts` 中已有同 `id+version` | 直接复用 `RuleContext` | 部分匹配**完全保留**，不中断 |
| **版本升级** | 同 id，version 不同 | 关旧版 + 建新版 | 新版状态名不同 → **从零开始** |
| **全新** | 找不到同 id | 建新版 | 从零开始 |
| **下线** | 旧有新无 | 关旧版 + 标记清理 | 状态延迟清理 |

### 4.2 为什么"同 id+version 复用"很重要

如果每次全量下发都重建所有规则：

- 每次规则更新（哪怕只改一条）都会导致**所有规则的部分匹配丢失**；
- 高并发流下，正在进行的 CEP 匹配会被频繁打断。

复用逻辑保证：**未变更规则完全无感**。

### 4.3 为什么升版后状态从零开始

```java
String stateSuffix = ruleKey.id + "-" + ruleKey.version;   // 不同版本 → 不同状态名
```

新版 `RuleContext` 创建时：

```java
ValueState<NFAState> computationStates = initContext.getKeyedStateStore()
        .getState(new ValueStateDescriptor<>(
                NFA_STATE_PREFIX + "-" + stateSuffix, new NFAStateSerializer()));
```

读到的是**全新的状态名**，因此 `value()` 为 null → `getNFAState` 返回初始状态。

**这是设计意图**：规则变了，旧的匹配进度不应继续沿用（否则可能用旧逻辑的中间状态走新逻辑）。

### 4.4 原子替换

```java
this.ruleContexts = newContexts;
```

- 一行赋值，没有中间态；
- 正在执行的 `processElement` 持有旧 Map 引用（Java 引用语义），不会被并发修改；
- 后续事件立即使用新 Map。

**注意**：`ruleContexts` 不是 `volatile`。RPC 线程写、算子线程读，理论上存在可见性问题。实际 Flink 的 OperatorEvent 处理与 `processElement` 在同一 task 线程（事件通过 mailbox 串行化），因此安全。

### 4.5 下线规则的状态清理

```java
pendingStateClear.add(entry.getKey());
closeRuleContext(entry.getValue());
```

- `closeRuleContext`：关闭 NFA 与用户函数（内存对象）；
- `pendingStateClear`：等待下一个 `processElement` 清理 keyed state。

**为什么不在当时清理？**
RPC 线程无 key 上下文（`ValueState.clear()` 需要 key）。这是本篇最重要的约束。

---

## 5. 增量更新：`handleIncrementalChange`

```java
private void handleIncrementalChange(PatternProcessorChangeEvent<IN> change) throws Exception {
    // ① 删除
    for (PatternProcessorChangeEvent.RuleIdentifier removed : change.getRemoved()) {
        RuleKey key = new RuleKey(removed.getId(), removed.getVersion());
        RuleContext<IN, OUT> oldRc = ruleContexts.get(key);
        if (oldRc != null) {
            pendingStateClear.add(key);
            closeRuleContext(oldRc);
            ruleContexts.remove(key);     // ← 原地修改
        }
    }

    // ② 更新（版本升级）
    for (PatternProcessor<IN> processor : change.getUpdated()) {
        RuleKey oldKey = findExistingRuleById(processor.getId());
        if (oldKey != null) {
            RuleContext<IN, OUT> oldRc = ruleContexts.get(oldKey);
            pendingStateClear.add(oldKey);
            closeRuleContext(oldRc);
            ruleContexts.remove(oldKey);
        }
        RuleKey newKey = new RuleKey(processor.getId(), processor.getVersion());
        ruleContexts.put(newKey, createRuleContext(processor, newKey));
    }

    // ③ 新增
    for (PatternProcessor<IN> processor : change.getAdded()) {
        RuleKey newKey = new RuleKey(processor.getId(), processor.getVersion());
        ruleContexts.put(newKey, createRuleContext(processor, newKey));
    }
}
```

### 5.1 与全量替换的差异

| 维度 | 全量 | 增量 |
|---|---|---|
| 遍历范围 | 所有规则 | 只有变更规则 |
| 未变更规则 | 复用 | **完全不动** |
| Map 操作 | 建新 Map + 原子替换 | **原地增删** |
| 适用场景 | TM 重启对齐 | 日常变更 |

### 5.2 为什么增量用原地修改

增量只涉及少数规则，原地修改更简单，且当前事件处理是串行的（mailbox），无并发遍历问题。

> ⚠️ 但要注意：如果 `handleIncrementalChange` 正在遍历时又有事件到达，Flink 的 mailbox 保证二者串行，所以不会 ConcurrentModificationException。

### 5.3 处理顺序：删 → 改 → 增

同一轮变更中，如果某规则既在 removed 又在 added：

- 先删（移除旧版本、标记清理）；
- 再增（创建新版本）。

保证不会出现"旧版本还在、新版本又建"的重复状态。

---

## 6. `createRuleContext()`：单规则上下文构建（7 步）

```java
private RuleContext<IN, OUT> createRuleContext(PatternProcessor<IN> processor, RuleKey ruleKey) throws Exception {
    String stateSuffix = ruleKey.id + "-" + ruleKey.version;

    // 1) 初始化用户函数
    PatternProcessFunction<IN, OUT> function =
            (PatternProcessFunction<IN, OUT>) processor.getPatternProcessFunction();
    if (function != null) {
        FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext());
        FunctionUtils.openFunction(function, new Configuration());
    }

    // 2) JSON → Pattern → NFA
    NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(
            processor.getPattern(getUserCodeClassloader()),
            function instanceof TimedOutPartialMatchHandler);
    NFA<IN> nfa = nfaFactory.createNFA();
    nfa.open(getRuntimeContext(), new Configuration());

    // 3) 独立 ValueState(NFAState)
    ValueState<NFAState> computationStates = initContext.getKeyedStateStore()
            .getState(new ValueStateDescriptor<>(
                    NFA_STATE_PREFIX + "-" + stateSuffix, new NFAStateSerializer()));

    // 4) 独立 MapState(eventQueue)
    MapState<Long, List<IN>> elementQueueState = initContext.getKeyedStateStore()
            .getMapState(new MapStateDescriptor<>(
                    EVENT_QUEUE_PREFIX + "-" + stateSuffix,
                    LongSerializer.INSTANCE,
                    new ListSerializer<>(inputSerializer)));

    // 5) 独立 InternalTimerService
    InternalTimerService<VoidNamespace> timerService = getInternalTimerService(
            TIMER_SERVICE_PREFIX + "-" + stateSuffix,
            VoidNamespaceSerializer.INSTANCE, this);

    // 6) CEP TimerService 包装
    TimerService cepTimerService = new TimerServiceImpl(timerService);

    // 7) 独立 Metrics
    Counter lateCounter = metrics.counter("numLateRecordsDropped-" + stateSuffix);
    Counter matchCounter = metrics.counter("patternMatchedTimes-" + stateSuffix);

    return new RuleContext<>(processor, nfa, function,
            computationStates, elementQueueState,
            timerService, cepTimerService, lateCounter, matchCounter);
}
```

### 6.1 逐步说明

| 步 | 动作 | 关键点 |
|---|---|---|
| 1 | 用户函数注入上下文并 `open()` | `function` 为 null 时跳过 |
| 2 | JSON→Pattern→NFA 编译 | 第二个参数决定是否启用超时处理 |
| 3 | 建 `ValueState<NFAState>` | 状态名带版本后缀 |
| 4 | 建 `MapState<Long, List<IN>>` | 需要 `inputSerializer` |
| 5 | 建独立 `InternalTimerService` | 名字带后缀，事件回调路由到 `this` |
| 6 | 包装成 CEP `TimerService` | NFA 只需要读时间 |
| 7 | 建独立 Metrics | 指标名带后缀 |

### 6.2 `NFACompiler.compileFactory` 的第二个参数

```java
function instanceof TimedOutPartialMatchHandler
```

- `true`：NFA 会保留超时部分匹配所需的信息；
- `false`：优化掉超时相关信息，减少状态。

**因此**：只有用户函数实现了 `TimedOutPartialMatchHandler`，超时处理才生效。`DetectionAlertFunction` 没实现 → 超时部分匹配不保留/不回调。

### 6.3 每规则独立 TimerService 的意义

```java
getInternalTimerService(TIMER_SERVICE_PREFIX + "-" + stateSuffix, ...)
```

- 不同规则注册的定时器在**不同命名空间**；
- 规则 A 的窗口超时不会触发规则 B 的回调逻辑（虽然 `onEventTime` 会遍历所有规则，但每条规则只处理自己的队列）；
- 时间线独立：规则 A 的 within 5min 与规则 B 的 within 30min 互不影响。

### 6.4 异常的影响

`createRuleContext` 任一步失败（JSON 解析、NFA 编译、状态创建）都会抛异常：

- 在 `handleUpdatePatternProcessorEvent` 中 → 整个全量更新失败；
- 在 `handleIncrementalChange` 中 → 整个增量更新失败，可能导致部分规则已删但新规则未建。

**当前没有按规则粒度的容错**（例如"跳过坏规则、保留其他规则"）。这是一个健壮性改进点。

---

## 7. 辅助方法

### 7.1 `findExistingRuleById`

```java
private RuleKey findExistingRuleById(String id) {
    for (RuleKey key : ruleContexts.keySet()) {
        if (key.id.equals(id)) return key;
    }
    return null;
}
```

- 只按 id 找，不关心版本；
- O(n) 遍历，规则数量通常较小，可接受。

### 7.2 `closeRuleContext`

```java
private void closeRuleContext(RuleContext<IN, OUT> rc) {
    try {
        if (rc.nfa != null) rc.nfa.close();
    } catch (Exception e) { LOG.warn("Error closing NFA", e); }

    try {
        if (rc.userFunction != null) FunctionUtils.closeFunction(rc.userFunction);
    } catch (Exception e) { LOG.warn("Error closing user function", e); }
}
```

- 只关内存对象，不动 keyed state；
- 异常被吞，避免单条规则关闭失败导致整个更新回滚。

---

## 8. `UpdatePatternProcessorEvent`

```java
public static class UpdatePatternProcessorEvent<IN> implements OperatorEvent {
    private final List<PatternProcessor<IN>> processors;
    public UpdatePatternProcessorEvent(List<PatternProcessor<IN>> processors) {
        this.processors = new ArrayList<>(processors);
    }
}
```

- 全量事件内嵌在算子类里（而非独立文件）；
- 构造时 copy，防止外部修改；
- 与增量事件的区别：**没有 added/removed/updated 分类**，接收方按"全量对齐"处理。

---

## 9. 热更新完整判断表

| 输入 | 使用的事件 | 处理方法 | Map 操作 |
|---|---|---|---|
| 首次启动（TM 就绪） | 全量 | `handleUpdatePatternProcessorEvent` | 建新 Map + 原子替换 |
| TM 重启恢复 | 全量 | 同上 | 同上 |
| 新增规则 | 增量 | `handleIncrementalChange` | `put` |
| 删除规则 | 增量 | 同上 | `remove` |
| 版本升级 | 增量 | 同上 | 先 `remove` 旧版，再 `put` 新版 |
| 规则列表清空 | 全量空列表 | `handleFullUpdate` | `clear` |

---

## 10. 设计要点与陷阱

| 点 | 说明 |
|---|---|
| 同 id+version 复用 | 未变更规则无感，避免匹配中断 |
| 升版从零 | 版本进入状态名 |
| 原子替换 | 全量更新无中间态 |
| 延迟清理 | RPC 线程无 key 上下文 |
| 增量原地修改 | 依赖 mailbox 串行 |
| 按规则容错缺失 | 一条坏规则会导致整批更新失败 |
| function=null | 热更新规则命中会 NPE |

---

## 11. 本篇小结

### 11.1 一句话总结

热更新靠"事件驱动 + Map 替换"：全量事件按 id+version 复用/新建/下线后原子替换整个注册表，增量事件只原地增删变更规则；状态名带版本后缀让升版天然从零开始，延迟清理队列解决 RPC 线程无 key 上下文的限制。

### 11.2 自测问题

1. 三类 OperatorEvent 分别对应什么场景？
2. 全量替换的三种分支是什么？为什么"同 id+version"要复用？
3. 为什么升版后状态从零开始？由哪行代码保证？
4. 为什么 `pendingStateClear` 必须延迟到 `processElement`？
5. 全量更新为什么用"建新 Map + 原子替换"，而增量用原地修改？
6. `createRuleContext` 的第 2 步中，第二个参数决定什么？
7. 一条规则创建失败会有什么后果？当前有按规则的容错吗？

### 11.3 下一篇

→ `07d-operator-lifecycle-inner-classes.md`：生命周期控制与内部类。
