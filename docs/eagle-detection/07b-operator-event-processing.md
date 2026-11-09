# eagle-detection 代码精读 · 07b 执行引擎：事件处理与时间语义

> 本篇聚焦 `DynamicCepOperator` 的事件处理链路：`processElement` → `processElementForRule` → 缓冲/回放 → `processEvent` → `advanceTime` → 输出。
> **读完后应能回答**：一条事件如何被所有规则评估？乱序事件如何保证时序？超时部分匹配怎么产生？

---

## 1. 事件处理全景

```
processElement(事件)
   │
   ├─ ① 补做 pendingStateClear
   ├─ ② 无规则 → 丢弃
   └─ ③ for 每条规则 → processElementForRule
                          │
                          ├─ ProcessingTime → 立即匹配
                          │     advanceTime → processEvent → updateNFA
                          │
                          └─ EventTime → 与 watermark 比较
                                ├─ 未迟到 → bufferEvent + registerTimer
                                └─ 已迟到 → numLateRecordsDropped++

onEventTime(timer)
   └─ for 每条规则 → processTimedEvents
                        └─ 按时间戳升序回放缓冲事件
                              advanceTime → processEvent → updateNFA
```

---

## 2. `processElement()`：事件入口

```java
@Override
public void processElement(StreamRecord<IN> element) throws Exception {
    // ① 延迟清理
    if (!pendingStateClear.isEmpty()) {
        for (RuleKey key : pendingStateClear) {
            RuleContext<IN, OUT> oldRc = ruleContexts.get(key);
            if (oldRc != null) {
                oldRc.computationStates.clear();
                oldRc.elementQueueState.clear();
            }
        }
        pendingStateClear.clear();
    }

    // ② 无规则保护
    if (ruleContexts.isEmpty()) {
        LOG.info("[CEP] ⚠ Event arrived but no active rules, dropping and waiting...");
        return;
    }

    // ③ 广播给所有规则
    for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry : ruleContexts.entrySet()) {
        RuleKey ruleKey = entry.getKey();
        RuleContext<IN, OUT> rc = entry.getValue();
        try {
            processElementForRule(element, ruleKey, rc);
        } catch (Exception e) {
            LOG.error("Error processing event for rule {}-v{}: {}",
                    ruleKey.id, ruleKey.version, e.getMessage(), e);
        }
    }
}
```

### 2.1 ① 延迟清理

- 热更新时（RPC 线程）只把旧 `RuleKey` 放进 `pendingStateClear`；
- 到下一次 `processElement`（算子线程、有 key 上下文）才真正 `clear()`。

**为什么必须延迟？**
`ValueState.clear()` 是 keyed state 操作，必须在有 key 上下文的算子线程执行；`handleOperatorEvent` 由 RPC 线程调用，此时没有当前 key。

**局限**：只能清掉"当前 key"的状态。其他 key 下的旧状态成孤儿。

### 2.2 ② 无规则丢弃

- 这是"规则未到、事件先到"的兜底。
- 注意日志每条事件都打一次，在高吞吐下可能刷屏，可考虑限流。

### 2.3 ③ 广播 + 异常隔离

- `try/catch` 在规则循环内：单条规则异常不影响其他规则。
- 遍历 `ruleContexts.entrySet()`：如果热更新在遍历中替换了 Map 引用，当前遍历仍用旧 Map（Java 引用语义），安全。

---

## 3. `processElementForRule()`：单规则分发

```java
private void processElementForRule(StreamRecord<IN> element, RuleKey ruleKey,
                                   RuleContext<IN, OUT> rc) throws Exception {

    // 生命周期：暂停/排空则跳过
    if (shouldSkipRule(ruleKey.id)) {
        return;
    }

    if (isProcessingTime) {
        NFAState nfaState = getNFAState(rc);
        long timestamp = getProcessingTimeService().getCurrentProcessingTime();
        advanceTime(rc, nfaState, timestamp);
        processEvent(rc, nfaState, element.getValue(), timestamp);
        updateNFA(rc, nfaState);
    } else {
        long timestamp = element.getTimestamp();
        IN value = element.getValue();

        if (timestamp > rc.timerService.currentWatermark()) {
            bufferEvent(rc, value, timestamp);
        } else {
            rc.numLateRecordsDropped.inc();
        }
    }
}
```

### 3.1 ProcessingTime 分支

1. 当前处理时间作为时间戳；
2. `advanceTime` 推进 NFA（可能触发超时）；
3. `processEvent` 匹配当前事件；
4. `updateNFA` 写回状态。

> 为什么 ProcessingTime 先 `advanceTime`？保证 NFA 内部时间单调前进。

### 3.2 EventTime 分支：迟到的判定

```java
if (timestamp > rc.timerService.currentWatermark()) {
    bufferEvent(rc, value, timestamp);
} else {
    rc.numLateRecordsDropped.inc();
}
```

- **严格大于** `watermark` 才算有效。
- `timestamp == watermark` 会被判为迟到——这是 Flink CEP 的常见语义（watermark 表示"不会再有 ≤ 该时间的事件"）。
- 迟到事件不缓冲、不匹配，只计数。

**注意**：这里用的是 `rc.timerService.currentWatermark()`（每规则独立 TimerService 的水位）。它们实际上是同一个物理水位，但代码通过每规则的 TimerService 读取。

---

## 4. 定时器回调

```java
@Override
public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
    for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry : ruleContexts.entrySet()) {
        processTimedEvents(entry.getValue(), false);
    }
}

@Override
public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
    for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry : ruleContexts.entrySet()) {
        processTimedEvents(entry.getValue(), true);
    }
}
```

**重要观察**：任一规则的定时器触发，都会**遍历所有规则**做一次回放。

**为什么这样设计？**
因为 timer 的回调签名只带 `(key, namespace)`，不带"哪条规则"的信息（namespace 是 `VoidNamespace`）。实现上无法区分，只能全量回放。

**代价**：定时器密集时存在重复扫描。但每条规则内部会检查自己的水位，未到时间则跳过，所以逻辑正确。

**优化方向**（未实现）：用自定义 namespace 携带规则标识，回调时只处理对应规则。

---

## 5. `processTimedEvents()`：缓冲回放

```java
private void processTimedEvents(RuleContext<IN, OUT> rc, boolean isProcTime) throws Exception {
    PriorityQueue<Long> sortedTimestamps = getSortedTimestamps(rc);
    NFAState nfaState = getNFAState(rc);

    while (!sortedTimestamps.isEmpty()
            && (isProcTime || sortedTimestamps.peek() <= rc.timerService.currentWatermark())) {
        long timestamp = sortedTimestamps.poll();
        advanceTime(rc, nfaState, timestamp);

        List<IN> elements = rc.elementQueueState.get(timestamp);
        if (elements != null) {
            for (IN event : elements) {
                processEvent(rc, nfaState, event, timestamp);
            }
        }
        rc.elementQueueState.remove(timestamp);
    }

    advanceTime(rc, nfaState,
            isProcTime ? rc.timerService.currentProcessingTime()
                       : rc.timerService.currentWatermark());
    updateNFA(rc, nfaState);
}
```

### 5.1 流程

1. 取出该规则缓冲队列中所有时间戳，放入小顶堆升序；
2. 循环处理 `≤ watermark` 的时间戳：
   - `advanceTime` 推进 NFA 到该时刻（触发超时/匹配）；
   - 取出该时间戳下的事件，逐条 `processEvent`；
   - 从 `MapState` 移除该时间戳；
3. 最后再 `advanceTime` 到当前水位/处理时间（清理尾部超时）；
4. 写回状态。

### 5.2 为什么升序

保证**乱序到达的事件按事件时间顺序**被 NFA 处理。这是 EventTime 语义正确性的核心。

### 5.3 为什么同时间戳可能有多个事件

`bufferEvent` 用 `MapState<Long, List<IN>>`，同一毫秒的事件会被放进同一个 List，按到达顺序处理。

### 5.4 为什么末尾还要 `advanceTime` 一次

处理完所有已到时间戳后，NFA 的内部时间可能仍落后于 watermark；再推进一次以触发"窗口整体超时"的部分匹配。

---

## 6. `processEvent()`：真正匹配

```java
private void processEvent(RuleContext<IN, OUT> rc, NFAState nfaState,
                          IN event, long timestamp) throws Exception {
    AfterMatchSkipStrategy skipStrategy = getAfterMatchSkipStrategy(rc);

    try (SharedBufferAccessor<IN> accessor = sharedPartialMatches.getAccessor()) {
        Collection<Map<String, List<IN>>> patterns =
                rc.nfa.process(accessor, nfaState, event, timestamp,
                        skipStrategy, rc.cepTimerService);

        if (rc.nfa.getWindowTime() > 0 && nfaState.isNewStartPartialMatch()) {
            registerTimer(rc, timestamp + rc.nfa.getWindowTime());
        }

        processMatchedSequences(rc, patterns, timestamp);
    }
}
```

**逐步解释：**

1. **取跳过策略**：从规则 Pattern 的 `getAfterMatchSkipStrategy()` 取（每次调用 `processor.getPattern()` 重新反序列化）。
2. **获取 SharedBufferAccessor**：`try-with-resources` 保证释放。
3. **`nfa.process(...)`**：Flink NFA 的核心方法，返回本次产生的完整匹配。
4. **窗口超时定时器**：
   - `nfa.getWindowTime() > 0` 表示有 `within`；
   - `nfaState.isNewStartPartialMatch()` 表示本次事件开启了新的部分匹配起点；
   - 注册 `timestamp + windowTime` 的定时器，用于未来的超时回调。
5. **输出匹配**。

**注意**：`registerTimer` 用的是每规则独立的 `timerService`，所以不同规则的超时互不影响。

---

## 7. `advanceTime()`：推进时间线

```java
private void advanceTime(RuleContext<IN, OUT> rc, NFAState nfaState, long timestamp) throws Exception {
    AfterMatchSkipStrategy skipStrategy = getAfterMatchSkipStrategy(rc);

    try (SharedBufferAccessor<IN> accessor = sharedPartialMatches.getAccessor()) {
        Tuple2<Collection<Map<String, List<IN>>>,
               Collection<Tuple2<Map<String, List<IN>>, Long>>> result =
                rc.nfa.advanceTime(accessor, nfaState, timestamp, skipStrategy);

        if (!result.f0.isEmpty()) {
            processMatchedSequences(rc, result.f0, timestamp);
        }
        if (!result.f1.isEmpty()) {
            processTimedOutSequences(rc, result.f1);
        }
    }
}
```

**返回两部分：**

| 结果 | 含义 | 处理 |
|---|---|---|
| `f0` | 因时间推进而完成的匹配 | `processMatchedSequences` |
| `f1` | 因超出窗口而超时的部分匹配 + 时间戳 | `processTimedOutSequences` |

**为什么 `advanceTime` 会产生完整匹配？**
某些模式（如 `times` 配合窗口）在时间推进到窗口末尾时才能确定完成。

---

## 8. 输出处理

### 8.1 `processMatchedSequences()`

```java
private void processMatchedSequences(RuleContext<IN, OUT> rc,
        Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
    setTimestamp(timestamp);
    for (Map<String, List<IN>> match : matchingSequences) {
        rc.patternMatchedTimes.inc();
        rc.userFunction.processMatch(match, context, (Collector<OUT>) collector);
    }
}
```

- `setTimestamp`：EventTime 下设置 `collector` 的绝对时间戳；
- 每次命中 `patternMatchedTimes++`；
- **`rc.userFunction` 未判空** → 热更新规则（function=null）命中时 NPE。

### 8.2 `processTimedOutSequences()`

```java
private void processTimedOutSequences(RuleContext<IN, OUT> rc,
        Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut) throws Exception {
    if (rc.userFunction instanceof TimedOutPartialMatchHandler) {
        TimedOutPartialMatchHandler<IN> handler =
                (TimedOutPartialMatchHandler<IN>) rc.userFunction;
        for (Tuple2<Map<String, List<IN>>, Long> match : timedOut) {
            setTimestamp(match.f1);
            handler.processTimedOutMatch(match.f0, context);
        }
    }
}
```

- 只有用户函数实现 `TimedOutPartialMatchHandler` 才回调，否则丢弃；
- `DetectionAlertFunction` **没有实现**该接口，所以超时部分匹配当前被静默丢弃。

---

## 9. 状态读写

```java
private NFAState getNFAState(RuleContext<IN, OUT> rc) throws IOException {
    NFAState state = rc.computationStates.value();
    return state != null ? state : rc.nfa.createInitialNFAState();
}

private void updateNFA(RuleContext<IN, OUT> rc, NFAState state) throws IOException {
    if (state.isStateChanged()) {
        state.resetStateChanged();
        state.resetNewStartPartialMatch();
        rc.computationStates.update(state);
    }
}
```

- 首次访问 → 创建初始状态；
- **只在 `isStateChanged()` 时写回**，避免每条事件都序列化一次状态（性能关键）；
- 写回后重置变更标记与新起点标记。

---

## 10. 缓冲与定时器辅助方法

```java
private void registerTimer(RuleContext<IN, OUT> rc, long timestamp) {
    if (isProcessingTime) {
        rc.timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp + 1);
    } else {
        rc.timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }
}

private void bufferEvent(RuleContext<IN, OUT> rc, IN event, long currentTime) throws Exception {
    List<IN> elements = rc.elementQueueState.get(currentTime);
    if (elements == null) {
        elements = new ArrayList<>();
        registerTimer(rc, currentTime);
    }
    elements.add(event);
    rc.elementQueueState.put(currentTime, elements);
}

private PriorityQueue<Long> getSortedTimestamps(RuleContext<IN, OUT> rc) throws Exception {
    PriorityQueue<Long> sorted = new PriorityQueue<>();
    for (Long ts : rc.elementQueueState.keys()) sorted.offer(ts);
    return sorted;
}
```

**细节：**

- ProcessingTime 注册 `timestamp + 1`：避免定时器与当前处理时间相同导致立即触发/不触发问题。
- 缓冲时**只在新建时间戳时注册定时器**，同一时间戳的后续事件复用。
- `getSortedTimestamps` 每次全量遍历 `keys()`，在事件密集时开销较大（可用 TreeMap 优化）。

---

## 11. 时间语义完整对比

| 维度 | ProcessingTime | EventTime |
|---|---|---|
| 时间戳来源 | `getProcessingTimeService().getCurrentProcessingTime()` | `element.getTimestamp()` |
| 是否缓冲 | 否 | 是（`elementQueueState`） |
| 触发时机 | 每条事件立即 | watermark 推进后回放 |
| 迟到处理 | 不存在 | `numLateRecordsDropped` |
| 定时器 | `registerProcessingTimeTimer` | `registerEventTimeTimer` |
| 窗口准确性 | 受机器时间影响 | 由数据决定，可重放 |

---

## 12. 本篇小结

### 12.1 一句话总结

事件先广播给所有规则；EventTime 下按时间戳缓冲、由 watermark 驱动升序回放，保证乱序下的时序正确；`nfa.process` 负责状态转移，`nfa.advanceTime` 负责产出超时与延迟完成的匹配；每规则独立的 TimerService 让超时互不干扰。

### 12.2 自测问题

1. `processElement` 为什么必须先执行 `pendingStateClear`？
2. EventTime 下迟到的判定条件是什么？等于 watermark 算不算迟到？
3. 为什么 `onEventTime` 要遍历所有规则，而不是只处理触发的那一条？
4. `processTimedEvents` 为什么按时间戳升序回放？
5. 为什么要注册 `timestamp + windowTime` 的定时器？
6. `updateNFA` 为什么只在 `isStateChanged()` 时写状态？
7. ProcessingTime 定时器为什么用 `timestamp + 1`？
8. `userFunction` 为 null 会在哪个方法爆？

### 12.3 下一篇

→ `07c-operator-hot-swap.md`：规则热更新的三种事件、全量替换的分支逻辑、`createRuleContext` 的 7 步。
