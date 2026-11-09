# eagle-detection 代码精读 · 06 JM 侧协调器

> 本篇精读 `cep/operator/` 下的 5 个类：`DynamicCepCoordinator`、`DynamicCepCoordinatorProvider`、`DynamicCepOperatorFactory`、`PatternProcessorChangeEvent`、`RuleLifecycleEvent`。
> **读完后应能回答**：全量规则列表怎么变成增量？增量怎么发到 TM？checkpoint 与故障恢复怎么保证规则一致？

---

## 1. 本部分在整体中的位置

```
【第 05 篇】Discoverer（JM 后台线程）
        │  onPatternProcessorsUpdated(latest)
        ▼
【本篇】DynamicCepCoordinator（JM）
        │  computeDelta → OperatorEvent
        ▼
【第 07 篇】DynamicCepOperator（TM）
```

协调器是整条热更新链路的**枢纽**：向上接规则发现，向下接规则执行。

---

## 2. Flink Coordinator 机制基础

先补必要背景：

| 概念 | 说明 |
|---|---|
| `OperatorCoordinator` | 运行在 **JobManager**，与某个算子实例一一对应，负责协调该算子的所有 subtask |
| `OperatorCoordinator.Provider` | 随 JobGraph 序列化的工厂，JM 用它创建 Coordinator |
| `CoordinatedOperatorFactory` | 算子工厂接口，提供 `getCoordinatorProvider()` |
| `OperatorEvent` | JM ↔ TM 的事件对象，通过 `OperatorEventDispatcher` 传输，需 `Serializable` |
| `SubtaskGateway` | Coordinator 向某个 subtask 发送事件的通道 |
| `executionAttemptReady` | subtask 启动/恢复就绪回调， Coordinator 在此拿到 `SubtaskGateway` |
| `checkpointCoordinator` | Coordinator 自身的状态随 checkpoint 持久化 |

---

## 3. `DynamicCepCoordinatorProvider`

```java
public class DynamicCepCoordinatorProvider<IN> implements OperatorCoordinator.Provider {
    private static final long serialVersionUID = 1L;
    private final String operatorName;
    private final OperatorID operatorID;
    private final PatternProcessorDiscovererFactory<IN> discovererFactory;

    @Override public OperatorID getOperatorId() { return operatorID; }

    @Override public OperatorCoordinator create(OperatorCoordinator.Context context) {
        return new DynamicCepCoordinator<>(operatorName, discovererFactory, context);
    }
}
```

- **Provider 本身必须可序列化**（随 JobGraph 下发）。
- `discovererFactory` 也必须是 `Serializable`（第 05 篇强调过）。
- `create()` 在 JM 侧执行，真正实例化协调器。

---

## 4. `DynamicCepOperatorFactory` 中的协调器注册

```java
@Override
public OperatorCoordinator.Provider getCoordinatorProvider(
        String operatorName, OperatorID operatorID) {
    return new DynamicCepCoordinatorProvider<>(operatorName, operatorID, discovererFactory);
}
```

另外，`createStreamOperator` 里做了两件关键事：

```java
DynamicCepOperator<IN, ?, OUT> operator = new DynamicCepOperator<>(inputSerializer, isProcessingTime);

operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());

// ① 手动注入 ProcessingTimeService
operator.setProcessingTimeService(parameters.getProcessingTimeService());

// ② 注册事件处理器
parameters.getOperatorEventDispatcher().registerEventHandler(operatorId, operator);
```

**为什么手动注入 `ProcessingTimeService`？**
`AbstractStreamOperator`（V1）不会自动注入，若不开处理时间定时器功能可能不明显，但本算子需要 `getProcessingTimeService()` 在 `processElementForRule` 的 ProcessingTime 分支取当前时间，必须手动设置。

**为什么注册 `OperatorEventHandler`？**
`DynamicCepOperator` 实现了 `OperatorEventHandler`，必须在 dispatcher 注册后，Coordinator 发的 `OperatorEvent` 才能路由到它。

---

## 5. `DynamicCepCoordinator` 精读（核心）

### 5.1 字段

```java
private final String operatorName;
private final PatternProcessorDiscovererFactory<IN> discovererFactory;
private final Context context;

private PatternProcessorDiscoverer<IN> discoverer;
private volatile List<PatternProcessor<IN>> currentProcessors;   // 当前已下发的规则基线
private volatile boolean started;

private final Map<Integer, SubtaskGateway> gateways = new ConcurrentHashMap<>();
```

| 字段 | 作用 |
|---|---|
| `discoverer` | 规则发现器 |
| `currentProcessors` | **当前基线**，用于增量计算与 checkpoint |
| `gateways` | subtask 编号 → 通信网关 |
| `started` | 状态标记（当前仅赋值，未有逻辑读取） |

`currentProcessors` 用 `volatile`：会被 Timer 线程写、RPC 线程读。

### 5.2 `start()`

```java
@Override
public void start() throws Exception {
    started = true;
    discoverer = discovererFactory.createPatternProcessorDiscoverer(
            Thread.currentThread().getContextClassLoader());
    discoverer.discoverPatternProcessorUpdates(this);   // this 作为 Manager
}
```

- 用**线程上下文 ClassLoader** 创建发现器。
- `discoverPatternProcessorUpdates(this)`：把自己注册为回调接收者。
- 之后由发现器的 Timer 线程驱动。

### 5.3 `onPatternProcessorsUpdated()`：回调入口

```java
@Override
public void onPatternProcessorsUpdated(List<PatternProcessor<IN>> processors) {
    synchronized (this) {
        PatternProcessorChangeEvent<IN> changeEvent = computeDelta(processors);

        if (changeEvent.isEmpty()) {
            LOG.info("[COORD] No incremental changes, skipping distribution");
            return;
        }

        this.currentProcessors = processors;
        sendToAllGateways(changeEvent);
    }
}
```

**顺序很重要：先算增量，再更新基线，再发送。**

- 计算增量时用的是**旧基线**（`currentProcessors`）；
- 发送前更新基线，保证后续比较基于最新值；
- `synchronized` 防止并发回调交叉。

**若发送失败（gateway 抛异常）**：基线已更新，但 TM 未收到——存在不一致风险。当前代码未捕获，异常会向上抛到 Timer 线程（被基类 catch 吞掉），规则可能丢失，直到下一次变更或 subtask 重新就绪补发。

### 5.4 `computeDelta()`：全量 → 增量（重点）

```java
private PatternProcessorChangeEvent<IN> computeDelta(List<PatternProcessor<IN>> latest) {
    // ① 空列表：移除所有
    if (latest == null || latest.isEmpty()) {
        List<RuleIdentifier> removed = new ArrayList<>();
        if (currentProcessors != null) {
            for (PatternProcessor<IN> pp : currentProcessors) {
                removed.add(new RuleIdentifier(pp.getId(), pp.getVersion()));
            }
        }
        return new PatternProcessorChangeEvent<>(
                Collections.emptyList(), removed, Collections.emptyList());
    }

    // ② 最新列表按 id 建索引
    Map<String, PatternProcessor<IN>> latestMap = new HashMap<>();
    for (PatternProcessor<IN> pp : latest) latestMap.put(pp.getId(), pp);

    List<PatternProcessor<IN>> added = new ArrayList<>();
    List<PatternProcessor<IN>> updated = new ArrayList<>();
    List<RuleIdentifier> removed = new ArrayList<>();

    if (currentProcessors == null || currentProcessors.isEmpty()) {
        // ③ 首次：全部新增
        added.addAll(latest);
    } else {
        // ④ 新增 / 更新
        for (PatternProcessor<IN> pp : latest) {
            PatternProcessor<IN> old = findInCurrent(pp.getId());
            if (old == null) {
                added.add(pp);
            } else if (pp.getVersion() > old.getVersion()) {
                updated.add(pp);
            }
        }
        // ⑤ 删除：旧有新无
        for (PatternProcessor<IN> old : currentProcessors) {
            if (!latestMap.containsKey(old.getId())) {
                removed.add(new RuleIdentifier(old.getId(), old.getVersion()));
            }
        }
    }

    return new PatternProcessorChangeEvent<>(added, removed, updated);
}
```

**四类判定：**

| 情况 | 判定条件 | 归类 |
|---|---|---|
| 首次 / 基线为空 | `currentProcessors == null \|\| isEmpty` | 全部 `added` |
| 新规则 | 旧列表中无此 id | `added` |
| 版本升级 | 同 id 且 `new.version > old.version` | `updated` |
| 规则下线 | 旧有新无 | `removed` |

**边界与陷阱：**

1. **version 相同但内容不同 → 不更新**。规则管理端必须递增版本。
2. **version 降低 → 不更新**（`>` 而非 `!=`）。
3. **latest 为空 ≠ 无变化**：会生成"全部 removed"，把 TM 上所有规则清空。这是危险行为——如果 API 因故障返回空数组，会误删全部规则。当前代码**无法区分"真的没规则"和"API 异常返回空"**。
4. `Collections.emptyList()` 用于空列表，但 `PatternProcessorChangeEvent` 构造函数会 copy，安全。

### 5.5 `sendToAllGateways()`

```java
private void sendToAllGateways(OperatorEvent event) {
    int count = 0;
    for (SubtaskGateway gateway : gateways.values()) {
        gateway.sendEvent(event);
        count++;
    }
    LOG.info("[COORD] " + event.getClass().getSimpleName()
            + " sent to " + count + " SubtaskGateway(s)"
            + (count == 0 ? " (TM not ready yet, will resend when ready)" : ""));
}
```

- 广播给所有已就绪 subtask。
- `count == 0`：TM 尚未就绪，事件丢弃，等待 `executionAttemptReady` 补发。
- **没有重试/确认机制**：`sendEvent` 是单向的，失败不会感知。

### 5.6 Checkpoint

```java
@Override
public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(currentProcessors);
        oos.flush();
        resultFuture.complete(baos.toByteArray());
    } catch (Throwable e) {
        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
        resultFuture.completeExceptionally(new CompletionException(..., e));
    }
}

@Override
public void resetToCheckpoint(long checkpointId, byte[] checkpointData) throws Exception {
    if (checkpointData == null) return;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
         ObjectInputStream ois = new ObjectInputStream(bais)) {
        currentProcessors = (List<PatternProcessor<IN>>) ois.readObject();
    }
}
```

**要点：**

- 用 **Java 原生序列化**（不是 Flink 的 TypeSerializer）。
- 因此 `PatternProcessor` 及其 `function` 必须真的 `Serializable`。
- 恢复后 `currentProcessors` 有值，`executionAttemptReady` 时会把全量发给 TM。
- 注释说 `@SuppressWarnings("unchecked")`，因为反序列化是擦除类型。

**风险**：如果 `function` 持有不可序列化字段，checkpoint 会失败。当前 `DetectionAlertFunction` 只有 String/int，安全。

### 5.7 Subtask 生命周期

```java
@Override
public void subtaskReset(int subtask, long checkpointId) {
    resendToSubtask(subtask);
}

@Override
public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {
    gateways.remove(subtask);
}

@Override
public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
    gateways.put(subtask, gateway);
    resendToSubtask(subtask);
}
```

| 回调 | 时机 | 处理 |
|---|---|---|
| `executionAttemptReady` | subtask 启动/恢复完成 | 记录 gateway，补发全量 |
| `executionAttemptFailed` | 失败 | 移除 gateway |
| `subtaskReset` | 重置到 checkpoint | 补发全量 |

```java
private void resendToSubtask(int subtask) {
    SubtaskGateway gateway = gateways.get(subtask);
    if (gateway != null && currentProcessors != null) {
        UpdatePatternProcessorEvent<IN> event =
                new UpdatePatternProcessorEvent<>(currentProcessors);
        gateway.sendEvent(event);
    }
}
```

**为什么补发全量？**
重启后的 TM 内存为空，没有规则基线，**无法理解增量**（它不知道"删除"的是哪条旧规则）。全量是唯一能对齐的方式。

---

## 6. `PatternProcessorChangeEvent`

```java
public class PatternProcessorChangeEvent<T> implements OperatorEvent, Serializable {
    private final List<PatternProcessor<T>> added;    // 新增
    private final List<RuleIdentifier> removed;       // 删除（只带 id+version）
    private final List<PatternProcessor<T>> updated;  // 更新（完整规则）
}
```

**为什么 removed 只带 `RuleIdentifier`？**
删除不需要规则内容，只需定位。减少 RPC 体积。

```java
public static class RuleIdentifier implements Serializable {
    private final String id;
    private final int version;
    // equals/hashCode 基于 id+version
}
```

`isEmpty()`：三类都为空时表示无变化。

---

## 7. `RuleLifecycleEvent`

```java
public class RuleLifecycleEvent implements OperatorEvent, Serializable {
    public enum Action { PAUSE, RESUME, DRAIN, DELETE }
    private final Action action;
    private final String ruleId;
}
```

| Action | 语义 |
|---|---|
| `PAUSE` | 软暂停：NFA 与状态保留，不接收新事件 |
| `RESUME` | 恢复 |
| `DRAIN` | 优雅排空：不接收新事件，等待已有部分匹配超时 |
| `DELETE` | 硬下线：立即关闭并清理 |

> ⚠️ **当前未接通**：Coordinator 的 `handleEventFromOperator` 为空，没有从外部/API 下发 `RuleLifecycleEvent` 的入口；TM 侧的处理逻辑虽然存在，但实际收不到该事件。

---

## 8. 完整时序（热更新）

```
Discoverer(Timer)                Coordinator(JM)                 TM(subtask)
      │                                │                              │
      │ arePatternProcessorsUpdated()  │                              │
      │─────── true ──────────────────►│                              │
      │                                │ computeDelta(latest)          │
      │                                │  ├ added/removed/updated      │
      │                                │  ├ currentProcessors = latest │
      │                                │  └ sendToAllGateways          │
      │                                │──────── OperatorEvent ───────►│
      │                                │                              │ handleOperatorEvent
      │                                │                              │  └ 热替换 NFA
      │                                │                              │
      │                                │◄──── executionAttemptReady ───│（重启时）
      │                                │ resendToSubtask(全量)         │
      │                                │──────── 全量事件 ────────────►│
```

---

## 9. 设计要点与陷阱

| 点 | 说明 |
|---|---|
| 增量最小化 | 日常变更只发变化部分 |
| 全量用于恢复 | 重启 TM 无基线，只能全量对齐 |
| 基线更新在发送前 | 发送失败会不一致 |
| 空列表 = 全部删除 | API 异常返回空会误清规则 |
| 原生序列化 | function 必须真正可序列化 |
| 无确认/重试 | `sendEvent` 单向 |
| 生命周期事件未接通 | `handleEventFromOperator` 为空 |

---

## 10. 本篇小结

### 10.1 一句话总结

协调器把"规则发现"与"规则执行"解耦：发现器给全量列表，协调器算增量、更新基线、广播事件，并在 checkpoint 与 subtask 生命周期中保证规则最终一致。

### 10.2 自测问题

1. `Coordinator` 与 `Provider` 分别在哪一侧、何时创建？
2. 为什么 `createStreamOperator` 要手动 `setProcessingTimeService` 和注册事件处理器？
3. `computeDelta` 的四类判定分别是什么？version 不递增会怎样？
4. 为什么 `currentProcessors` 要在发送前更新？有什么风险？
5. 为什么 TM 恢复时必须发全量？
6. `latest` 为空数组时会发生什么？为什么危险？
7. `RuleLifecycleEvent` 为什么实际收不到？

### 10.3 下一篇

→ `07a-operator-data-structure.md`：进入 TM 侧执行引擎，先看数据结构与生命周期。
