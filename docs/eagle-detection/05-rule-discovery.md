# eagle-detection 代码精读 · 05 规则发现层

> 本篇精读 `cep/processor/` 下的接口与发现器实现（共 11 个类）。
> **读完后应能回答**：规则从哪来？怎么判断规则变了？`initialProcessors` 兜底怎么工作？为什么轮询线程必须吞异常？

---

## 1. 本部分在整体中的位置

```
【外部规则源】eagle-api GET /cep/rules（或 MySQL）
        │
        ▼
【本篇】Discoverer 体系（JM 侧后台 Timer 线程）
        │  onPatternProcessorsUpdated(latest)
        ▼
【第 06 篇】DynamicCepCoordinator
```

发现层是"规则进入引擎的第一站"。它只负责**发现变化并拉取最新列表**，不负责下发——下发是协调器的事。

---

## 2. 三层接口设计

### 2.1 `PatternProcessorDiscoverer`

```java
public interface PatternProcessorDiscoverer<T> extends Closeable {
    void discoverPatternProcessorUpdates(PatternProcessorManager<T> manager);
}
```

- 一个方法：启动发现，并注册回调管理器。
- 继承 `Closeable`：协调器关闭时要停掉轮询线程。

### 2.2 `PatternProcessorDiscovererFactory`

```java
public interface PatternProcessorDiscovererFactory<T> extends Serializable {
    PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception;
}
```

- **必须 `Serializable`**：工厂被放进 `DynamicCepOperatorFactory`，随 JobGraph 序列化到 JM。
- `userCodeClassLoader`：JM 侧创建发现器时，用于加载用户类（与规则条件有关）。

### 2.3 `PatternProcessorManager`

```java
public interface PatternProcessorManager<T> {
    void onPatternProcessorsUpdated(List<PatternProcessor<T>> patternProcessors);
}
```

- 回调接口。实现者是 `DynamicCepCoordinator`。
- 传入的是**最新完整列表**（不是增量），增量由协调器计算。

**三者关系：**

```
Factory --create--> Discoverer --callback--> Manager(Coordinator)
```

---

## 3. `PeriodicPatternProcessorDiscoverer` 精读（基类）

### 3.1 结构

```java
public abstract class PeriodicPatternProcessorDiscoverer<T>
        implements PatternProcessorDiscoverer<T> {

    private final Long intervalMillis;
    private final Timer timer;

    protected PeriodicPatternProcessorDiscoverer(Long intervalMillis) {
        this.intervalMillis = intervalMillis;
        this.timer = new Timer();
    }

    public abstract boolean arePatternProcessorsUpdated();
    public abstract List<PatternProcessor<T>> getLatestPatternProcessors() throws Exception;
}
```

**子类只需实现两个数据访问方法**，调度逻辑由基类负责。

### 3.2 调度实现

```java
@Override
public void discoverPatternProcessorUpdates(PatternProcessorManager<T> manager) {
    timer.schedule(new TimerTask() {
        @Override
        public void run() {
            try {
                if (arePatternProcessorsUpdated()) {
                    List<PatternProcessor<T>> processors = getLatestPatternProcessors();
                    manager.onPatternProcessorsUpdated(processors);
                }
            } catch (Exception e) {
                e.printStackTrace();   // ★ 必须 catch
            }
        }
    }, 0, intervalMillis);   // 首次 0ms 立即执行
}
```

**三个关键点：**

| 点 | 说明 |
|---|---|
| `delay=0` | 首次**立即**执行，不等一个周期 |
| `period=intervalMillis` | 之后周期执行 |
| `catch (Exception)` | **必须**：`TimerTask` 抛未捕获异常会终止整个 `Timer` 线程，轮询永久停止 |

> 这是 `java.util.Timer` 的经典陷阱：`Timer` 内部只有一个线程，任务异常 → 线程死亡 → 后续任务不再执行，且**不报错**。

### 3.3 `close()`

```java
@Override
public void close() throws IOException {
    timer.cancel();
}
```

`Timer.cancel()` 终止线程；已调度未执行的任务被丢弃。

---

## 4. `ApiPatternProcessorDiscoverer` 精读（生产实现）

### 4.1 字段

```java
private final String apiBaseUrl;
private final ClassLoader userCodeClassLoader;
private final List<PatternProcessor<T>> initialProcessors;   // 启动兜底
private List<PatternProcessor<T>> latestProcessors;          // 上一次结果（变更基线）

private static final ObjectMapper mapper = CepJsonUtils.getObjectMapper();
```

| 字段 | 作用 |
|---|---|
| `apiBaseUrl` | eagle-api 地址 |
| `initialProcessors` | 启动期同步加载的规则，API 不可达时兜底 |
| `latestProcessors` | 上一次拉取结果，用于变更比较 |

### 4.2 `arePatternProcessorsUpdated()`：变更检测

```java
@Override
public boolean arePatternProcessorsUpdated() {
    // ① 首次：优先用 initialProcessors
    if (latestProcessors == null && initialProcessors != null && !initialProcessors.isEmpty()) {
        return true;
    }

    // ② 轮询 API
    try {
        String rulesUrl = apiBaseUrl + "/cep/rules";
        String json = httpGet(rulesUrl);
        if (json == null || json.isEmpty()) return false;

        JsonNode root = mapper.readTree(json);
        JsonNode dataNode = root.has("data") ? root.get("data") : root;

        List<PatternProcessor<T>> current = new ArrayList<>();
        if (dataNode.isArray()) {
            for (JsonNode item : dataNode) {
                String ruleId = item.get("ruleId").asText();
                int version = item.get("version").asInt();
                String patternStr = item.get("pattern").asText();
                String functionClassName = ...;   // 读了但没用
                current.add(new DefaultPatternProcessor<>(ruleId, version, patternStr, null));
            }
        }

        if (latestProcessors == null || isChanged(current)) {
            latestProcessors = current;
            return true;
        }
        return false;
    } catch (Exception e) {
        return false;   // 网络异常 → 视为无变化
    }
}
```

**① 首次兜底逻辑**

- `latestProcessors == null` 表示还没拉取过。
- 若有 `initialProcessors`，直接返回 `true`（让上游走 `getLatestPatternProcessors()` 取初始规则）。
- 这一步**避免了启动时的 HTTP 请求**，保证规则立即可用。

**② 变更判定 `isChanged()`**

```java
private boolean isChanged(List<PatternProcessor<T>> current) {
    if (latestProcessors == null) return true;
    if (latestProcessors.size() != current.size()) return true;

    Map<String, Integer> oldMap = latestProcessors.stream()
            .collect(Collectors.toMap(PatternProcessor::getId, PatternProcessor::getVersion));
    Map<String, Integer> newMap = current.stream()
            .collect(Collectors.toMap(PatternProcessor::getId, PatternProcessor::getVersion));

    return !oldMap.equals(newMap);
}
```

**判定依据：`id → version` 映射是否相等。**

含义：
- 规则数量变了 → 变；
- 同 id 的 version 变了 → 变；
- **规则内容变了但 version 没变 → 不算变**（这是重要约束：规则管理端必须递增版本号）。

`Collectors.toMap` 在 **id 重复**时会抛 `IllegalStateException`——如果 API 返回了同 id 多条，会进入 catch 被当成"无变化"。

**③ `function` 字段的隐患**

```java
String functionClassName = item.has("function") && !item.get("function").isNull()
        ? item.get("function").asText() : null;

DefaultPatternProcessor<T> pp = new DefaultPatternProcessor<>(
        ruleId, version, patternStr, null);   // ← 读了但传 null
```

`functionClassName` 读取后**从未使用**，规则的回调函数为 `null`。

后果（第 02、07 篇已述）：热更新后的规则命中时 `userFunction.processMatch(...)` → **NPE**。

修复方向（不在本次任务范围）：
- 按 `functionClassName` 反射实例化（需要约定 `(String, int)` 构造器）；或
- 给 `DefaultPatternProcessor` 注入一个默认输出函数；或
- 在 `processMatchedSequences` 判空跳过。

### 4.3 `getLatestPatternProcessors()`

```java
@Override
public List<PatternProcessor<T>> getLatestPatternProcessors() throws Exception {
    if (latestProcessors == null && initialProcessors != null && !initialProcessors.isEmpty()) {
        latestProcessors = new ArrayList<>();
        return new ArrayList<>(initialProcessors);
    }
    if (latestProcessors == null) {
        throw new IllegalStateException(
                "latestProcessors is null. Call arePatternProcessorsUpdated() first.");
    }
    return new ArrayList<>(latestProcessors);
}
```

**注意一个微妙点：**

首次走 initialProcessors 分支时，把 `latestProcessors` 设为**空列表**（而不是 initialProcessors）。这样下次轮询：
- `latestProcessors != null` → 不走兜底；
- 会去请求 API，与空列表比较 → 若 API 有规则，视为变更。

**代价**：启动后第一次轮询一定会再拉一次 API（即使规则相同），但这是可接受的，因为它能及时发现"启动后 API 规则已变化"。

**返回值是副本**（`new ArrayList<>`），避免调用方修改内部状态。

### 4.4 `httpGet()`

```java
conn.setConnectTimeout(5000);
conn.setReadTimeout(5000);
conn.setRequestProperty("Accept", "application/json");
int code = conn.getResponseCode();
if (code != 200) return null;
```

- 5s 超时，非 200 返回 null。
- 没有重试与退避；依赖下一次轮询。
- `finally` 中 `disconnect()`。

---

## 5. `ApiPatternProcessorDiscovererFactory`

```java
public class ApiPatternProcessorDiscovererFactory<T>
        implements PatternProcessorDiscovererFactory<T> {

    private static final long serialVersionUID = 1L;
    private final String apiBaseUrl;
    private final List<PatternProcessor<T>> initialProcessors;
    private final Long intervalMillis;

    public PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception {
        return new ApiPatternProcessorDiscoverer<>(
                apiBaseUrl, userCodeClassLoader, initialProcessors, intervalMillis);
    }
}
```

- `Objects.requireNonNull(apiBaseUrl)`。
- `intervalMillis` 为 null 时默认 30000ms。
- 工厂本身可序列化；**它在 JM 侧被调用**，创建出真正的发现器。

---

## 6. `JDBCPeriodicPatternProcessorDiscoverer`

### 6.1 表结构约定

| 列 | 类型 | 说明 |
|---|---|---|
| `id` | VARCHAR | 规则 id |
| `version` | INT | 版本号 |
| `pattern` | TEXT | GraphSpec JSON |
| `function` | VARCHAR | `PatternProcessFunction` 全限定类名（可选） |

### 6.2 与 API 版的差异

| 维度 | API 版 | JDBC 版 |
|---|---|---|
| 日志前缀 | `[API-DISCOVER]` | `[DISCOVER]` |
| 变更基线 | `List` + `isChanged` | `Map<String, RowData>` |
| 同 id 多版本 | API 保证唯一 | **保留 version 最大的一条** |
| 失败处理 | 直接返回 false | `reconnect()` |
| function | 传 null（bug） | **反射实例化** |

### 6.3 同 id 取最大 version

```java
while (rs.next()) {
    String id = rs.getString("id");
    int version = rs.getInt("version");
    RowData existing = current.get(id);
    if (existing != null && existing.version >= version) continue;
    current.put(id, new RowData(id, version, rs.getString("pattern"), rs.getString("function")));
}
```

### 6.4 反射实例化 function

```java
if (!StringUtils.isNullOrWhitespaceOnly(row.function)) {
    func = (PatternProcessFunction<T, ?>) userCodeClassLoader
            .loadClass(row.function)
            .getConstructor(String.class, int.class)
            .newInstance(id, version);
}
```

**约定**：输出函数必须提供 `(String id, int version)` 构造器——与 `DetectionAlertFunction` 一致。

### 6.5 连接容错

```java
try {
    this.connection = DriverManager.getConnection(jdbcUrl);
    ...
} catch (SQLException e) {
    // 连接失败不阻断创建（允许 initialProcessors 模式）
    this.connection = null;
    this.statement = null;
}
```

- 构造期连接失败 → 降级为 initialProcessors 模式。
- 查询期失败 → `reconnect()` 后返回 false（下次再试）。

### 6.6 当前状态

`DetectionApp` **未使用** JDBC 版，生产走 API 版。保留它是作为"MySQL 直连"的备选方案。

---

## 7. `RuleState`（未使用）

```java
public enum RuleState {
    ACTIVE, PAUSED, DRAINING, STOPPED, ERROR
}
```

- 语义与 `RuleLifecycleEvent.Action` 有重叠。
- **当前代码中未被引用**：真正的暂停/排空控制走 `DynamicCepOperator` 内部的 `pausedRuleIds` / `drainingRuleIds` 集合。
- 属于"预留但未接通"的设计。

---

## 8. 发现层完整流程

```
Coordinator.start()
   │
   ├─ factory.createPatternProcessorDiscoverer(classLoader)
   │
   └─ discoverer.discoverPatternProcessorUpdates(coordinator)
          │
          └─ Timer.schedule(task, 0, 30s)
                 │
                 ├─ arePatternProcessorsUpdated()
                 │     ├─ 首次 + initialProcessors → true
                 │     └─ 轮询 API → isChanged(id→version) 
                 │
                 └─ 若 true → getLatestPatternProcessors()
                        │
                        └─ coordinator.onPatternProcessorsUpdated(latest)
```

---

## 9. 设计要点与陷阱

| 点 | 说明 |
|---|---|
| 首次立即执行 | `delay=0`，保证启动后马上有规则 |
| initialProcessors 兜底 | 避免启动期 HTTP 依赖 |
| 变更只看 version | 内容变了但没升版本不会生效 |
| Timer 异常必须吞 | 否则轮询线程死亡 |
| 返回值是副本 | 防止外部修改内部状态 |
| function 传 null | 热更新规则命中会 NPE |
| JDBC 构造期容错 | 连不上也能以初始规则启动 |

**排障提示：**

- 看不到规则生效：先看日志 `[API-DISCOVER] ✅ Rule change detected` 与 `[CEP] ★ Rules loaded`。
- API 返回格式变化：改 `dataNode` 解析或字段名（`ruleId`）。
- 规则改了内容没生效：检查 version 是否递增。

---

## 10. 本篇小结

### 10.1 一句话总结

发现层用"周期轮询 + 回调"把外部规则源的变化转成内存中的规则列表；`initialProcessors` 保证启动期可用，`id→version` 映射作为变更判定，API 版是生产路径、JDBC 版是备选。

### 10.2 自测问题

1. 三个接口各自的职责是什么？为什么工厂必须可序列化？
2. `TimerTask` 里为什么必须 catch 所有异常？
3. `initialProcessors` 首次返回后为什么把 `latestProcessors` 设为空列表？
4. 变更判定为什么只比较 `id→version`？有什么副作用？
5. API 版和 JDBC 版的 `function` 处理差异是什么？导致什么问题？
6. `getLatestPatternProcessors` 为什么返回副本？

### 10.3 下一篇

→ `06-jm-coordinator.md`：全量规则列表如何变成增量、如何通过 OperatorEvent 下发到 TM。
