# eagle-detection 代码精读 · 02 API 门面与规则抽象

> 本篇精读 4 个文件：`cep/DynamicCEP.java`、`cep/PatternWrapper.java`、`cep/processor/PatternProcessor.java`、`cep/processor/DefaultPatternProcessor.java`。
> **读完后应能回答**：`DynamicCEP.dynamicPatterns()` 到底做了什么？"一条规则"在内存里由哪些部分组成？为什么规则要存 JSON 而不是 `Pattern` 对象？

---

## 1. 本部分在整体中的位置

```
【上一篇】DetectionApp
        │  传入 DataStream<Transaction> + DiscovererFactory
        ▼
【本篇】DynamicCEP.dynamicPatterns()
        │  创建 DynamicCepOperatorFactory，插入算子
        ▼
【第 07 篇】DynamicCepOperator
```

本篇是"从业务作业进入 CEP 引擎"的门口。它的代码量很小（`DynamicCEP` 约 65 行、`PatternWrapper` 约 20 行），但每个决定都影响后续所有设计。

---

## 2. `DynamicCEP.java` 精读

### 2.1 类的定位

`DynamicCEP` 是**静态门面类**，对标官方 Flink CEP 的 `CEP` 类。它不持有状态，只做一件事：

> 把「事件流 + 规则发现器 + 时间语义 + 输出类型」组装成一个带热更新能力的 CEP 算子，并挂到数据流上。

```java
public class DynamicCEP {
    public static <T, R> SingleOutputStreamOperator<R> dynamicPatterns(
            DataStream<T> input,
            PatternProcessorDiscovererFactory<T> discovererFactory,
            boolean isProcessingTime,
            TypeInformation<R> outTypeInfo)
```

| 参数 | 业务含义 |
|---|---|
| `input` | 交易事件流（`Transaction`） |
| `discovererFactory` | 规则发现器工厂（JM 侧用它创建轮询器） |
| `isProcessingTime` | `false` = EventTime（本模块固定用 `false`） |
| `outTypeInfo` | 输出类型（`String` 告警文本） |

### 2.2 源码逐段拆解

```java
public static <T, R> SingleOutputStreamOperator<R> dynamicPatterns(
        DataStream<T> input,
        PatternProcessorDiscovererFactory<T> discovererFactory,
        boolean isProcessingTime,
        TypeInformation<R> outTypeInfo) {

    // ① 创建算子工厂
    DynamicCepOperatorFactory<T, R> factory =
            new DynamicCepOperatorFactory<>(
                    discovererFactory,
                    input.getType().createSerializer(input.getExecutionConfig()),
                    isProcessingTime);

    // ② 用 raw type 绕过泛型推断
    DataStream rawStream = input;

    // ③ 已是 KeyedStream：直接插算子
    if (rawStream instanceof KeyedStream) {
        return ((KeyedStream) rawStream)
                .transform("DynamicCepOperator", outTypeInfo,
                        (OneInputStreamOperatorFactory) factory);
    }

    // ④ 不是 KeyedStream：补 keyBy + 强制单并行度
    return rawStream.keyBy(new NullByteKeySelector())
            .transform("DynamicCepOperator", outTypeInfo,
                    (OneInputStreamOperatorFactory) factory)
            .forceNonParallel();
}
```

#### ① 工厂参数

```java
new DynamicCepOperatorFactory<>(
        discovererFactory,
        input.getType().createSerializer(input.getExecutionConfig()),
        isProcessingTime);
```

| 参数 | 为什么需要 |
|---|---|
| `discovererFactory` | 工厂会随 JobGraph 序列化到 JM；JM 侧用它创建 `DynamicCepCoordinator` |
| `input.getType().createSerializer(...)` | 算子内部要手动构造 `MapState<Long, List<IN>>` 等状态描述符，必须显式提供事件序列化器 |
| `isProcessingTime` | 决定算子走处理时间还是事件时间分支 |

**为什么不能省掉 `TypeSerializer`？**

原生 CEP 算子由 Flink 的 `TypeExtractor` 自动推断类型并注入序列化器；而本项目的 `DynamicCepOperator` 是**自定义 `AbstractStreamOperator`**，需要自己建状态：

```java
new MapStateDescriptor<>(
        EVENT_QUEUE_PREFIX + "-" + stateSuffix,
        LongSerializer.INSTANCE,
        new ListSerializer<>(inputSerializer));   // ← 这里必须要有 inputSerializer
```

所以必须在 `dynamicPatterns` 阶段把序列化器显式传进去。

#### ② 为什么要用 raw type

原注释写得很直白：

> Generic type inference may fail across interface hierarchies; use raw type to bypass.

`DynamicCepOperatorFactory` 同时实现了 `AbstractStreamOperatorFactory`、`OneInputStreamOperatorFactory`、`CoordinatedOperatorFactory` 三个接口层次，Java 泛型推断在这种多重继承下容易失败。用 `DataStream rawStream = input;` 擦除泛型后再强转，是**工程上的妥协**。

代价：丢失编译期类型检查，需要 `@SuppressWarnings({"rawtypes", "unchecked"})` 压制。

#### ③ KeyedStream 分支

如果调用方已经 `keyBy`（比如按 `payerId`），则直接 `transform`，**并行度保持调用方设定**。

此时语义是：**每个 key 在每个 subtask 内独立匹配**——适合"按账户维度做 CEP"的场景。

#### ④ 非 KeyedStream 分支（本模块实际走的路）

```java
rawStream.keyBy(new NullByteKeySelector())
        .transform(...)
        .forceNonParallel();
```

**`NullByteKeySelector` 是什么？**

Flink 内置的 key selector，对所有事件返回同一个 `byte` 值（0）。效果是：**所有事件落入同一个 key**。

**`forceNonParallel()` 是什么？**

把该算子的并行度强制设为 1。无论全局并行度配置多少，这个算子只会有一个 subtask。

**为什么必须这样？**

因为当所有事件都在同一个 key 上时，如果算子并行度 > 1，同 key 的事件也只会路由到某一个 subtask，其他 subtask 空转——没有意义。反过来，强制单并行度能保证：

1. 全局只有一份规则注册表、一份 NFA 状态，语义最简单；
2. 规则热更新只需考虑一个 subtask 的状态，避免多副本一致性问题。

**代价**：**吞吐上限被锁死在单并行度**。这是当前架构最明显的瓶颈。

> ⚠️ 重要结论：`DetectionApp` 传入的是普通 `DataStream`（未 keyBy），因此 **CEP 算子一定是单并行度**。即使 `flink.parallelism=2`，这个算子也只会是 1。

#### ⑤ 为什么用 `transform` 而不是 `process`

| API | 接受参数 | 能力 |
|---|---|---|
| `process(ProcessFunction)` | 只能接受 `ProcessFunction` | 无法实现 `Triggerable`、`OperatorEventHandler` |
| `transform(..., OneInputStreamOperatorFactory)` | 可以插入任意 `OneInputStreamOperator` | 完全控制算子生命周期、状态、定时器、事件通道 |

`DynamicCepOperator` 需要：
- `Triggerable`：响应事件时间/处理时间定时器（超时处理）；
- `OperatorEventHandler`：接收 JM 下发的规则变更；
- 自定义 keyed state（多规则的 `ValueState`/`MapState`）；
- 自定义 `InternalTimerService`（每规则一份）。

这些都不是 `ProcessFunction` 能做到的，所以必须走底层 `transform`。

### 2.3 与官方 `CEP.pattern()` 的对照

| 能力 | 官方 `CEP.pattern()` | `DynamicCEP.dynamicPatterns()` |
|---|---|---|
| Pattern 来源 | 编译期 Java 代码 | 运行时 JSON（可热更新） |
| 规则数量 | 1 条 | N 条并行 |
| 底层算子 | `CepOperator` | 自定义 `DynamicCepOperator` |
| 状态名 | 固定 | 带 `{id}-{version}` 后缀 |
| 更新通道 | 无 | OperatorEvent |
| 并行度 | 跟随输入 | 未 keyBy 时强制 1 |

### 2.4 本类设计要点与陷阱

| 要点 | 说明 |
|---|---|
| 门面极薄 | 只做装配，业务逻辑全在工厂和算子 |
| `TypeSerializer` 必须传 | 自定义状态描述符需要它 |
| `raw type` + `forceNonParallel` | 泛型推断妥协 + 单并行度保证 |
| 已 keyBy 则不强制单并行 | 调用方可自行决定 key 维度 |
| 算子名称固定 | `"DynamicCepOperator"`，在 Flink UI 上可见 |

---

## 3. `PatternWrapper.java` 精读

### 3.1 完整代码

```java
public class PatternWrapper<T, F extends T> extends Pattern<T, F> {

    public PatternWrapper(
            String name,
            Pattern<T, ? extends T> previous,
            Quantifier.ConsumingStrategy consumingStrategy,
            AfterMatchSkipStrategy afterMatchSkipStrategy) {
        super(name, previous, consumingStrategy, afterMatchSkipStrategy);
    }
}
```

### 3.2 它解决什么问题

Flink 的 `Pattern` 构造函数签名是：

```java
protected Pattern(
        String name,
        Pattern<T, ? extends T> previous,
        Quantifier.ConsumingStrategy consumingStrategy,
        AfterMatchSkipStrategy afterMatchSkipStrategy)
```

`protected` 的含义：**只有同包类或子类能调用**。

本项目的 `NodeSpec.toPattern()` 在 `com.alarm.eagle.cep.json.spec` 包下，与 `org.apache.flink.cep.pattern.Pattern` **不同包**，无法直接 `new Pattern(...)`。

`PatternWrapper` 通过**继承 + `super(...)`** 把受保护构造器暴露成 `public`：

```java
// NodeSpec.toPattern 中：
Pattern pattern = new PatternWrapper(
        this.getName(), previous, consumingStrategy, afterMatchSkipStrategy);
```

### 3.3 为什么不用反射

| 方式 | 问题 |
|---|---|
| 反射 `getDeclaredConstructor().setAccessible(true)` | JDK 17+ 模块系统限制，需要 `--add-opens`；编译期无检查、易随版本失效 |
| 继承 + `super()` | 编译期类型安全、无模块访问问题、稳定 |

这是**利用 Java 语言特性绕过访问控制**的经典手法，比反射优雅且可靠。

### 3.4 使用位置

`PatternWrapper` 只在 `NodeSpec.toPattern()` 中被使用一次（第 03 篇详解）。它是纯粹的"桥接工具类"，不参与业务。

> 注意：`PatternWrapper` 只暴露构造器，不重写任何方法。它产生的对象就是普通 `Pattern`，后续 `NFACompiler` 正常处理。

---

## 4. `PatternProcessor.java` 精读

### 4.1 完整接口

```java
public interface PatternProcessor<T> extends Serializable {

    String getId();

    int getVersion();

    Pattern<T, ?> getPattern(ClassLoader classLoader);

    PatternProcessFunction<T, ?> getPatternProcessFunction();

    String getPatternJSON();
}
```

### 4.2 设计意图

`PatternProcessor` 是**一条 CEP 规则的完整抽象**。它回答四个问题：

| 方法 | 回答的问题 | 使用方 |
|---|---|---|
| `getId()` | 这条规则是谁？ | 热更新、增量计算、生命周期控制 |
| `getVersion()` | 是第几版？ | 版本隔离、增量判定 |
| `getPattern(...)` | 匹配逻辑是什么？ | `DynamicCepOperator.createRuleContext` |
| `getPatternProcessFunction()` | 命中后做什么？ | `processMatchedSequences` |
| `getPatternJSON()` | 原始定义是什么？ | 调试、持久化、传输 |

### 4.3 为什么继承 `Serializable`

这是**硬性要求**，源于两条传输路径：

1. **JobGraph 序列化**：`initialProcessors` 被放进 `ApiPatternProcessorDiscovererFactory`，工厂随 JobGraph 从客户端发到 JM。
2. **OperatorEvent RPC**：规则变更时，`PatternProcessor` 列表要从 JM 发到 TM。

因此规则对象（包括其 `function`）必须可序列化。

### 4.4 `getPattern(ClassLoader)` 为什么要传 `ClassLoader`

因为规则 JSON 里的条件可能是 `CLASS` 类型，需要按类名加载用户自定义的 `IterativeCondition`：

```java
// ClassConditionSpec.toIterativeCondition
Class<?> clazz = classLoader.loadClass(className);
```

Flink 中用户代码运行在独立的 `UserCodeClassLoader` 下，**不能用 `Class.forName` 或默认 ClassLoader**，必须用 `getUserCodeClassloader()` 传入，否则可能 `ClassNotFoundException`。

### 4.5 为什么用接口而不是抽象类

规则来源可能多样（JSON、JDBC、未来其他形式），接口让实现更灵活；当前只有一个实现 `DefaultPatternProcessor`。

---

## 5. `DefaultPatternProcessor.java` 精读

### 5.1 完整字段

```java
public class DefaultPatternProcessor<T> implements PatternProcessor<T> {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final int version;
    private final String patternStr;          // ★ GraphSpec JSON 字符串
    private final PatternProcessFunction<T, ?> function;
```

| 字段 | 说明 |
|---|---|
| `id` | 规则唯一标识，`requireNonNull` |
| `version` | 版本号，**不校验**（可为 0） |
| `patternStr` | 规则 JSON，`requireNonNull` |
| `function` | 命中回调，**允许为 null**（这是隐患源头） |

**全部 `final`**：对象不可变，天然线程安全；也符合"规则一旦创建就代表某个版本快照"的语义。

### 5.2 为什么存 JSON 而不是 `Pattern` 对象（核心设计）

类注释给了三条理由，逐条解释：

1. **`Pattern` 在 NFA 编译时会被修改**
   - `NFACompiler` 在编译时可能给 Pattern 注入 `untilCondition`、规整量词等。
   - 如果多个规则/多个 subtask 共享同一个 `Pattern` 对象，会互相污染。
2. **JSON 是纯数据，天然可序列化**
   - `Pattern` 内部持有 `IterativeCondition`、`Quantifier` 等，序列化行为复杂且不稳定；
   - JSON 字符串是 `String`，跨 RPC 传输零障碍。
3. **版本独立，无副作用**
   - 每条规则、每个版本各自持有一份 JSON，反序列化出独立的 `Pattern` 实例。

> 一句话：**JSON 是"规则的定义"，Pattern 是"规则的编译产物"；定义要传输和存储，产物只在运行时临时生成。**

### 5.3 `getPattern()`：每次重新反序列化

```java
@Override
@SuppressWarnings("unchecked")
public Pattern<T, ?> getPattern(ClassLoader classLoader) {
    try {
        return (Pattern<T, ?>) CepJsonUtils
                .convertJSONStringToPattern(patternStr, classLoader);
    } catch (Exception e) {
        throw new RuntimeException(
                "Failed to deserialize pattern for rule: " + id, e);
    }
}
```

**关键点：没有缓存。** 每次调用都完整走一遍 `JSON → GraphSpec → Pattern`。

这是刻意设计：
- 避免 `Pattern` 对象被 NFA 编译修改后影响下次使用；
- 避免不同版本/不同 subtask 之间共享可变对象。

**代价**：规则热更新时会有一次 JSON 解析开销。但热更新是低频操作，可接受。

**异常处理**：反序列化失败包装成 `RuntimeException`，会中断 `createRuleContext`，进而导致整个 `handleOperatorEvent` 抛出，**整批规则更新失败**（第 07 篇会讲其影响）。

### 5.4 `function` 可为 null 的隐患

构造函数没有对 `function` 做 `requireNonNull`：

```java
this.function = function;   // 允许 null
```

`getPatternProcessFunction()` 直接返回，也可能为 `null`。

**两条规则来源的行为差异：**

| 来源 | function |
|---|---|
| `DetectionApp.fetchRulesFromApi` | `new DetectionAlertFunction(ruleId, version)` ✅ |
| `ApiPatternProcessorDiscoverer` | `null` ❌（读取了 `function` 字段却没用） |

**后果**：热更新后，`DynamicCepOperator.processMatchedSequences` 会执行：

```java
rc.userFunction.processMatch(match, context, collector);   // userFunction == null → NPE
```

`createRuleContext` 里对 `function` 做了判空（跳过 `open`），但**匹配输出路径没有判空**。这是一个真实的待修复缺陷【待确认业务】。

### 5.5 `toString()`

```java
@Override
public String toString() {
    return "DefaultPatternProcessor{id='" + id + "', version=" + version + '}';
}
```

日志友好，但不输出 patternStr（可能很长）。

---

## 6. 规则在内存中的完整形态

把本篇串起来，一条规则从"JSON"到"可执行 NFA"的完整链路：

```
JSON 字符串（GraphSpec）
   │  存在 DefaultPatternProcessor.patternStr
   ▼
DefaultPatternProcessor
   ├── id / version
   ├── patternStr ──┐
   └── function     │
                    │ getPattern(classLoader)
                    ▼
              CepJsonUtils.convertJSONStringToPattern
                    │
                    ▼
              GraphSpec（POJO）
                    │ GraphSpec.toPattern()
                    ▼
              Flink Pattern 链（临时对象，不缓存）
                    │ NFACompiler.compileFactory(pattern, ...)
                    ▼
              NFAFactory → NFA（每规则一个实例）
```

**各层职责：**

| 层 | 对象 | 生命周期 | 是否可序列化 |
|---|---|---|---|
| 定义层 | `DefaultPatternProcessor` | 长期（JM 持有、随 RPC 传输） | ✅ |
| 中间层 | `GraphSpec` | 临时 | ✅（Jackson） |
| 编译输入 | `Pattern` | 临时 | 不依赖序列化 |
| 执行层 | `NFA` | 与规则上下文同生命周期 | ❌（运行时对象） |

---

## 7. 本篇小结

### 7.1 一句话总结

`DynamicCEP` 是一个极薄的装配门面，用 `transform` 插入自定义算子；`PatternProcessor` 把"一条规则"抽象成 **id + version + JSON + 回调函数**，而 JSON 而非 `Pattern` 的选择，是整套热更新能力的数据基础。

### 7.2 自测问题

1. `dynamicPatterns` 为什么要显式传入 `TypeSerializer`？
2. 为什么非 KeyedStream 要 `keyBy(NullByteKeySelector)` + `forceNonParallel()`？代价是什么？
3. 为什么用 `transform` 而不是 `process`？
4. `PatternWrapper` 解决了什么访问控制问题？为什么不用反射？
5. `PatternProcessor` 为什么要继承 `Serializable`？两条传输路径分别是什么？
6. `DefaultPatternProcessor.getPattern()` 为什么不缓存结果？
7. `function` 为 null 会在哪里引发 NPE？

### 7.3 下一篇

→ `03-rule-json-serialization.md`：`GraphSpec` 如何把 JSON 还原成 Flink `Pattern` 链，`toPattern()` 逐行拆解。
