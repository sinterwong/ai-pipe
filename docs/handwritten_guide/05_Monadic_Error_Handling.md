# 05. Monadic 错误处理系统 (Result<T> 与 Error)

在现代高性能 C++ 系统设计中，传统的错误处理往往陷入两难的尴尬境地：
1.  **抛出异常（Exceptions）**：虽然调用链清晰，但是在性能关键的热路径（Hot Path）上，一旦抛出异常，会导致极其严重的 CPU 栈回溯（Stack Unwinding）开销，吞吐量断崖式下跌。
2.  **返回 Boolean 状态码 + Out 传出参数**：代码中充斥着丑陋的指针传递，极易造成判空漏洞，且无法携带详细的上下文诊断信息。

AI Pipe 采用了**函数式编程中的单子（Monadic）思想**，手搓了一套零运行时开销（Zero Overhead）的 `Result<T>` 与 `Error` 错误处理机制，优雅地终结了异常的使用。

---

## 1. 核心设计原理

### 1.1 `Result<T>` 的 Monadic 设计哲学
一个 Monadic `Result<T>` 实质上是一个**带标记的联合体（Tagged Union）**，它在同一时刻要么包含一个合法的成功值 `T`，要么包含一个诊断错误 `Error`。

```
+-----------------------------------------------------------+
|                          Result<T>                        |
|                                                           |
|       +-------------------+       +-------------------+   |
|  Or   |      Value (T)    |  Or   |    Error Object   |   |
|       |  (Success Path)   |       |  (Diagnostic Context) |
|       +-------------------+       +-------------------+   |
+-----------------------------------------------------------+
```

1.  **零堆分配（Zero Allocation）**：
    `Result<T>` 应该直接将值 `T` 和 `Error` 存放在栈上（借助 `std::variant` 或手动联合体），绝不在成功路径上触发任何动态内存分配（Heap Allocation），从而保证极高的数据局部性。
2.  **声明式链式处理**：
    通过实现 `.map()`、`.and_then()`（C++23 std::expected 风格的 monadic 操作符），可以使用链式调用的方式流畅地处理层层依赖的 fallible 函数。
3.  **显示转换与显式检查**：
    由于 `Result<T>` 带有了 `[[nodiscard]]` 强约束，编译器会强迫调用方对潜在的出错情况进行显式处理，消灭了由于疏忽遗漏检查而引发的未定义行为。

---

## 2. 核心巧思与实现细节

### 2.1 零动态分配的高吞吐 Variant 设计
如何优雅地在同一个栈内存中存放 `T` 或 `Error`？

**实现巧思**：
利用 `std::variant<T, Error>`。在 C++20 中，`std::variant` 拥有极佳的硬件加速性能，它在物理布局上是一个紧凑的 `Union` 加上一个 `Index`（通常是 1 个字节的 Tag 区分是值还是错误）。
*   当获取值时，通过直接访问 Variant 中的 `T` 即可，开销等同于普通结构体字段读取。
*   对于 `Result<void>`（即不带返回值的操作，仅关注成功或失败），特化（Specialization）实现为 `std::optional<Error>`。如果没有值，代表成功；如果拥有可选的 `Error`，代表失败。

### 2.2 丰富上下文的 Error 诊断体
高性能的错误处理不仅要快，还要能在出错时让开发者一眼看清事故现场。

**实现巧思**：
设计 `Error` 结构体，让其同时携带：
1.  `ErrorCode`（枚举，大分类如 `InvalidArgument`、`QueueFull` 等，用于程序进行自动化逻辑重试分发）。
2.  `std::string message`（详细的出错上下文说明，包含具体参数值）。
3.  `std::string node_name`（在 AI Pipe 执行流中，记录具体是哪一个 DAG 节点产生了该错误，极大地加速了复杂管道中节点故障的排查定位）。

### 2.3 `[[nodiscard]]` 的契约式安全
如果一个函数可能失败，但是调用方竟然无视了其返回值、直接往下执行，这极易引发崩溃。

**实现巧思**：
在 `Result<T>` 类声明上方添加 `[[nodiscard]]`。
```cpp
template <typename T>
class [[nodiscard]] Result {
    // ...
};
```
当你在代码中直接：
`pipeline.pushInput("preprocess", data);`
而没有用 `auto res = ...` 承接时，现代编译器在编译期会抛出强烈的警告或错误，强制要求开发者必须进行结果校验。

---

## 3. 手搓实现参考骨架

你可以通过以下详尽的 C++20 骨架来实现：

```cpp
enum class ErrorCode {
    Ok = 0,
    InvalidArgument = 101,
    QueueFull = 301,
    NodeException = 401,
    InternalError = 501
};

class Error {
public:
    Error(ErrorCode code, std::string message, std::string nodeName = "")
        : m_code(code), m_message(std::move(message)), m_nodeName(std::move(nodeName)) {}

    ErrorCode code() const { return m_code; }
    const std::string& message() const { return m_message; }
    const std::string& nodeName() const { return m_nodeName; }

    std::string toString() const {
        std::string res = "[" + std::to_string(static_cast<int>(m_code)) + "] " + m_message;
        if (!m_nodeName.empty()) {
            res += " (at node: " + m_nodeName + ")";
        }
        return res;
    }

private:
    ErrorCode m_code;
    std::string m_message;
    std::string m_nodeName;
};

template <typename T>
class [[nodiscard]] Result {
public:
    // 构造成功值
    Result(T value) : m_data(std::move(value)) {}

    // 构造错误值
    Result(Error error) : m_data(std::move(error)) {}

    bool isOk() const { return std::holds_alternative<T>(m_data); }
    bool isError() const { return std::holds_alternative<Error>(m_data); }

    explicit operator bool() const { return isOk(); }

    const T& value() const {
        if (isError()) {
            throw std::runtime_error("Result contains error: " + std::get<Error>(m_data).toString());
        }
        return std::get<T>(m_data);
    }

    T& value() {
        if (isError()) {
            throw std::runtime_error("Result contains error: " + std::get<Error>(m_data).toString());
        }
        return std::get<T>(m_data);
    }

    const Error& error() const {
        return std::get<Error>(m_data);
    }

    // Monadic interface: value_or
    T valueOr(const T& defaultValue) const {
        if (isOk()) return std::get<T>(m_data);
        return defaultValue;
    }

    // Monadic interface: and_then
    template <typename F>
    auto andThen(F&& func) -> decltype(func(std::declval<T>())) {
        if (isOk()) {
            return func(std::get<T>(m_data));
        }
        return std::get<Error>(m_data); // 自动转换成下游对应的 Result 错误
    }

private:
    std::variant<T, Error> m_data;
};

// Result<void> 特化，用于仅表示成功/失败的函数
template <>
class [[nodiscard]] Result<void> {
public:
    Result() : m_error(std::nullopt) {}
    Result(Error error) : m_error(std::move(error)) {}

    bool isOk() const { return !m_error.has_value(); }
    bool isError() const { return m_error.has_value(); }

    explicit operator bool() const { return isOk(); }

    void value() const {
        if (isError()) {
            throw std::runtime_error("Result contains error: " + m_error->toString());
        }
    }

    const Error& error() const {
        return m_error.value();
    }

private:
    std::optional<Error> m_error;
};
```

在手搓整个 AI Pipe 时，你将感受到极其流畅的错误控制流：没有多余的 `try-catch` 块嵌套，代码扁平、整洁，而且在 CPU 性能诊断中，错误路径不会造成任何额外的时钟周期耗损。这是对高性能系统的至上敬意。
