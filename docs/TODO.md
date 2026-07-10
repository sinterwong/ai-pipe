# AI Pipe 未来规划 TODO（post-v0.5.0）

> 前一版 TODO（2026-07 架构审计路线图，6 阶段 35 项）已于 v0.4.0 全部完成并随
> v0.5.0 补齐评审差距，文档见 git 历史 v0.4.0 tag。本文件规划 v0.5.0 之后的
> 演进方向：所有条目均为**增强项**，无已知正确性欠账。
>
> 规则不变：每完成一项勾选并注明提交；条目动手前先确认前提仍成立。

## 状态图例

- [ ] 未开始　[x] 已完成　（进行中的条目在行尾标注 `(WIP)`）

---

## 近期（v0.6 候选）

- [x] **F1. JSON 构图加载器**：基于 `NodeRegistry`（v0.4.0 P6.1）实现
  `Result<Graph> loadGraphFromJson(...)`——节点（type/name/config）+ 边 +
  引擎选项的声明式描述。nlohmann/json 已在 3rdparty 中（目前仅测试用），
  需决策：作为可选组件（`AI_PIPE_WITH_JSON`）保持核心零依赖。
  ——已按可选组件方案落地（`AI_PIPE_WITH_JSON`，默认 OFF，OFF 时编译为返回
  `InvalidConfiguration` 的 stub，链接兼容不变），提供
  `loadGraphFromJson` / `loadPipelineFromJson` 及文件变体，schema 严格校验
  （未知键报错），文档 `docs/JSON_Graph_Loader.md`。提交 9a1d721。
- [ ] **F2. clang-tidy 转必过门禁**：先分诊咨询 job 的现有告警，固化
  `.clang-tidy` checks 集（排除误报类），修净后移除 `continue-on-error`。
- [ ] **F3. KeepLatest 语义细化**：明确"保留最新 N 帧"在并发生产者下的精确
  语义边界（当前实现在竞争窗口内可能短暂超出 N），补契约文档与并发测试。
- [x] **F4. aarch64 交叉编译 CI**：现有 `platforms/linux/aarch64.cmake` 依赖
  外部 NDK 式 clang 工具链；补一个基于 `g++-aarch64-linux-gnu` 的通用
  toolchain 文件 + CI build-only job，守住嵌入式可移植性宣称。
  ——新增 `platforms/linux/aarch64-gnu.cmake`（零外部输入）、`aarch64`
  CMake preset、CI `build-aarch64` job（WERROR + JSON 加载器一并交叉编译，
  `file` 校验产物确为 ARM aarch64）。提交 `feat: aarch64 cross-compile
  toolchain + CI build-only gate (F4)`。

## 中期（v0.7+）

- [ ] **F5. 多流同步**：帧对齐目前仅按 FrameId（P4.1），`stream_id` 已在
  DataPacket 头部但不参与对齐。多摄像头场景需要 (stream, frame) 二元对齐
  或基于 `TimestampFrameMetadata` 的时间戳容差对齐（API 已定义未接线）。
- [ ] **F6. Join 对齐超时降级（可选项）**：审计目标中有意搁置的一项——当前
  落后分支由重调度机制自然等待、永失配对帧直接丢弃。为"宁要不完整数据也
  不要等待"的场景提供可配置的等待上限与降级策略（部分输入执行/跳帧）。
- [ ] **F7. 执行追踪 hooks**：per-frame span 事件（入队/调度/执行/传播）经
  可注入 sink 输出，支持导出 chrome://tracing / Perfetto 格式，把第 9 章
  统计从聚合数字升级为可视化时间线。
- [ ] **F8. 动态插件加载**：`dlopen` 扫描目录自动注册节点（注册宏在共享库
  静态初始化时生效，NodeRegistry 基础已具备），需定义插件 ABI 边界与
  版本握手。
- [ ] **F9. 类型化数据通路第二阶段**：`process()` 的 `PortDataMap`
  （`std::map<string, ptr>`）是热路径上最后的字符串键容器（每次执行构造）。
  替换为索引化端口数组的新 process API 是**重大 API 变更**——先在真实负载
  上 profile 证明收益再动手，避免为微优化破坏 API。

## v1.0 / API 冻结条件

- [ ] **F10. 真实业务负载验证**：至少一个生产场景 7×24 长期运行（审计结论：
  框架的下一步考验应来自真实负载而非继续内部打磨）。
- [ ] **F11. ABI/SemVer 政策文档**：冻结前明确公共头文件清单、ABI 兼容承诺
  与弃用流程。
- [ ] **F12. 公共 API 终审**：逐头文件 review（遗留的抛异常兼容层
  `getParam`/`throwIfCancelled` 是否随 1.0 移除或永久保留需定稿）。

## 长期观察项（性能报告跟踪，见 docs/Performance_Report.md §5.4）

- [ ] **F13. 高 worker 数轻量帧场景的调度竞争**：worker 数远超有效并行度时
  流式吞吐下降；候选方向是 StreamScheduler 自适应并行度。
- [ ] **F14. 大队列容量的吞吐衰减**：容量增大时环形缓冲缓存局部性变差；
  候选方向是自适应容量或分段缓冲。
- [ ] **F15. 深线性管线微批处理**：线性链数据依赖限制强扩展性；帧级流水线
  重叠（多帧同时处于不同 stage）可突破，但会改变批模式完成语义，需设计。

