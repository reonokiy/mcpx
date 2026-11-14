# 原生 Schema 编译与缓存分配测量

此记录测量校验器缓存实际释放的 Rust allocator 请求字节，用于了解编译后的内存放大。它不是 RSS、峰值内存、跨平台上限或性能回归门槛。

## 复现

```sh
cargo run -p mcpx-core --example schema_profile --release --locked > target/mcpx-schema-profile.json
```

测量程序位于 `crates/mcpx-core/examples/schema_profile.rs`，只有独立原生 example 和分配回归测试安装 instrumented System allocator；库没有全局 allocator。快照保存在 [schema-memory-native.json](schema-memory-native.json)，包含环境、原始字节、时间和缓存计数。

本次环境：Linux x86_64，AMD Ryzen 9 9950X，Rust 1.100.0，release，单线程。依赖版本来自 Cargo.lock，分配记录使用 stats_alloc 0.1.10。

## 方法

先预热共享 meta-schema 与 SHA 实现。每种形状单独建立 Catalog，执行一次冷绑定、释放绑定后重新绑定，再执行 100 次有效入参校验。重新绑定命中缓存，计数要求编译 1 次、命中 1 次。校验计时包含 JSON clone、深度/数字/字节检查及实际 Schema 校验。字节测长已使用有界 counting writer，不为测长另分配整个 JSON 文本；本快照随该实现重新运行。

释放所有应用 binding 后，读取 allocator 计数、清空缓存并再次读取。两次净请求字节之差是表中的释放字节，包含校验器及缓存 entry/order 元数据；不包含共享 registry/static、allocator 元数据和碎片。冷分配流量另行报告，不能把它当作保留量或峰值。stats_alloc 的 bytes_allocated/bytes_deallocated 已计入 realloc 增长/缩减，不再累加 bytes_reallocated。具体语义见 [Stats 文档](https://docs.rs/stats_alloc/0.1.10/stats_alloc/struct.Stats.html)。

每项时间仅一个观测样本，受调度、CPU 状态及预热影响；没有分位数或显著性推断。此程序只测原生 allocator，没有据此推断 Wasm 堆。

## 观测

| Schema 形状 | 根源字节 + registry 源计费 | 清空释放请求字节 | 比值 | 冷绑定 μs | 再绑定 μs |
| --- | ---: | ---: | ---: | ---: | ---: |
| 单字段字符串 | 85 | 2,164 | 25.46 | 21.28 | 1.87 |
| 256 整数字段 | 14,258 | 247,926 | 17.39 | 468.80 | 67.31 |
| 1000 字符串枚举 | 9,080 | 81,108 | 8.93 | 139.02 | 35.94 |
| 条件、组合与内部引用 | 347 | 9,136 | 26.33 | 45.42 | 2.50 |
| 32 正则字段 | 1,910 | 50,015 | 26.19 | 157.02 | 8.23 |
| 外部引用的 256 字段 | 14,457 | 248,829 | 17.21 | 163.70 | 0.37 |

registry 源计费包含其 JSON、URI 和每文档固定费用；因此外部引用行的分母不是精确的 JSON 字节。分子也包含缓存元数据，不能把比值解释为每个 Validator 对象单独的大小。完整原值和方法保存在 JSON 中。

128 项 LRU 限制缓存持有的校验器数量。它不会撤销 live binding/BoundArgs/continuation，也不约束调用方持有的 Arc 快照。底层 jsonschema 的 Validator 没有公开完整堆大小接口；源字节和这里的单次观测不足以实现可证明的编译内存上限。真实保留量/峰值预算、编译中止、CPU 工作预算、更多复杂引用和多个实际宿主的测量仍是完整技术方案的后续验收项。

## 策略版本 2：错误诊断分配复查

[独立测量结果](schema-policy-2-native.json) 由同一 release 示例在 Linux x86_64 / AMD Ryzen 9 9950X / Rust 1.100.0 下生成，策略版本为 2；原上方 JSON 保留为早先测量，不混用两批数据。新增比较在编译之外测量一次缺失必填字段校验：当前 mcpx 首个错误路径包含输入检查，旧路径使用同形状的 raw jsonschema validator，先 `iter_errors()` 再 `take(32)` 并复制指针。两者都构造有界的 mcpx Error，不格式化敏感实例正文。

| 缺失字段 | 旧策略请求分配字节 | 当前首个错误路径请求分配字节 |
| --- | --- | --- |
| 64 | 21,178 | 408 |
| 512 | 156,026 | 408 |
| 4096 | 1,234,810 | 408 |

这些是单次顺序样本的 allocator 请求流量，不是峰值 live heap、RSS、百分位或跨平台通用界。对应时间保留在 JSON，不据此宣布稳定速度倍率。原生回归在 4096 字段形状上设置宽松的 64 KiB 请求分配门槛，以捕获重新汇总数千条平面诊断的退化。库不安装全局 allocator；仪器仅在独立测试/示例中启用。

首个错误接口仍可能为 anyOf/oneOf 等组合构造内部诊断；正则预检/NFA/DFA/VM 限额也不等于整个校验器或其缓存的 allocator/CPU 硬预算。本批仅证明该平面错误形状的差异，完整资源隔离继续保留为未完成工作。
