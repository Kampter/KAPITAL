### 项目简介
1. 使用uv这个python管理工具
2. 使用 picows 这个低延迟的websokect library
3. 获取 okx 中 hype_usdt 的 trader 和 books 的频道数据
4. 需要一个低延迟的延迟性能监控工具，不会影响数据获取和后续处理计算
5. 直接打印关键信息，不需要持久化存储
6. 核心性能敏感逻辑通过 Rust + PyO3 暴露给 Python，全面替换原先的 Numba 路线

### v0.1需求
1. 使用 orjson 解析 picows 来的数据
2. 修改延迟统计到us级别
3. 使用 orjson + Rust ring buffer（PyO3 暴露简单 API，Python 侧只负责调度）
4. 自动计算每帧延迟（OKX ts → 本地 ts）
5. 输出实时 Δlatency 指标（p50/p95）
6. 修复trader数据
输出：main.py

### v0.2需求
1. okx_latency_bench.py是我在 aws 香港的测试脚本，如有需要你可以调整这个代码
2. optimize.sh是 aws香港的一键优化脚本
3. 我的云主机：c7i.large，ubuntu 24.04 lts

### v0.3需求
目前的来源是main.py，输出或者在同一个文件内(由你决定)实现下列功能
1. 轻量内存化 pipeline：使用 rust + ffi（PyO3 模块包含 ring buffer、特征聚合、逻辑回归）
2. 10ms/50ms/100ms短时间窗口
3. 少量核心因子imbalance_top1、spread、trade_volume_last_Tms
4. 概率信号用来计算多空，置信度用来计算仓位大小
5. 逻辑回归
6. 整理一下当前的项目实现，把优化和延迟测试放在一个目录，真正的量化流程代码放在一个目录

### v0.4 Rust FFI 迁移路线
1. 在 `quant` 目录下新增 `rust/` 子项目，使用 `maturin` 或 `setuptools-rust` 构建 PyO3 扩展；保持与 `uv` 的依赖管理兼容。
2. 首先实现一个无锁 ring buffer，封装 push/iter 逻辑，并提供批量导出 numpy array 的接口以兼容现有分析代码。
3. 将延迟统计和短周期特征计算（10ms/50ms/100ms 窗口聚合）迁移到 Rust，确保 Rust 侧直接输出我们需要的指标结构体，再映射为 Python dataclass。
4. 在 Rust 中实现轻量逻辑回归（在线更新 + 推理），通过 FFI 暴露 `fit_partial` 与 `predict_proba` 接口，替换原先的 Numba 加速函数。
5. Python 端只负责 I/O、调度和可视化，所有性能敏感路径保证在 Rust 中完成，测试层面覆盖 PyO3 接口与 Python 集成。

### v0.5需求
这是目前运行 main.py的输出
```
[signal] HYPE-USDT mid=42.870500 spread=0.001000 imbalance=-0.003 vol10=0.0000 vol50=0.0000 vol100=0.0000 prob=0.471 conf=0.058 dir=short latency=0us Δlatency p50=0us p95=0us
[signal] HYPE-USDT mid=42.869500 spread=0.001000 imbalance=0.187 vol10=0.0000 vol50=0.0000 vol100=0.0000 prob=0.470 conf=0.059 dir=short latency=0us Δlatency p50=0us p95=0us
```
1. 调整延迟的计算，目前都是0us，这是明显不对的，保证每段计算延迟的准确性，包括获取数据，解析数据，计算因子，下单通信的每一段的延迟
2. 加入过滤系统，过滤掉低概率，低置信的信号，过滤的阈值统一管理

### v0.51需求
1. 把v0.5中的参数，统一放到config文件中，统一管理
    min_confidence: float = 0.1
    min_probability_long: float = 0.55
    max_probability_short: float = 0.45
2. 我可能没有写全所有的参数，需要察看streaming.py里面的代码