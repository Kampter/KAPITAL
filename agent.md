### 项目简介
1. 使用uv这个python管理工具
2. 使用 picows 这个低延迟的websokect library
3. 获取 okx 中 hype_usdt 的 trader 和 books 的频道数据
4. 需要一个低延迟的延迟性能监控工具，不会影响数据获取和后续处理计算
5. 直接打印关键信息，不需要持久化存储

### v0.1需求
1. 使用 orjson 解析 picows 来的数据
2. 修改延迟统计到us级别
3. 使用 orjson + Numpy ring buffer
4. 自动计算每帧延迟（OKX ts → 本地 ts）
5. 输出实时 Δlatency 指标（p50/p95）
6. 修复trader数据
输出：main.py

### v0.2需求
1. okx_latency_bench.py是我在 aws 香港的测试脚本，如有需要你可以调整这个代码
2. optimize.sh是 aws香港的一键优化脚本
3. 我的云主机：c7i.large，ubuntu 24.04 lts