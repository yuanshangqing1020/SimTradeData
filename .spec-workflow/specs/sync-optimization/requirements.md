# Requirements Document

## Introduction

sync-optimization 旨在优化 SimTradeData 同步系统的性能和资源利用效率。当前同步系统已经完成基本功能实现（100% PTrade API支持、零冗余架构、完整数据同步），但在性能方面仍有显著优化空间。根据性能基准测试结果（task 8.3），批量模式同步100只股票需要约82.9秒，逐个模式需要约912μs/股票，这表明存在性能瓶颈。本spec专注于识别和解决这些瓶颈，提升同步速度、降低内存使用、优化并发处理，最终达到 >500条/秒 的性能目标。

关键优化领域:
- **BaoStock连接管理**: 当前存在频繁重连问题，影响同步速度
- **并发处理优化**: 流水线模式可以进一步优化批次大小和线程数
- **数据库批量操作**: 可以减少事务开销，提升写入性能
- **内存使用优化**: 大规模同步时需要控制内存峰值
- **缓存策略**: 减少重复计算和数据库查询

## Alignment with Product Vision

本feature与 product.md 的以下目标直接对齐:

1. **高性能查询引擎** (product.md 第24行): 同步性能直接影响数据可用性，优化同步速度使数据更及时可用
2. **完整数据同步系统** (product.md 第26行): 在保持功能完整性的同时提升性能
3. **提升性能** (product.md 第34行): 达成 >500条/秒 同步速度的业务目标
4. **性能优先原则** (product.md 第51行): 在保证数据完整性前提下优化性能

Success Metrics对齐:
- 查询性能提升: 200-500% (product.md 第43行) → 同步性能也需达成类似提升
- 测试覆盖率: 100% (product.md 第40行) → 优化后保持100%测试覆盖率

Technical Requirements对齐 (tech.md):
- 数据同步速度: >500条/秒 (tech.md 第165行)
- 内存使用: <2GB 正常运行 (tech.md 第166行)
- 并发支持: 多线程查询 (tech.md 第168行)

## Requirements

### Requirement 1: BaoStock连接池优化

**User Story:** 作为系统开发者，我希望优化BaoStock连接管理，避免频繁重连，从而提升数据源访问速度和稳定性。

#### 业务价值
- 减少连接开销：每次重连需要约1-2秒，大规模同步时影响显著
- 提升稳定性：避免因频繁重连导致的会话超时错误
- 改善用户体验：同步速度更快，等待时间更短

#### 技术背景
从 `baostock_adapter.py` (第33-106行) 可以看到，当前实现的问题:
1. 使用 `_session_timeout = 600秒` 检测超时
2. 每个方法调用都执行 `_ensure_connection()` 检查
3. 缺少连接池和会话复用机制
4. 没有预热机制，首次调用慢

#### Acceptance Criteria

1. WHEN 数据源管理器启动 THEN 系统 SHALL 建立连接池，预创建1-2个BaoStock会话连接
2. WHEN API调用需要连接 THEN 系统 SHALL 从连接池获取可用连接，而不是每次重新连接
3. WHEN 连接空闲超过配置时长(如5分钟) THEN 系统 SHALL 自动回收并创建新连接
4. WHEN 连接池无可用连接 THEN 系统 SHALL 等待或创建临时连接，不阻塞其他线程
5. IF 连接在使用中出现超时错误 THEN 系统 SHALL 标记该连接为无效并从池中移除

### Requirement 2: 数据库批量写入优化

**User Story:** 作为系统开发者，我希望优化数据库批量写入性能，减少事务开销，从而提升大规模同步速度。

#### 业务价值
- 提升同步速度：批量写入比逐条写入快3-10倍
- 降低磁盘IO：减少fsync次数
- 支持大规模数据：5000+股票同步不卡顿

#### 技术背景
从 `incremental.py` (第410-461行 `smart_backfill_symbol`) 可以看到:
1. 逐条执行UPDATE语句 (第456行)
2. 没有使用事务批量提交
3. 缺少批量INSERT支持

#### Acceptance Criteria

1. WHEN 同步单个股票数据 THEN 系统 SHALL 将多条记录合并为一个批量INSERT或UPDATE操作
2. WHEN 批量操作记录数 >100条 THEN 系统 SHALL 分批提交，每批100条
3. WHEN 批量写入失败 THEN 系统 SHALL 回滚整个批次，记录错误，不影响其他批次
4. WHEN 使用批量写入 THEN 系统 SHALL 使用executemany而不是多次execute
5. IF 数据库支持 THEN 系统 SHALL 使用WAL模式提升并发写入性能

### Requirement 3: 并发流水线优化

**User Story:** 作为系统开发者，我希望优化增量同步的并发流水线，找到最佳的批次大小和线程数配置，从而最大化同步吞吐量。

#### 业务价值
- 提升同步速度：充分利用CPU和IO资源
- 降低延迟：减少同步等待时间
- 节省资源：避免过度并发导致的资源浪费

#### 技术背景
从 `incremental.py` (第898-996行 `_sync_pipeline`) 可以看到:
1. 硬编码批次大小为5 (第958行)
2. 硬编码最大线程数为2 (第959行)
3. 缺少自适应调整机制
4. 没有性能监控和优化建议

当前配置 (第46-50行):
```python
self.batch_size = self.config.get("sync.batch_size", 50)
self.max_workers = self.config.get("sync.max_workers", 3)
```

#### Acceptance Criteria

1. WHEN 系统启动 THEN 系统 SHALL 检测CPU核心数和可用内存，推荐最佳并发配置
2. WHEN 同步开始 THEN 系统 SHALL 使用配置的批次大小和线程数，默认值应基于性能测试优化
3. WHEN 同步进行中 THEN 系统 SHALL 监控线程利用率和内存使用，记录性能指标
4. IF 内存使用超过阈值(如1.5GB) THEN 系统 SHALL 动态降低批次大小或线程数
5. WHEN 同步完成 THEN 系统 SHALL 输出性能统计，包括吞吐量、平均延迟、资源利用率

### Requirement 4: 智能缓存机制

**User Story:** 作为系统开发者，我希望实现智能缓存机制，减少重复计算和数据库查询，从而提升同步效率。

#### 业务价值
- 减少计算开销：避免重复计算衍生字段
- 降低数据库负载：缓存常用查询结果
- 提升响应速度：缓存命中率高时显著提速

#### 技术背景
从 `sync/manager.py` 和 `incremental.py` 可以看到:
1. 缺少股票元数据缓存 (如上市日期、市场、状态)
2. 交易日历每次都查数据库 (incremental.py 第814-833行)
3. 最后数据日期频繁查询 (incremental.py 第545-572行)
4. 市场判断有缓存但可以优化 (manager.py _determine_market)

#### Acceptance Criteria

1. WHEN 系统启动 THEN 系统 SHALL 预加载活跃股票列表、交易日历(最近2年)到内存缓存
2. WHEN 查询股票元数据 THEN 系统 SHALL 优先从缓存读取，缓存未命中才查数据库
3. WHEN 缓存数据更新 THEN 系统 SHALL 自动刷新缓存，保持数据一致性
4. WHEN 缓存占用内存 >500MB THEN 系统 SHALL 淘汰最少使用(LRU)的缓存项
5. IF 配置禁用缓存 THEN 系统 SHALL 直接查询数据库，不使用缓存

### Requirement 5: 性能监控和分析

**User Story:** 作为系统开发者，我希望有完善的性能监控和分析工具，识别性能瓶颈，指导优化方向。

#### 业务价值
- 快速定位问题：实时监控关键性能指标
- 数据驱动优化：基于实际数据调整配置
- 持续改进：跟踪优化效果

#### 技术背景
当前缺少:
1. 详细的性能日志 (每个阶段耗时)
2. 资源使用监控 (CPU、内存、磁盘IO)
3. 瓶颈分析工具
4. 性能报告生成

#### Acceptance Criteria

1. WHEN 同步开始 THEN 系统 SHALL 记录开始时间、初始资源使用
2. WHEN 每个阶段完成 THEN 系统 SHALL 记录阶段名称、耗时、处理记录数、资源使用
3. WHEN 同步完成 THEN 系统 SHALL 生成性能报告，包含各阶段耗时占比、吞吐量、资源峰值
4. IF 某阶段耗时 >总时长的50% THEN 系统 SHALL 在报告中标记为瓶颈，给出优化建议
5. WHEN 生成报告 THEN 系统 SHALL 支持JSON和可读文本两种格式

## Non-Functional Requirements

### Code Architecture and Modularity
- **Single Responsibility Principle**:
  - 连接池管理独立模块 (`connection_pool.py`)
  - 批量写入优化在 `database/batch_writer.py`
  - 缓存管理独立模块 (`performance/cache_manager.py`)
  - 性能监控独立模块 (`monitoring/performance_monitor.py`)
- **Modular Design**:
  - 优化模块可插拔，不影响现有功能
  - 支持通过配置启用/禁用优化特性
- **Dependency Management**:
  - 优化模块依赖现有核心模块，不引入新外部依赖
- **Clear Interfaces**:
  - 连接池提供统一的连接获取和释放接口
  - 批量写入器提供统一的批量操作接口

### Performance
- **同步速度目标**:
  - 批量模式: >500条/秒 (当前约1.2条/秒，需提升417倍)
  - 逐个模式: >100条/秒 (当前约109条/秒，已接近目标)
- **内存使用目标**:
  - 正常运行: <500MB
  - 峰值(5000只股票同步): <1.5GB
- **响应时间**:
  - 连接池获取连接: <10ms
  - 缓存查询命中: <1ms
  - 批量写入100条记录: <50ms

### Security
- **连接安全**:
  - 连接池中的连接必须验证有效性
  - 超时连接自动清理
  - 不泄漏连接资源
- **数据完整性**:
  - 批量写入失败必须回滚
  - 缓存数据必须与数据库一致
  - 优化不能影响数据准确性

### Reliability
- **错误处理**:
  - 连接池耗尽时提供降级策略
  - 批量写入失败能回退到逐条写入
  - 缓存失效不影响功能，只降低性能
- **监控告警**:
  - 性能异常(如吞吐量 <10条/秒)触发告警
  - 资源使用超限(如内存 >1.5GB)触发告警
- **测试覆盖**:
  - 保持100%测试覆盖率
  - 新增性能测试用例
  - 压力测试验证优化效果

### Usability
- **配置简单**:
  - 提供合理的默认配置
  - 配置项有清晰说明和推荐值
- **日志清晰**:
  - 性能日志易于理解
  - 瓶颈分析报告有具体建议
- **向后兼容**:
  - 优化不破坏现有API
  - 配置兼容旧版本
