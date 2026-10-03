# Autocomplete Engine（输入联想服务）

逐字输入场景下的候选词联想服务：支持前缀匹配与词内匹配，按历史热度与最近使用时间对候选做综合排序；索引增量更新/删除与查询并发进行，查询永远读到某个已提交的完整版本；服务重启后状态与最后一次已提交状态一致。

## 目录

- [快速开始](#快速开始)
- [排序语义](#排序语义)
- [一致性边界](#一致性边界)
- [归一化规则](#归一化规则)
- [租户隔离](#租户隔离)
- [配置项](#配置项)
- [HTTP API](#http-api)
- [批量导入](#批量导入)
- [测试](#测试)

## 快速开始

```bash
go build -o autocomplete-server ./cmd/autocomplete-server
./autocomplete-server -addr :8080 -data-dir ./data
```

```bash
# 创建租户
curl -X PUT http://localhost:8080/v1/tenants/t1

# 写入候选（热度/次数增量累加）
curl -X POST http://localhost:8080/v1/tenants/t1/candidates \
  -d '{"term":"Coffee","heat":5,"uses":3}'

# 查询（大小写不敏感，展示保留原词形）
curl 'http://localhost:8080/v1/tenants/t1/search?q=COFF'
# {"matches":[{"term":"Coffee",...}],"version":2}

# 批量导入（同词条去重累加）
curl -X POST http://localhost:8080/v1/tenants/t1/import \
  --data-binary $'latte,1,1\nlatte,1,1\nmocha,2,2\n'
```

## 排序语义

每个候选词的得分为：

```
age = now - LastUsed                       // 距最近一次使用的时间
heatDecay = 2^(-age / HeatHalfLife)        // 热度衰减
recDecay  = 2^(-age / RecencyHalfLife)     // 新鲜度衰减
score = Heat * heatDecay + RecencyWeight * recDecay
```

- **历史热度**（`Heat`，导入/更新累加）随时间按指数半衰期衰减，半衰期由 `HeatHalfLife` 配置（默认 30 天）。
- **新鲜度**由最近一次使用时间 `LastUsed` 决定，衰减更快（`RecencyHalfLife` 默认 3 天），权重 `RecencyWeight` 默认 1.0。
- **时间来源**：衰减基准 `now` 来自注入的 `Clock` 接口（生产为真实时钟，测试可用冻结时钟）。所有时间判定都基于该时钟，因此衰减基准明确、可复现。
- **确定性**：排序键为 `(score 降序, term 升序)`。同一份数据、同一份配置、同一个时钟读数下，多次查询返回完全一致的顺序。
- 匹配方式：归一化后的词包含归一化后的查询串即命中（前缀匹配是词首包含，词内匹配是任意位置包含，二者统一为包含判断）。

## 一致性边界

### 读写并发（MVCC）

- 已发布的快照**不可变**：写入时复制（copy-on-write）受影响租户的视图，在副本上应用变更，再通过原子指针切换发布新版本。
- 查询只读取原子指针，**不获取写锁**，因此慢查询不会阻塞写入；写入也不会阻塞查询。
- 任一时刻的查询只能读到某个已提交的完整版本：候选对象一旦写入即不可变，更新总是产生新对象，不存在字段级混合，也不会读到写了一半的候选。
- 写入失败（配额超限、参数非法、WAL 落盘失败）在发布前返回错误，已提交版本不受污染。

### 持久化与恢复（WAL + 检查点）

- 每次写入先写 **预写日志**（`wal.log`）：`begin / put / delete / commit` 事务标记，每条记录带 CRC32 校验；`commit` 标记 `fsync` 后才发布内存快照。
- **只有带 commit 标记的事务会被回放**：崩溃导致的未提交事务（有 begin/put 无 commit）在恢复时被忽略；CRC 校验失败的撕裂记录及其后续内容被忽略。
- 事务 ID 单调递增且持久化到检查点标记；恢复时只回放事务 ID 大于检查点的已提交事务，**不重复应用、不跳过**。
- 检查点（`snapshot.json` + `checkpoint.json`）原子生成（临时文件 + rename + fsync），随后截断 WAL，保证内存与磁盘占用都有界。崩溃发生在任意时刻都可恢复到一致状态。
- 服务正常关闭时自动做一次检查点。

### 批量导入

- 一次导入是**单个事务**：先在内存中按归一化键去重累加（同批次重复词条热度与次数累加，不覆盖），再写 WAL 事务，最后原子发布。
- 导入中途失败（非法行等）不产生任何已提交数据；崩溃后恢复同样看不到未提交的导入。
- 导入源支持本地文件（`FileSource`，逗号/制表符分隔 `term,heat,uses`）与内存管道（`PipeSource`，channel 馈入）。

### 配额

- 每租户可配置候选数上限（`MaxCandidates`）。超限写入返回 `ErrQuotaExceeded`，不污染已提交版本。

## 归一化规则

索引与查询使用完全相同的归一化键（`normalize.Key`）：

1. **NFC 组合**：组合字符与基字符合成（如 `e` + 锐音符 → `é`）；
2. **全半角折叠**：全角 ASCII/标点 → 半角；
3. **大小写折叠**：full case folding（如 `ß` → `ss`），匹配对大小写不敏感。

展示始终保留原始词形（`Term` 字段永不变更）。同一个归一化键再次写入时，展示形保留首次写入的词形。

输入校验（非法/超长明确拒绝，原因可区分）：

| 错误 | 触发条件 |
| --- | --- |
| `ErrEmptyTerm` / `ErrEmptyQuery` | 空输入 |
| `ErrTermTooLong` / `ErrQueryTooLong` | rune 数超过 `MaxQueryLen` |
| `ErrInvalidTerm` / `ErrInvalidQuery` | 非法 UTF-8 或含控制字符 |
| `ErrNegativeDelta` | 热度/次数增量为负 |

## 租户隔离

- 所有数据按租户物理分命名空间；每个引擎操作都限定在单一租户内，API 不存在跨租户访问路径。
- 对未注册租户的读写返回 `ErrTenantNotFound`；租户维度的统计（候选数、总次数）与配额互不串扰。

## 配置项

| 配置 | 命令行参数 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `DataDir` | `-data-dir` | `./data` | 持久化目录；空字符串表示纯内存（不持久化） |
| `MaxResults` | `-max-results` | `20` | 单次查询最多返回的候选数（超出按排序截断） |
| `ScanLimit` | `-scan-limit` | `10000` | 单次查询最多扫描的候选数，超出返回 `ErrScanLimit` |
| `QueryTimeout` | `-query-timeout` | `200ms` | 查询墙钟超时，超出返回 `ErrQueryTimeout`（不返回半成品） |
| `MaxQueryLen` | `-max-query-len` | `64` | 词/查询的最大 rune 长度 |
| `HeatHalfLife` | `-heat-half-life` | `30*24h` | 热度衰减半衰期 |
| `RecencyHalfLife` | `-recency-half-life` | `3*24h` | 新鲜度衰减半衰期 |
| `RecencyWeight` | `-recency-weight` | `1.0` | 新鲜度加分权重 |
| `Clock` | — | 真实时钟 | 衰减时间源（可注入冻结时钟用于测试） |

错误均为可区分的具名错误：`ErrQueryTimeout`、`ErrScanLimit`、`ErrQuotaExceeded`、`ErrTenantNotFound`、`ErrTermNotFound` 等。HTTP 状态码映射：400（参数错误）、404（租户/词不存在）、409（租户已存在）、429（配额超限）、503（超时/扫描超限）。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | `/v1/tenants/{id}` | 创建租户，body `{"max_candidates":N}` |
| GET | `/v1/tenants/{id}` | 租户统计（候选数、总次数） |
| POST | `/v1/tenants/{id}/candidates` | 写入/累加，body `{"term":"...","heat":f,"uses":n}` |
| DELETE | `/v1/tenants/{id}/candidates/{term}` | 删除词 |
| GET | `/v1/tenants/{id}/search?q=...&limit=N` | 联想查询 |
| POST | `/v1/tenants/{id}/import` | 批量导入，body 为 `term,heat,uses` 文本（逗号或制表符分隔，`#` 开头为注释） |
| GET | `/healthz` | 健康检查 |

## 测试

```bash
go test ./...            # 全部测试
go test -race ./...      # 含数据竞争检测
```

覆盖场景：归一化规则、前缀/词内匹配、确定性排序、热度衰减（冻结时钟）、重复更新累加、并发读写（-race）、配额、租户隔离、输入校验、批量导入去重累加与失败回滚、WAL 已提交/未提交区分、撕裂记录、检查点与重启恢复、超时与扫描上限、HTTP API 端到端。
