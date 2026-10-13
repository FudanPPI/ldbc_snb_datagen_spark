# LDBC SNB Datagen (Spark) — 倾斜增强版

基于官方 [LDBC SNB Datagen (Spark)](https://github.com/ldbc/ldbc_snb_datagen_spark) 的**增强改造版本**。在完整保留原生 LDBC SNB Schema、生成规范与基准兼容性的前提下，通过参数化增强模块，向 TB 级异构社交网络图中注入真实业务典型病态特征，用于分布式图计算引擎、图数据库、分布式调度等系统的极限性能评测。

> **设计原则**：原生生成逻辑零侵入 —— 所有增强特征以独立 Transform 实现（作用于生成的中间数据 IR），默认全部关闭（行为与原生版完全一致），通过 CLI 参数逐项开启；支持"原生基线 / 增强版"双数据集对照实验。

详细设计文档见 [docs/ENHANCE.md](docs/ENHANCE.md)。

## 增强特性

五大增强模块，全部独立开关、参数可调：

### 1. 分区负载偏斜（Partition Skew）

原生 SNB 节点 ID 随机分布、分片天然均衡，无法模拟集群热点。本模块使用**确定性双射 ID 重映射**（零 shuffle、零 Action，纯列表达式），把超级节点、热人及其全部关联内容（knows 边、帖子、评论、点赞、论坛成员）聚集到 [0, N/K) 热分区，构造「高热分区 / 普通分区 / 空分区」差异化负载：

```ini
enhance.remap.enabled      = true
enhance.remap.hotEvery     = 20    # 每 20 个 Person id 取 1 个为热人
```

### 2. 拓扑结构倾斜（Topology Skew）

| 特性 | 说明 | 关键参数 |
|---|---|---|
| 超级节点 | 极小比例 Person 获得海量 knows 边（度数放大 100~10000 倍），模拟社交大 V / 锁竞争热点 | `supernode.rate=0.0001`、`supernode.minExtra`、`supernode.maxExtraDegreeMul` |
| 热点内容 | 超级节点发布的帖子/评论获得海量点赞、回复、标签边（热点遍历、边拆分调度） | `hotmsg.enabled` |
| 多尺度连通分量 | 注入大量单点/小规模孤立连通分量（预留 ID 带），测试 WCC、BFS 对碎片化拓扑的适配 | `smallcomp.count`、`smallcomp.cliqueMax` |
| 社区碎片化 | 随机切断部分 knows 边，打破单一连通图 | `fragment.dropKnowsRatio` |
| 非对称单向边 | 追加单向 knows 关系，制造异构边分布不均 | `unidir.flipRatio` |
| R-MAT 子图 | 预留节点带上融合 Graph500 风格幂律子图（PageRank/BFS/SSSP 对比） | `rmat.enabled` |

### 3. 属性 Zipf 长尾倾斜（Attribute Skew)

将 `locationIP`、`browserUsed` 及 Tag 关联边（HasInterest / PostHasTag / CommentHasTag / ForumHasTag）从均匀分布改造为标准 **Zipf 分布**——少数值高频出现、大量值稀疏唯一，测试索引过滤、热点属性缓存命中率：

```ini
enhance.zipf.enabled = true
enhance.zipf.s       = 1.1
enhance.zipf.attrs   = locationIP,browserUsed,tag
```

### 4. 时序突发负载（Temporal Burst）

- **突发写入**：将部分近期活动的时间戳压缩进短时窗口，模拟热点事件瞬时点赞/发帖爆发（测 WAL 吞吐、事务冲突）；
- **超级节点点赞风暴**：热帖的点赞时间集中在极小窗口内（MVCC / 分布式锁竞争场景）；
- **冷热分层**：按比例把活动时间压入「95% 冷数据 / 5% 热数据」两个时间带，模拟存储分层。

```ini
enhance.burst.enabled   = true
enhance.burst.ratio     = 0.2
enhance.hotcold.split   = false
```

### 5. 异常脏数据注入（Dirty Data）

精准模拟线上数据异常场景，全部占比可调，测试引擎容错、数据校验、脏数据过滤能力：

| 类型 | 注入方式 | 参数 |
|---|---|---|
| 空值 | 消息 content / imageFile / language 置 NULL | `dirty.nullContentRatio` |
| 空串 | 非空字符串属性置 "" | `dirty.emptyContentRatio` |
| 自环边 | knows 边 P1 == P2 | `dirty.selfLoopRatio` |
| 重复边 | 完全重复的边行 | `dirty.dupEdgeRatio` |
| 悬空外键 | likes 指向不存在预留 ID 带 | `dirty.danglingLikeRatio` |
| 非法值 | 非法 IP `999.999.999.999` | `dirty.invalidIpRatio` |
| 越界时间 | 时间戳 0 / 2100 年 | `dirty.outOfRangeDateRatio` |

## 快速开始

### 方式一：容器（推荐）

镜像自包含 JRE 11 + Spark 3.2.1 + datagen jar + 全部增强参数文件与校验脚本，可在任何有 Docker 的机器上直接生成数据：

```bash
# 构建镜像（下载源仅 github.com / repo1.maven.org / archive.apache.org；
# Docker Hub 不可达时自动改用 cdimage.ubuntu.com 的最小 rootfs 作基础镜像）
./scripts/build-image.sh                 # 产出 ldbc/datagen-enhance:latest

# 原生模式：SF1 全量 CSV 输出到宿主 ./out
docker run --rm -v "$(pwd)/out:/out" ldbc/datagen-enhance \
  --scale-factor 1 -f csv -n 8 --output-dir /out

# 增强模式：全特性参数文件（镜像内置 /opt/enhance/conf/）
docker run --rm -v "$(pwd)/out:/out" \
  -e DRIVER_MEMORY=8g -e MASTER='local[8]' \
  -e SPARK_CONF="spark.local.dir=/tmp/sparktmp" \
  ldbc/datagen-enhance \
  --scale-factor 1 -f csv -n 8 --output-dir /out \
  --enhance-param-file /opt/enhance/conf/enhance-defaults.ini

# 结构校验（feature 痕迹、FK 完整性、行数守恒）
docker run --rm -v "$(pwd)/out:/out" --entrypoint python3 \
  ldbc/datagen-enhance /opt/enhance/scripts/verify_enhance.py /out
```

容器内资源通过环境变量控制：`MASTER`（默认 `local[*]`）、`DRIVER_MEMORY`（默认 `4g`）、`DRIVER_CORES`、`SPARK_CONF`（额外的 `-c k=v` 对）。自定义参数文件挂载进容器即可（`-v $PWD/my.ini:/my.ini`）。

### 方式二：本地构建

```bash
# 依赖：JDK 8/11（勿用 17）、sbt、Spark 3.2.x
./scripts/build.sh          # sbt assembly，产出 target/ldbc_snb_datagen_2.12-*-jar-with-dependencies.jar
```

### 运行（原生模式）

```bash
spark-submit --class ldbc.snb.datagen.LdbcDatagen \
  target/ldbc_snb_datagen_2.12-0.6.0-enhance-jar-with-dependencies.jar \
  --scale-factor 1 --format csv --mode raw --output-dir out_sf1_native
```

### 运行（增强模式）

```bash
spark-submit --class ldbc.snb.datagen.LdbcDatagen \
  target/ldbc_snb_datagen_2.12-0.6.0-enhance-jar-with-dependencies.jar \
  --scale-factor 1 --format csv --mode raw --output-dir out_sf1_enhanced \
  --enhance-param-file conf/enhance-defaults.ini
# 或命令行直接传参：
#   --enhance-params enhance.enabled=true,enhance.remap.enabled=true,enhance.supernode.rate=0.001
```

- `--enhance-param-file <file>`：增强参数文件（INI 格式 `key=value`），样例见 [conf/enhance-defaults.ini](conf/enhance-defaults.ini)（全关 + 注释文档）与 [conf/enhance-sf1000.ini](conf/enhance-sf1000.ini)（SF1000 全特性）；
- `--enhance-params k=v[,k=v...]`：命令行覆盖，优先级高于参数文件；
- 未指定时**完全等同原生行为**。

### 1TB 级生成（SF1000）

SF1000 规模下单阶段直出时，merged knows 边集（约 10.5 亿条）会占满 Old Gen
触发 GC 死亡螺旋（详见 [scripts/gen-sf1000-two-phase.sh](scripts/gen-sf1000-two-phase.sh)
头注释的失败分析）。推荐两阶段生成——每阶段独占整个堆：

```bash
./scripts/gen-sf1000-two-phase.sh /path/to/sf1000-out --verify
```

- **Phase 1**：GenerationStage 产出原生 IR（parquet）→ `<out>/native/`；
  knows 合并集以 `MEMORY_AND_DISK` 持久化，内存不足自动 spill 落盘；
- **Phase 2**：`--skip-generation --input-dir <out>/native` 仅运行
  TransformationStage（IdRemap → TopologySkew → Zipf → Burst → DirtyData）
  → `<out>/enhanced/`；
- `--verify`：结束后自动运行 Spark 版结构校验
  （[scripts/verify_enhance_spark.py](scripts/verify_enhance_spark.py)）。

默认 64GB 内存预算（24 线程 / 50g driver 堆 + 10g off-heap / 2400 shuffle
分区），可用环境变量覆盖：`THREADS`、`DRIVER_MEM`、`OFFHEAP_SIZE`、
`SHUFFLE_PARTS`、`SCALE_FACTOR`、`ENHANCE_PARAM_FILE`（默认
[conf/enhance-sf1000.ini](conf/enhance-sf1000.ini)）。SF100 已完成全流程
64GB 预算验证。

单阶段脚本 [scripts/gen-sf1000.sh](scripts/gen-sf1000.sh) 保留作为参考
（历次 OOM 调优记录见脚本头注释），大规模生成请使用两阶段脚本。

### 验证增强特征

```bash
python3 scripts/verify_enhance.py <enhanced-output-dir> [native-output-dir]
# 校验：预留带 Person 数、超级节点度数偏斜、热点回复、突发窗口、脏数据痕迹、
#       Zipf 偏斜、FK 完整性（故意脏行除外）、与原生基线的行数对照
```

SF1000 等大规模输出使用 Spark 版校验（行式版需全量读内存，仅适合 SF≤100）：

```bash
spark-submit scripts/verify_enhance_spark.py <enhanced-output-dir> --quick
```

## 增强实现架构

```
GenerationStage（原生，未改动）
    └── IR: <out>/graphs/parquet/raw/composite-merged-fk/<entity>
TransformationStage:
    GraphSource.read(IR)
      → EnhanceTransform(增强模块，新增)          ← IdRemap → TopologySkew → ZipfSkew → TemporalBurst → DirtyData
      → ExplodeAttrs → ExplodeEdges → [BI 转换] → ConvertDates → GraphSink(最终 CSV)
```

- 增强模块全部基于 Spark DataFrame 表达式 + `xxhash64` 确定性哈希（无 UDF、无窗口排序），可扩展至 SF1000（约 1062 万 Person、数十亿级边）；
- IR 生成一次后可反复只重跑 TransformationStage 调增强参数；
- 模块执行顺序固定：分区重映射先行（保证新增边/聚集基于重映射后的 ID），脏数据最后（避免被其它模块"修复"）。

## 兼容性说明

- 输出 Schema 与官方 LDBC SNB 完全一致（未增加扩展列），兼容官方交互式/BI 查询集与校验规则；
- 脏数据、悬空外键等特征会**故意**引入规范违规数据——这正是容错评测的测试点，加载器需知晓；
- 增强特征全部开启时，数据体量相对原生会有一定增长（超级节点边、小组件、R-MAT 子图）。

## 原生版本说明（简）

原生工具链的完整文档（Docker 镜像、AWS EMR、BI 模式细节、因子表等）参见[上游 README](https://github.com/ldbc/ldbc_snb_datagen_spark)。本仓库常用参数：

| 参数 | 说明 |
|---|---|
| `--scale-factor` | 规模因子（SF1 ≈ 1.06 万 Person；SF1000 ≈ 1062 万） |
| `--mode raw / bi` | 原始快照 / BI 快照+增量更新流 |
| `--format csv / parquet` | 输出格式（sep=`\|`，带 header） |
| `--oversize-factor` | 控制文件大小/数量 |
| `-n / --num-threads` | 并行度与产出文件数 |
| `--epoch-millis` | BI 模式时间戳用毫秒 long |

图 Schema 与官方一致：

![](https://raw.githubusercontent.com/ldbc/ldbc_snb_docs/dev/figures/schema-comfortable.png)

## Troubleshooting

* 本地运行报 `java.net.UnknownHostException: <hostname>`：把 `<hostname>` 加进 `/etc/hosts`（`127.0.1.1 <hostname>`）。
* Spark 磁盘不足：设置 `SPARK_LOCAL_DIRS` 指向大容量磁盘目录。
* SF1000 本地生成内存不足：提高 `--driver-memory` 并增大 `spark.sql.shuffle.partitions`（参考 `scripts/gen-sf1000.sh`）。
