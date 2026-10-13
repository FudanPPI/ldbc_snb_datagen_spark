# Skew-Enhancement Modules (F1–F6)

This fork of the LDBC SNB Datagen adds deterministic, Spark-native skew
enhancements on top of the raw IR graph. The goal is to produce benchmark
input that resembles real production graphs — partition skew, supernodes,
Zipf-distributed attributes, bursty write patterns and dirty data — so that
storage engines and query engines can be tested against realistic pressure
instead of the perfectly uniform data the native generator emits.

All modules are **off by default**: with the master switch disabled the
output is byte-identical to native Datagen (verified on all 18 tables).

## Pipeline

The enhancement runs as one `Transform` on the raw graph, after native
generation, in a fixed order:

```
IdRemap → TopologySkew → ZipfSkew → TemporalBurst → DirtyData
```

Order is meaningful: id remap must run first (all later logic operates on
remapped ids); dirty data runs last so no other module "repairs" dirty rows.

| Module | Feature | Mechanism |
|---|---|---|
| F1 | parameter framework | `EnhanceConfig`, CLI + ini file, all-off identity |
| F2 | partition skew | bijective person-id remap, hot band `[0, N/K)` |
| F3 | topology skew | supernodes, hot messages, small components, R-MAT, fragmentation, asymmetry |
| F4 | attribute skew | Zipf rewrite of low-cardinality attributes |
| F5 | temporal skew | burst windows, hot/cold split, supernode like storms |
| F6 | dirty data | null/empty content, invalid IP, bad dates, self-loops, duplicate edges, dangling likes |

## Design principles

- **Deterministic.** All randomness is `xxhash64(id, salt)` based — same
  input and parameters always produce the same output, on any cluster size
  and partition count.
- **Shuffle-free.** F2/F5/F6 are pure column expressions (bit arithmetic +
  hash); F3/F4 only need tiny broadcast joins or one driver-side aggregate.
- **Referentially honest.** Generated edges only reference ids that are
  guaranteed to exist (hot band, reserved bands); dangling FKs are produced
  exclusively and deliberately by F6 into a never-allocated id band
  (bucket 253), so "clean" and "dirty" are always distinguishable.
- **Reserved id bands.** Buckets 253 (dangling), 254 (hot-message replies),
  255 (small components + R-MAT) are never used by native ids.

## Modules

### F1 — Parameter framework

`--enhance-params 'k=v,k2=v2'` and/or `--enhance-param-file <ini>`; ini file
supports `key=value` / `key: value` and `#` comments. Unknown keys are
rejected. Precedence: CLI > file > defaults. See `conf/enhance-defaults.ini`
for the full annotated list.

> NOTE: `--enhance-params` values are comma-split by scopt, so multi-value
> parameters (e.g. `enhance.zipf.attrs=a,b`) must go through the param file.

### F2 — IdRemap (partition skew)

Native person ids are `formId(composeId(seq, creationDate), blockId)` with
`seq` dense in `[0, numPersons)`. The remap acts on `seq` bijectively:

- hot (`seq % K == 0`): `seq → seq/K`, bucket/blockId collapsed to 0 —
  a contiguous id band `[0, M)`, `M = ceil(N/K)`;
- cold: `seq → M + seq − ceil(seq/K)` with original bucket/blockId kept.

Downstream hash/id-range partitioning then concentrates all hot entities in
one partition. Knows pairs are re-normalised (`P1 ≤ P2`) after remap.

### F3 — TopologySkew

- **Supernodes**: hash sample (`enhance.supernode.rate`) of the hot band gets
  `minExtra … maxExtraDegreeMul` additional knows edges; targets sampled from
  the hot band, so no dangling FKs and no lookup joins.
- **Hot messages**: posts created by supernodes receive `hotmsg.likes` extra
  likes (likers from the hot band), `hotmsg.replies` reply comments (ids in
  the bucket-254 reserve band, each also getting `hotmsg.likes` likes), and
  `hotmsg.tags` extra tags.
- **Small components**: `smallcomp.count` schema-conformant persons in the
  bucket-255 reserve band grouped into cliques of at most `cliqueMax`.
- **R-MAT subgraph**: Graph500-style power-law subgraph over `rmat.nodes`
  reserve-band persons with `edgesPerNode` edges/node (a/b/c/d quadrants).
- **Fragmentation**: drops a hash-selected `fragment.dropKnowsRatio` of knows
  edges, breaking communities apart.
- **Asymmetry**: appends flipped copies of a hash-selected
  `unidir.flipRatio` of knows edges (`P1 > P2` rows appear).

### F4 — ZipfSkew

Rewrites a low-cardinality attribute (`zipf.attrs`) so its value distribution
follows a Zipf law of exponent `zipf.s`: distinct values are collected on the
driver, a bucket→value table is built from prefix sums of `(rank+1)^-s` and
broadcast-joined back on `hash01(id, attr, salt)`. The value set never grows;
nulls are preserved.

### F5 — TemporalBurst

- **Burst windows**: a `burst.ratio` hash sample of Post/Comment gets its
  `creationDate` re-written into `burst.windowCount` windows of
  `burst.windowMillis` inside the last `burst.recentDays` days.
- **Hot/cold split**: a `hotcold.hotRatio` hash sample is compressed into the
  last 10% of the entity's data span.
- **Supernode like storms**: likes on supernode posts are re-dated into the
  same burst windows (supernode set recomputed with the same deterministic
  formula, independent of whether F3 ran).

Untouched rows keep their exact original dates; `deletionDate` is only pushed
forward (always ≥ creationDate + 1 day for re-dated rows).

### F6 — DirtyData

All selections are hash samples; ids are never modified:

| Parameter | Effect |
|---|---|
| `dirty.nullContentRatio` | Post/Comment content → NULL |
| `dirty.emptyContentRatio` | Post/Comment content → "" |
| `dirty.invalidIpRatio` | Post/Comment locationIP → `999.999.999.999` |
| `dirty.outOfRangeDateRatio` | Post/Comment creationDate → 0 (epoch) |
| `dirty.selfLoopRatio` | knows edges collapse to self-loops (P2 := P1) |
| `dirty.dupEdgeRatio` | knows edges duplicated verbatim |
| `dirty.danglingLikeRatio` | likes re-pointed at never-allocated bucket-253 ids |

## Usage

```bash
spark-submit --class ldbc.snb.datagen.LdbcDatagen \
  ldbc_snb_datagen_2.12_spark3.2-0.6.0-enhance-jar-with-dependencies.jar \
  --scale-factor 1 -f csv -n 8 --output-dir out \
  --enhance-param-file conf/enhance-defaults.ini
```

Enable features by flipping knobs in the ini (or appending
`--enhance-params enhance.enabled=true,...`). Change `enhance.salt` to
produce a different but equally deterministic variant.

## Container

The container image is self-contained (JRE 11, Spark 3.2.1, the datagen jar,
all enhance parameter files and the structural verifier):

```bash
scripts/build-image.sh                                  # build ldbc/datagen-enhance:latest
docker run --rm -v "$PWD/out:/out" ldbc/datagen-enhance \
  --scale-factor 1 -f csv --output-dir /out \
  --enhance-param-file /opt/enhance/conf/enhance-defaults.ini
docker run --rm -v "$PWD/out:/out" --entrypoint python3 \
  ldbc/datagen-enhance /opt/enhance/scripts/verify_enhance.py /out
```

Inside the image: `/opt/datagen/datagen.jar`, `/opt/enhance/{conf,scripts,docs}`.
Resource knobs via env: `MASTER`, `DRIVER_MEMORY`, `DRIVER_CORES`, `SPARK_CONF`.

## Validation

`scripts/verify_enhance.py` runs a structural gate over the combined output
(feature presence, FK integrity outside deliberate dirt, row counts vs a
native reference run):

```bash
python3 scripts/verify_enhance.py <enhanced-out> [native-out]
```

Per-module **exact replay** verifications (xxhash64 + formula re-implementation
in Python, every row compared) were performed at SF1 during development:
F2 id-set bijection; F3 24 checks; F4 24 checks (top share 14.9% vs 15.1%
theoretical); F5 15 checks (4.6M rows row-exact); F6 26 checks; combined
all-modules run 18 checks. See commit history for the verification scripts.

## Known native-data caveats

- Native LDBC rows may legitimately have `deletionDate < creationDate + 1d`
  (short lifespans); F5 never fixes those, it only guarantees the invariant
  for rows it re-dates.
- Native image posts carry empty `content`; F6's empty/null-content dirt is
  therefore counted on top of an existing native baseline.
