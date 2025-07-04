## What's Changed
* chore: bump main branch version to 0.15 by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/5984
* ci: read next release version from toml by default by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/5986
* chore: update rust toolchain by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/5818
* feat: introduce `RegionStatAwareSelector` trait by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/5990
* chore: update nix for new toolchain by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/5991
* feat: uddsketch_merge udaf by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/5992
* fix: check if memtable is empty by stats by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/5989
* chore: make txn_helper pub by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6002
* ci: update dev-builder image version to 2025-04-15-1a517ec8-202504280… by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6003
* fix: prune primary key with multiple columns may use default value as statistics by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/5996
* feat: remove own greatest fn by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/5994
* fix: only consider the datanode that reports the failure by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6004
* chore: only retry when retry-able in flow by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/5987
* fix: sanitize_connection_string by @fengjiachun in https://github.com/GreptimeTeam/greptimedb/pull/6012
* fix: disable recursion limit in prost by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6010
* feat: flush leader region before downgrading by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/5995
* ci: fix the bugs of release-dev-builder-images and add update-dev-builder-image-tag by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6009
* feat: implement batch region opening in metric engine by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6017
* chore: rename parameter from "table" to "flow_name"  by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6020
* fix: always create mito engine by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6018
* feat: enhance maintenance mode API and handling by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6022
* chore: upgrade hydroflow depend by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6011
* ci: nix action update by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6025
* feat: optimize region migration concurrency with fine-grained table lock by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6023
* feat(pipeline): auto transform by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6013
* feat: cast strings to numerics automatically in mysql connections by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6015
* feat(meta): enhance region lease handling with operating status by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6027
* fix: force streaming mode for instant source table by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6031
* refactor: datanode instance builder by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6034
* docs: refine readme by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6038
* chore: add logs dashboard by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6028
* ci: update website greptimedb version when releasing automatically by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6037
* refactor: remove the "mode" configuration item completely by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6042
* chore: rm unnecessary depend for flow by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6047
* feat: bridge bulk insert by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/5927
* fix: do not add projection to cast timestamp in label_values by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6040
* fix: improve region migration error handling and optimize leader downgrade with lease check by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6026
* feat(pipeline): select processor by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6019
* refactor: remove some async in ServerHandlers by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6057
* feat: flow add static user/pwd auth by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6048
* fix: reset tags when creating an empty metric in prom call by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6056
* feat: try cast timestamp types from number string by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6060
* feat: scan with sst minimal sequence by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6051
* fix: ensures logical and physical region have the same timestamp unit by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6041
* feat: update pgwire to 0.29 by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6058
* fix: csv format escaping by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6061
* ci: run only in the `GreptimeTeam/greptimedb` repository by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6064
* chore: support rename syntax in field by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6065
* feat: impl bulk memtable and bridge bulk inserts by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6054
* fix: alter table modify type should also modify default value by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6049
* feat: set read-preference for grpc client by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6069
* feat: mem prof can gen flamegraph directly by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6073
* ci: only trigger downstream when release success by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6074
* feat: add datanode workloads support by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6055
* fix!: disable append mode in trace services table by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6066
* ci: automatically update helm-charts when release by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6071
* chore: mv anyhow depend out of cfg by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6081
* ci: update homebrew greptime version when release by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6082
* chore: more cfg stuff on windows by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6083
* refactor: introduce row group selection by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/6075
* chore: fix clippy error by feature-gating Query import by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6085
* fix: flownode chose fe randomly&not starve lock by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6077
* fix: typos by @omahs in https://github.com/GreptimeTeam/greptimedb/pull/6084
* feat: implement PlainBatch struct by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6079
* feat(meta): add pusher deregister signal to mailbox receiver by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6072
* feat: implement commutativity rule for prom-related plans by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/5875
* chore: bump rskafka version by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6090
* fix: promql regex escape behavior by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6094
* feat(bulk): write to multiple time partitions by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6086
* fix: table metadata collection by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6102
* perf: avoid some atomic operation on array slice by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6101
* refactor: add `SlowQueryRecorder` to record slow query in system table and refactor slow query options by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6008
* ci: update nix build linker by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6103
* chore: modify default `slow_query.threshold` from 5s to 30s by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6107
* feat: don't hide atomic write dir by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6109
* feat: New scanner `SeriesScan` to scan by series for querying metrics by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/5968
* fix: clean files under the atomic write dir on failure by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6112
* fix: append noop entry when auto topic creation is disabled by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6092
* fix: fast path for single region bulk insert by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6104
* fix: update promql-parser for regex anchor fix by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6117
* feat: export to s3 add more options by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6091
* feat: Prometheus remote write with pipeline by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/5981
* fix: flow update use proper update by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6108
* ci: fix release job dependencies by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6125
* chore: remove etcd from acknowledgement as not recommended by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6127
* feat: implement clamp_min and clamp_max by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6116
* feat: introduce index result cache by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/6110
* chore: Add more data format support to the pipeline dryrun api. by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6115
* ci: add pull requests permissions to semantic check job by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6130
* chore: enable github folder typo check and fix typos by @yinheli in https://github.com/GreptimeTeam/greptimedb/pull/6128
* chore: update toolchain to 2025-05-19 by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6124
* feat: accommodate default column name with pre-created table schema by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6126
* chore: reduce unnecessary txns in alter operations by @fengjiachun in https://github.com/GreptimeTeam/greptimedb/pull/6133
* fix: update dev-build image tag by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6136
* feat: update dashboard to v0.9.1 by @ZonaHex in https://github.com/GreptimeTeam/greptimedb/pull/6132
* feat: support altering multiple logical table in one remote write request by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6137
* chore: update flush failure metric name and update grafana dashboard by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6138
* feat: update dashboard to v0.9.2 by @ZonaHex in https://github.com/GreptimeTeam/greptimedb/pull/6140
* chore: remove stale wal config entries by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6134
* docs: change docker run mount directory by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6142
* fix: flaky prom gateway test by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6146
* fix: region worker stall metrics by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6149
* ci: add issues write permission by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6145
* feat(flow): support prom ql(in tql) in flow by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6063
* fix(flow): flow task run interval by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6100
* fix: ident value in set search_path by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6153
* fix: require input ordering in series divide plan by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6148
* chore: add the missing `v` prefix for `NEXT_RELEASE_VERSION` variable by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6160
* ci: turn off fail fast strategy by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6157
* feat!: revise compaction picker by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6121
* fix: invalid table flow mapping cache by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6135
* perf: optimize bulk encode decode by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6161
* chore: metasrv starting not blocking by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6158
* fix: bulk insert case sensitive by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6165
* fix: set column index can't work in physical table by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6179
* fix: add simple test for rds kv backend by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6167
* refactor: replace FlightMessage with arrow `RecordBatch` and `Schema` by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6175
* chore: change info to debug for scanning physical table by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6180
* fix(promql): handle field column projection with correct qualifier by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6183
* fix: alter table update table column default by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6155
* feat: add CLI tool to export metadata by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6150
* docs: nit from github -> GitHub by @yihong0618 in https://github.com/GreptimeTeam/greptimedb/pull/6199
* chore: correct some CAS ordering args by @fengjiachun in https://github.com/GreptimeTeam/greptimedb/pull/6200
* chore: add metrics for rds kv backend by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6201
* chore: switch nix index to 25.05 release by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6181
* fix: remove poison key before retrying procedure on retryable errors by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6189
* chore: fix feature gates for pg and mysql kvbackend by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6211
* feat: support parsing trigger create sql by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6197
* feat: supports @@session.time_zone for mysql by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6210
* feat(wal): support bulk wal entries by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6178
* chore: fix rds kv backend test by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6214
* feat(http): lossy string validation in prom remote write by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6213
* feat: update pgwire to 0.30 by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6209
* feat: support SQL parsing for trigger show by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6217
* chore: example of http config in metasrv by @fengjiachun in https://github.com/GreptimeTeam/greptimedb/pull/6218
* chore: clear metadata filed after updating metadata by @fengjiachun in https://github.com/GreptimeTeam/greptimedb/pull/6215
* chore: shared pipeline under same catalog with compatibility by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6143
* fix: remove stale region failover detectors by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6221
* feat: supports select @@session.time_zone by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6212
* docs: fix bad link by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6222
* chore: add some metrics to grafana dashboard by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6169
* chore: add pg mysql be default feature in cli by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6230
* feat: bloom filter index applier support or eq chain by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/6227
* fix(mito): revert initial builder capacity for TimeSeriesMemtable by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6231
* refactor: extract some common functions and structs in election module by @CookiePieWw in https://github.com/GreptimeTeam/greptimedb/pull/6172
* refactor(flow): limit the size of query by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6216
* feat: pipeline with insert options by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6192
* fix: do not accommodate fields for multi-value protocol by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6237
* ci: add option to choose whether upload artifacts to S3 in the development build by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6232
* feat: add trigger ddl manager by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6228
* fix: add missing features by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6245
* feat(flow): flow streaming mode in list expr support by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6229
* chore: test sleep longer by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6247
* fix: add "query" options to standalone by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6248
* fix: ignore incomplete WAL entries during read by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6251
* chore: allow numberic values in alter statements by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6252
* chore: pub flow info by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6253
* feat: don't allow creating logical table with partitions by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6249
* fix: convert JSON type to JSON string in COPY TABLE TO statment by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6255
* fix: skip wal replay when opening follower regions by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6234
* ci: increase upload s3 retry times by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6263
* feat(pipeline): vrl processor by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6205
* chore: support table suffix in hint by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6223
* feat: add the gauge to indicate the CPU and Memory limit in the cgroups envrionment by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6238
* feat: Support export cli export to OSS by @zqr10159 in https://github.com/GreptimeTeam/greptimedb/pull/6225
* fix(mito): use 1day as default time partition duration by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6202
* refactor: respect `data_home` as root data home directory by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6050
* feat: add some metasrv metrics to grafana dashboard by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6264
* feat: disable compression for `do_get` API by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6254
* ci: refactor bump downstream versions worflow and adds demo-scene by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6171
* refactor: support to get trace id with time range by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6236
* fix: export metrics settings in sample config by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6170
* chore: improve CI debugging and resource configuration by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6274
* fix(meta): enhance postgres election client with timeouts and reconnection by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6276
* feat: refactor grpc options of metasrv by @fengjiachun in https://github.com/GreptimeTeam/greptimedb/pull/6275
* refactor: unify function registry (Part 1) by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/6262
* fix: null value handling on PromQL's join by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6289
* ci: add signature information when updating downstream repository by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6282
* refactor(cli)!: reorganize cli subcommands by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6265
* chore: add option for arrow flight compression mode by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6283
* fix: config docs by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6294
* chore: silence clippy by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6298
* feat(cli): add metadata get commands by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6299
* refactor: remove `PipelineMap` and use `Value` instead by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6278
* chore: add failover cache for pipeline table by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6284
* ci: use the new meta backendStorage etcd structure by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6303
* fix: doc links by @nicecui in https://github.com/GreptimeTeam/greptimedb/pull/6304
* feat: implement process manager and  information_schema.process_list  by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/5865
* feat: support using expressions as literal in PromQL by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6297
* feat: introduce file group in compaction by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6261
* feat: organize EXPLAIN ANALYZE VERBOSE's output in JSON format by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6308
* feat: parallelism hint in grpc by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6306
* fix: check for zero parallelism by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6310
* feat: process id for session, query context and postgres by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6301
* feat: update dashboard to v0.9.3 by @ZonaHex in https://github.com/GreptimeTeam/greptimedb/pull/6311
* refactor: Extract mito codec part into a new crate by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6307
* fix: always use linux path style in windows platform unit tests by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6314
* feat: support killing process by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6309
* chore: add connection info to `QueryContext` by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6319
* fix: event api content type only check type and subtype by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6317
* fix: handle corner case in catchup where compacted entry id exceeds region last entry id by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6312
* feat: bulk support flow batch by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6291
* fix: ignore missing columns and tables in PromQL by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6285
* feat: support arbitrary constant expression in PromQL function by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6315
* ci: add pr label workflow by @daviderli614 in https://github.com/GreptimeTeam/greptimedb/pull/6316
* feat: support special labels parsing in prom remote write by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6302
* feat: handle `Ctrl-C` command in MySQL client by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6320
* refactor: make flownode gRPC services able to be added dynamically by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6323
* chore: add skip error for pipeline skip error log by @paomian in https://github.com/GreptimeTeam/greptimedb/pull/6318
* fix: override logical table's partition column with physical table's by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6326
* chore: clean up unused impl &standalone use mark dirty by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6331
* feat: Add `DROP DEFAULT` by @linyihai in https://github.com/GreptimeTeam/greptimedb/pull/6290
* fix: carry process id in query ctx by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6335
* fix: revert string builder initial capacity in `TimeSeriesMemtable` by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6330
* feat: introduce CLI tool for repairing logical table metadata by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6322
* feat: support setting FORMAT in TQL ANALYZE/VERBOSE by @waynexia in https://github.com/GreptimeTeam/greptimedb/pull/6327
* fix(metric): prevent setting memtable type for metadata region by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6340
* fix(jaeger-api): incorrect `find_traces()` logic and multiple api compatible issues by @zyy17 in https://github.com/GreptimeTeam/greptimedb/pull/6293
* chore: add metrics for active series and field builders by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6332
* fix: add path exist check in copy_table_from (#6182) by @Arshdeep54 in https://github.com/GreptimeTeam/greptimedb/pull/6300
* refactor: make finding leader in metasrv client dynamic by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6343
* chore: print series count after wal replay by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6344
* chore(deps): switch greptime-proto to official repository by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6347
* fix: reordered write cause incorrect kv by @v0y4g3r in https://github.com/GreptimeTeam/greptimedb/pull/6345
* fix(metric-engine): properly propagate errors during batch open operation by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6325
* chore: security updates by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6351
* feat(cli): add metadata del commands by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6339
* feat: support execute sql in frontend_client by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6355
* fix(meta): enhance mysql election client with timeouts and reconnection by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6341
* refactor: make metadata region option opt-in by @sunng87 in https://github.com/GreptimeTeam/greptimedb/pull/6350
* refactor: make scanner creation async by @MichaelScofield in https://github.com/GreptimeTeam/greptimedb/pull/6349
* feat: dist auto step aggr pushdown by @discord9 in https://github.com/GreptimeTeam/greptimedb/pull/6268
* feat(storage): Add skip_ssl_validation option for object storage HTTP client by @rgidda in https://github.com/GreptimeTeam/greptimedb/pull/6358
* docs: added YouTube link to documentation by @Olexandr88 in https://github.com/GreptimeTeam/greptimedb/pull/6362
* refactor(cli): simplify metadata command parameters by @WenyXu in https://github.com/GreptimeTeam/greptimedb/pull/6364
* chore: prints a warning when skip_ssl_validation is true by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6367
* fix: test test_tls_file_change_watch by @killme2008 in https://github.com/GreptimeTeam/greptimedb/pull/6366
* feat: update dashboard to v0.10.0 by @ZonaHex in https://github.com/GreptimeTeam/greptimedb/pull/6368
* feat(pipeline): introduce pipeline doc version 2 for combine-transform by @shuiyisong in https://github.com/GreptimeTeam/greptimedb/pull/6360
* feat: cherry-pick #6384 #6388 #6396 #6403 #6412 #6405 to 0.15 branch by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6414
* feat: pick #6416 to release/0.15 by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/6445
* chore: cherry pick #6385, #6390, #6432, #6446, #6444 to 0.15 branch by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6447
* chore: cherry pick pr 6391 to release/v0.15 by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6449
* chore: pick #6399, #6443, #6431, #6453, #6454 to v0.15 by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6455

## New Contributors
* @omahs made their first contribution in https://github.com/GreptimeTeam/greptimedb/pull/6084
* @yinheli made their first contribution in https://github.com/GreptimeTeam/greptimedb/pull/6128
* @zqr10159 made their first contribution in https://github.com/GreptimeTeam/greptimedb/pull/6225
* @Arshdeep54 made their first contribution in https://github.com/GreptimeTeam/greptimedb/pull/6300
* @rgidda made their first contribution in https://github.com/GreptimeTeam/greptimedb/pull/6358
* @Olexandr88 made their first contribution in https://github.com/GreptimeTeam/greptimedb/pull/6362

**Full Changelog**: https://github.com/GreptimeTeam/greptimedb/compare/v0.14.4...v0.15.0
