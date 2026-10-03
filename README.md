# 本地容器制品仓库（Local OCI Registry）

基于 **Python + FastAPI + SQLite + 本地文件存储** 的轻量 OCI 风格制品仓库。
支持分块/断点续传 blob、按摘要全局去重、重启后恢复上传、不可变 manifest
发布校验、标签 CAS 移动，以及带 dry-run 复核的可达性垃圾回收。

## 功能特性

| 能力 | 说明 |
| --- | --- |
| OCI 风格 API | `/v2/<repo>/blobs/uploads`、`/v2/<repo>/manifests/<ref>`、`/v2/<repo>/blobs/<digest>`、`/v2/<repo>/tags/list` |
| 分块上传 / 续传 | `POST → PATCH* → PUT?digest=`；`GET` 会话返回 `Range` 进度，可断点续传 |
| 摘要校验 | 上传完成时按内容计算 sha256，与客户端摘要不一致即拒绝并取消会话 |
| 按摘要去重 | blob 全局内容寻址存储，跨仓库/重复推送只占一份磁盘；支持 `?mount=&from=` 跨仓库挂载 |
| 重启恢复 | 上传会话与进度持久化在 SQLite，重启后自动对齐磁盘实际进度，可继续续传 |
| 不可变 manifest | digest 寻址的 manifest 内容与摘要不符即拒绝；发布时校验 config/layers/subject/子 manifest 引用必须已存在，杜绝悬空引用 |
| 标签 CAS | 移动已存在的标签必须携带期望摘要（`If-Match` 头或 `?expected=`），不匹配返回 `409 TAG_CONFLICT` |
| 安全 GC | 以「标签 + 未过期上传租约」为根建立可达图；**先 dry-run 生成持久化复核计划（`plan_id`，1 小时有效、重启有效），再凭 `plan_id` 执行**；执行前在 SQLite 中原子认领计划（`pending → executing`），多个进程共享同一数据目录并发执行同一计划时只有一个进程认领成功并删除，竞争方返回 `409 GC_PLAN_IN_PROGRESS` 且不碰候选；认领后进程崩溃则计划永久停留在 `executing`（不可重放），全部删除完成才标记 `executed`；执行时重算可达性/租约，只删计划内仍符合条件的对象，复核窗口内新产生的候选不会被误删；**跨进程读写锁（`fcntl.flock`）把发布的「引用校验→标签提交」与 GC 的「可达性复核→元数据删除→文件删除」串行化：发布先完成则 GC 跳过已可达候选，GC 先删除则 PUT 返回 `400 MANIFEST_BLOB_UNKNOWN` 且标签不变，不存在「标签可读而 blob 404」；与并发拉取/发布互斥** |

## 快速开始（Docker Compose 一键启动）

```bash
docker compose up --build -d
```

服务监听在 **http://localhost:5000**（容器内 8000 端口），数据持久化在
`registry-data` 卷中。验证：

```bash
curl http://localhost:5000/v2/        # -> {} ，响应头含 Docker-Distribution-API-Version: registry/2.0
curl http://localhost:5000/healthz    # -> {"status":"ok"}
```

也可以本地直接运行（Python 3.11+）：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 5000
```

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REGISTRY_DATA_DIR` | `./data`（镜像内 `/data`） | 数据目录（SQLite + blob 文件 + 上传临时文件） |
| `REGISTRY_UPLOAD_LEASE_SECONDS` | `3600` | 上传会话租约；超时无读写即被 GC 回收 |
| `REGISTRY_GC_GRACE_SECONDS` | `3600` | GC 宽限期；创建不足该时长的未引用 blob 不回收，保护「已推送、待发布」窗口 |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v2/` | API 版本探测 |
| POST | `/v2/{repo}/blobs/uploads` | 创建上传会话（202）；`?digest=` 整体上传（201）；`?mount=&from=` 跨仓库挂载 |
| GET | `/v2/{repo}/blobs/uploads/{id}` | 查询上传进度（204 + `Range`），用于续传 |
| PATCH | `/v2/{repo}/blobs/uploads/{id}` | 追加分块，可选 `Content-Range` 校验偏移 |
| PUT | `/v2/{repo}/blobs/uploads/{id}?digest=` | 完成上传（可携带最后一块），校验摘要 |
| DELETE | `/v2/{repo}/blobs/uploads/{id}` | 取消上传 |
| GET/HEAD | `/v2/{repo}/blobs/{digest}` | 拉取 blob |
| PUT | `/v2/{repo}/manifests/{ref}` | 发布 manifest；`ref` 为标签时需 CAS（`If-Match` 或 `?expected=`） |
| GET/HEAD | `/v2/{repo}/manifests/{ref}` | 拉取 manifest（`ref` 可为标签或 digest） |
| GET | `/v2/{repo}/tags/list` | 列出标签 |
| POST | `/gc` | 垃圾回收。`{"dry_run": true, "grace_seconds": 3600}` 生成持久化复核计划并返回 `plan_id`；`{"dry_run": false, "plan_id": "..."}` 先原子认领计划（跨进程互斥），成功才执行；竞争返回 `409 GC_PLAN_IN_PROGRESS` |

错误统一为 OCI 格式：`{"errors":[{"code","message","detail"}]}`。

## curl 示例

以下假设服务在 `localhost:5000`，仓库为 `demo/app`。

### 1. 分块上传 blob（断点续传）

```bash
# 准备数据并计算摘要
printf 'hello-layer-content' > layer.bin
DIGEST="sha256:$(sha256sum layer.bin | cut -d' ' -f1)"

# 创建上传会话，记录 Location
LOC=$(curl -s -D - -o /dev/null -X POST \
  http://localhost:5000/v2/demo/app/blobs/uploads | awk '/^[Ll]ocation:/{print $2}' | tr -d '\r')
echo "$LOC"   # /v2/demo/app/blobs/uploads/<uuid>

# 上传前 5 字节（模拟中断）
curl -s -X PATCH -H 'Content-Type: application/octet-stream' \
  --data-binary @- "http://localhost:5000$LOC" <<< '' >/dev/null  # 仅占位
head -c 5 layer.bin | curl -s -D - -o /dev/null -X PATCH \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @- "http://localhost:5000$LOC" | grep -i range
# Range: 0-4

# 断点续传：先查询进度，再带 Content-Range 续传剩余部分
curl -s -D - -o /dev/null "http://localhost:5000$LOC" | grep -i range
tail -c +6 layer.bin | curl -s -X PATCH \
  -H 'Content-Type: application/octet-stream' -H 'Content-Range: 5-18' \
  --data-binary @- "http://localhost:5000$LOC" -o /dev/null

# 完成上传并校验摘要（服务按内容重算 sha256，不符则 400 并取消会话）
curl -s -D - -o /dev/null -X PUT "http://localhost:5000$LOC?digest=$DIGEST"
# 201 Created, Docker-Content-Digest: sha256:...
```

小文件也可整体上传：

```bash
curl -s -X POST --data-binary @layer.bin \
  "http://localhost:5000/v2/demo/app/blobs/uploads?digest=$DIGEST"   # 201，重复推送会去重秒回
```

### 2. 发布 manifest（标签 CAS）

```bash
# 先推 config 与 layer 两个 blob（略，同上），然后组装 manifest
cat > manifest.json <<EOF
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config":   {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "$CONFIG_DIGEST", "size": $CONFIG_SIZE},
  "layers":   [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "$DIGEST", "size": 19}]
}
EOF

# 首次打 tag：无需期望摘要
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 201

# 移动 tag 必须携带期望摘要（CAS），否则 409 TAG_CONFLICT
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest-v2.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 409

# 正确姿势：If-Match 携带 tag 当前指向的 digest
CUR=$(curl -sI http://localhost:5000/v2/demo/app/manifests/v1 | awk '/^[Dd]ocker-[Cc]ontent-[Dd]igest:/{print $2}' | tr -d '\r')
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  -H "If-Match: $CUR" \
  --data-binary @manifest-v2.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 201，tag 完成移动
```

发布时服务端会校验 manifest 引用的 config/layers（以及 index 的子
manifest、referrers 的 subject）必须已存在于本仓库，否则返回
`400 MANIFEST_BLOB_UNKNOWN`，从源头杜绝悬空引用。

### 3. 拉取

```bash
curl -s http://localhost:5000/v2/demo/app/manifests/v1            # 按 tag 拉 manifest
curl -s http://localhost:5000/v2/demo/app/manifests/$MANIFEST_DIGEST
curl -s http://localhost:5000/v2/demo/app/blobs/$DIGEST -o layer.bin
curl -s http://localhost:5000/v2/demo/app/tags/list
```

### 4. 垃圾回收（dry-run 复核计划 → 原子认领 → 凭 plan_id 执行）

GC 采用**两阶段复核**，修复了「dry-run 后标签移动产生新垃圾、执行时被一并
删除」以及「两个进程同时执行同一计划、各自读取 pending 并重复删除」两类
竞态：

1. dry-run 把候选清单**快照持久化**成计划（`plan_id`，状态 `pending`），
   执行时只处理计划内、且此刻仍符合条件的对象；复核窗口内新出现的候选一律
   留给下次审阅。
2. 执行（`dry_run:false`）**先在 SQLite 中原子认领**：单条写事务把计划
   从 `pending` 更新为 `executing` 并立即提交（`UPDATE ... WHERE
   state='pending' AND expires_at>now`）。多个进程挂载同一数据目录并发请求
   同一 `plan_id` 时，只有一个进程认领成功；其余进程收到
   `409 GC_PLAN_IN_PROGRESS`，**不复核、不删除任何候选**。
3. 认领成功的进程再重算可达性与租约、只删计划内仍符合条件的对象，
   **全部删除完成后**才把计划标记为 `executed`。

> 计划保存在 SQLite 中，**进程重启后依然有效**；生成后 **1 小时**内可执行；
> 一份计划**只能执行一次**。认领成功后若执行进程异常或崩溃，计划停留在
> **`executing`（不可重放的执行中状态）**——已删除的部分不会重复执行，
> 对同一 `plan_id` 的重试同样返回 `409 GC_PLAN_IN_PROGRESS`（detail 中
> `stale:true` 表示认领方疑似已崩溃），剩余候选请重新 dry-run 审阅后用新
> 计划回收。

#### 第一步：dry-run 生成计划（不删任何数据）

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": true, "grace_seconds": 0}' \
  http://localhost:5000/gc
```

实际响应（`grace_seconds: 0` 仅为演示立刻可回收；生产可用默认 3600）：

```json
{
  "dry_run": true,
  "executed": false,
  "plan_id": "7d4ff3878fc848959fef26bdf39e3cce",
  "grace_seconds": 0,
  "generated_at": "2026-10-03T02:24:31.973878+00:00",
  "expires_at": "2026-10-03T03:24:31.973878+00:00",
  "summary": {
    "blobs_to_delete": 2,
    "manifests_to_delete": 1,
    "uploads_to_delete": 1,
    "bytes_to_reclaim": 30,
    "manifests_reachable": 2,
    "blobs_reachable": 3,
    "blobs_kept_by_grace": 0,
    "uploads_kept_by_lease": 1
  },
  "delete": {
    "blobs": [
      {"digest": "sha256:492fa4fc2ff764ecd108c3c4fbabd4ff6561a17d8feacfcf100b9f3da05a349c",
       "size": 17, "repos": ["demo/app"],
       "reason": "unreferenced and older than grace period"},
      {"digest": "sha256:9e41582b7b5eced4b02b2f9112ce85ceef30b92cf2b3fb4c92f10061b3400c0b",
       "size": 13, "repos": ["demo/app"],
       "reason": "unreferenced and older than grace period"}
    ],
    "manifests": [
      {"repo": "demo/app",
       "digest": "sha256:28931bb7077f5cb2d2083be81dcb42725febe13ff28a40ae7bc7c461056900e6",
       "media_type": "application/vnd.oci.image.manifest.v1+json",
       "reason": "untagged"}
    ],
    "uploads": [
      {"id": "3601015371a64bc7a702fab2d965a8f1", "repo": "demo/app", "size": 0,
       "expired_at": "2026-10-03T02:23:37.960170+00:00", "reason": "lease expired"}
    ]
  },
  "keep": {
    "blobs_grace_protected": [],
    "uploads_leased": [
      {"id": "95473834eb4d4fe5ad4a28c3cf79ac2e", "repo": "demo/app", "size": 0,
       "expires_at": "2026-10-03T03:24:31.957696+00:00"}
    ]
  }
}
```

复核要点：`delete.blobs / delete.manifests / delete.uploads` 即本次计划的候选；
`keep` 是宽限期/租约保护下保留的对象。记下 `plan_id`。

#### 第二步：凭 plan_id 执行

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": false, "plan_id": "7d4ff3878fc848959fef26bdf39e3cce"}' \
  http://localhost:5000/gc
```

服务端先原子认领该计划（认领成功后再复核、删除）。实际响应——`deleted` 是
真正删除的项，`skipped` 是计划内、但执行时已重新可达/续租/消失而跳过的项：

```json
{
  "dry_run": false,
  "executed": true,
  "plan_id": "7d4ff3878fc848959fef26bdf39e3cce",
  "grace_seconds": 0,
  "generated_at": "2026-10-03T02:24:31.973878+00:00",
  "executed_at": "2026-10-03T02:25:01.028073+00:00",
  "summary": {
    "blobs_deleted": 2,
    "manifests_deleted": 1,
    "uploads_deleted": 1,
    "blobs_skipped": 0,
    "manifests_skipped": 0,
    "uploads_skipped": 0,
    "bytes_reclaimed": 30
  },
  "deleted": {
    "blobs": [
      {"digest": "sha256:492fa4fc2ff764ecd108c3c4fbabd4ff6561a17d8feacfcf100b9f3da05a349c",
       "size": 17, "repos": ["demo/app"], "reason": "still unreferenced at execution"},
      {"digest": "sha256:9e41582b7b5eced4b02b2f9112ce85ceef30b92cf2b3fb4c92f10061b3400c0b",
       "size": 13, "repos": ["demo/app"], "reason": "still unreferenced at execution"}
    ],
    "manifests": [
      {"repo": "demo/app",
       "digest": "sha256:28931bb7077f5cb2d2083be81dcb42725febe13ff28a40ae7bc7c461056900e6",
       "media_type": "application/vnd.oci.image.manifest.v1+json",
       "reason": "still untagged at execution"}
    ],
    "uploads": [
      {"id": "3601015371a64bc7a702fab2d965a8f1", "repo": "demo/app", "size": 0,
       "reason": "lease still expired at execution"}
    ]
  },
  "skipped": {"blobs": [], "manifests": [], "uploads": []}
}
```

复核窗口内若把一个候选重新打上标签，执行时它会进入 `skipped` 而不是被删，例如：

```json
"skipped": {
  "blobs": [
    {"digest": "sha256:....", "reason": "reachable again: referenced by a tagged manifest"}
  ],
  "manifests": [
    {"repo": "demo/app", "digest": "sha256:....",
     "reason": "reachable again: a tag (or a tagged parent) points to it"}
  ],
  "uploads": [
    {"id": "....", "repo": "demo/app", "reason": "lease renewed or still active"}
  ]
}
```

#### 拒绝执行的情况（均不删数据）

| 情况 | 状态码 | `errors[0].code` |
| --- | --- | --- |
| 未带 `plan_id` / 空白 | 400 | `GC_PLAN_REQUIRED` |
| `plan_id` 未知 | 404 | `GC_PLAN_UNKNOWN` |
| 计划生成已超过 1 小时 | 410 | `GC_PLAN_EXPIRED` |
| 计划已经执行过（旧计划重试） | 409 | `GC_PLAN_ALREADY_EXECUTED` |
| 计划已被另一进程认领、正在执行；或认领后执行方崩溃留下不可重放的 `executing` 计划 | 409 | `GC_PLAN_IN_PROGRESS` |

实测请求与响应：

```bash
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false}' http://localhost:5000/gc
# HTTP 400
{"errors":[{"code":"GC_PLAN_REQUIRED",
  "message":"execution (dry_run=false) requires the plan_id of a prior dry-run report; run a dry-run first and re-submit the returned plan_id",
  "detail":{"hint":"POST /gc with {\"dry_run\": true} to obtain a plan_id"}}]}

$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "does-not-exist"}' http://localhost:5000/gc
# HTTP 404
{"errors":[{"code":"GC_PLAN_UNKNOWN",
  "message":"unknown GC plan_id: 'does-not-exist'; run a new dry-run",
  "detail":{"plan_id":"does-not-exist"}}]}

$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "32507fa25c9747df9bb8b53c7b6693b6"}' http://localhost:5000/gc
# HTTP 410（计划超过生成后 1 小时）
{"errors":[{"code":"GC_PLAN_EXPIRED",
  "message":"GC plan '32507fa25c9747df9bb8b53c7b6693b6' expired at 2026-10-03T02:25:42.964437+00:00 (plans are valid for one hour after generation); run a new dry-run",
  "detail":{"plan_id":"32507fa25c9747df9bb8b53c7b6693b6",
            "expires_at":"2026-10-03T02:25:42.964437+00:00"}}]}

$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "7d4ff3878fc848959fef26bdf39e3cce"}' http://localhost:5000/gc
# HTTP 409（旧计划重试：计划已经执行过）
{"errors":[{"code":"GC_PLAN_ALREADY_EXECUTED",
  "message":"GC plan '7d4ff3878fc848959fef26bdf39e3cce' was already executed at 2026-10-03T02:25:01.028073+00:00; repeated execution deletes nothing, run a new dry-run for a new plan",
  "detail":{"plan_id":"7d4ff3878fc848959fef26bdf39e3cce",
            "executed_at":"2026-10-03T02:25:01.028073+00:00"}}]}
```

并发竞争失败（另一进程认领成功、正在执行）时的实际 409 响应——本请求没有
复核也没有触碰任何候选：

```bash
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "071462689fce42529d3b4464801ad1a5"}' \
    http://localhost:5000/gc
# HTTP 409
{"errors":[{"code":"GC_PLAN_IN_PROGRESS",
  "message":"GC plan '071462689fce42529d3b4464801ad1a5' is already being executed by another process (claimed at 2026-10-03T02:26:05.298377+00:00 by registry-2:1042:9f31a2bc); concurrent execution of the same plan is rejected and no candidates were touched by this request, retry later or run a new dry-run",
  "detail":{"plan_id":"071462689fce42529d3b4464801ad1a5",
            "claimed_at":"2026-10-03T02:26:05.298377+00:00",
            "claimer":"registry-2:1042:9f31a2bc",
            "stale":false}}]}
```

若认领成功的进程随后崩溃、计划长时间停留在 `executing`（超过 5 分钟），重试
得到同样的 409，但 `detail.stale` 为 `true`、消息提示认领方已崩溃：

```json
{"errors":[{"code":"GC_PLAN_IN_PROGRESS",
  "message":"GC plan '...' was claimed at ... by dead-host:1:deadbeef but never finished (the executing process crashed or was killed); it cannot be replayed and no candidates were touched by this request, run a new dry-run for a new plan",
  "detail":{"plan_id":"...", "claimed_at":"...", "claimer":"dead-host:1:deadbeef",
            "stale":true}}]}
```

#### 跨进程并发实测：同一 plan_id 只有一个进程删除

启动两个独立进程、挂载**同一个数据目录**（相当于两个容器共用同一卷），对
同一个 `plan_id` 同时发起 20 个执行请求（每个进程 10 个）：

```bash
REGISTRY_DATA_DIR=/data uvicorn app.main:app --port 8000 &   # 进程 A
REGISTRY_DATA_DIR=/data uvicorn app.main:app --port 8001 &   # 进程 B
for i in $(seq 1 10); do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "ea2789edc5934436b3fb00e792d443e7"}' :8000/gc &
  curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": false, "plan_id": "ea2789edc5934436b3fb00e792d443e7"}' :8001/gc &
done; wait
```

实际结果：**恰好 1 个 `200`，其余 19 个全部 `409`**——执行窗口内竞争失败的
是 `GC_PLAN_IN_PROGRESS`，赢家完成后到达的是 `GC_PLAN_ALREADY_EXECUTED`；
计划内的候选只被删除一次：

```text
      1 200
     19 409

1 200-ok
9 GC_PLAN_ALREADY_EXECUTED
10 GC_PLAN_IN_PROGRESS
```

认领后执行进程崩溃的语义同理：计划保持 `executing`，不会有第二个进程重放它；
尚未删除的候选依然完好，重新 dry-run 后可以用**新计划**审阅、回收。

#### 跨进程协调：GC 与并发发布（PUT manifest）不再产生悬空标签

仅靠 SQLite 原子认领只能防住「同一计划被执行两次」，防不住下面这类**不同操作**
之间的跨进程交错（进程内 `RLock` 在两个进程间互不可见）：

```text
进程 A：POST /gc (dry_run:false, plan_id)
        ├─ 认领计划（pending→executing）
        ├─ 复核可达性：候选 blob 此刻无标签引用 → 判定不可达
        │                                            进程 B：PUT /v2/demo/app/manifests/release
        │                                            ├─ 引用校验：blob 还在 ✓
        │                                            ├─ 写 manifest
        ├─ 删除 blob 元数据 + 文件                    └─ 提交标签 release（201）
        └─ 结果：GET manifests/release 可读，GET blobs/<digest> 却 404（悬空引用）
```

修复方式是在数据目录中增加一把**跨进程读写锁**（`fcntl.flock` 锁文件
`<data>/.registry.lock`，见 `app/lock.py`），把两个临界区在**所有进程间**串行化：

| 临界区 | 锁 | 覆盖范围 |
| --- | --- | --- |
| 发布 manifest | 独占（EX） | Registry 引用校验（config/layers/子 manifest/subject 必须存在）→ 写 manifest → 标签 CAS 提交 |
| GC dry-run | 独占（EX） | 可达图快照与候选清单持久化，保证快照一致 |
| GC 执行（认领之后） | 独占（EX） | 可达性/租约复核 → 删元数据 → 删文件 → 计划标记 executed |
| 拉取 blob | 共享（SH） | 命名空间引用检查 → 打开文件句柄；句柄打开后即释放，锁外流式发送 |
| blob 提交/挂载、上传写、启动恢复 | 独占（EX） | 落库与文件落盘、孤儿清扫 |

加锁顺序在所有路径上统一为「**先跨进程锁、再进程内 RLock**」，不会出现环形
等待；同线程可同模式重入。拉取用共享锁因此 GC 删除（EX）必须等进行中的拉取
打开句柄后才能开始，而 POSIX 下已打开的 fd 不受随后 `unlink` 影响——流式下载
不会被 GC 打断。

于是只有两种线性顺序，二者都不产生悬空引用：

- **发布先完成**：标签已提交，GC 复核重算可达图时看到新标签 → 候选重新可达，
  进入 `skipped.blobs`（`reachable again: referenced by a tagged manifest`），
  不删除；标签与引用此后都可拉取。
- **GC 先删除**：发布的引用校验在 EX 锁内看到 blob/manifest 已不存在 →
  返回 `400 MANIFEST_BLOB_UNKNOWN`，标签**未创建/未移动**（CAS 与校验在同一
  临界区内）；已有标签保持原值，其引用仍可拉取。

> 计划认领（`pending→executing`）与跨进程读写锁是两层独立机制：认领负责
> 「同一计划只能被一个进程执行一次、崩溃不可重放」；读写锁负责「认领之后的
> 复核-删除窗口与并发发布/拉取互斥」。

##### 实测一：GC 先删除 → PUT 收到 400，标签不变、旧引用可拉取

两个独立 uvicorn 进程挂载**同一数据目录**（`/tmp/livedata`，分别监听 8010 与
8011）。标签 `release` 指向一个旧镜像；另有一个无引用、已老过宽限期的候选
blob。进程 A 执行 GC 并在删除候选 blob 后仍持独占锁，进程 B 同时发起
PUT 试图把 `release` 移动到引用候选 blob 的新 manifest：

```bash
$ curl -s -D - -o /tmp/put.body -X PUT \
    -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
    -H "If-Match: sha256:a9c3b4a1dfeb9e52ae4c9991997478c8a6d00843a84d268b2ee6a74849c1cdd5" \
    --data-binary @newm.json \
    http://127.0.0.1:8011/v2/demo/app/manifests/release
# HTTP/1.1 400 Bad Request
{"errors":[{"code":"MANIFEST_BLOB_UNKNOWN",
  "message":"manifest references unknown content in repo 'demo/app': sha256:d808d30f0fd6d4a357a5f4f67ddb3d2373d23e790fd7c7ce48c122b1f1994f34, sha256:13f1d3bb4ac6eb0b9b9c24e3a08be635a78bc32ef5c0525178af27542a64f41e",
  "detail":{"repo":"demo/app",
            "missing":["sha256:d808d30f0fd6d4a357a5f4f67ddb3d2373d23e790fd7c7ce48c122b1f1994f34",
                       "sha256:13f1d3bb4ac6eb0b9b9c24e3a08be635a78bc32ef5c0525178af27542a64f41e"]}}]}
```

PUT 在锁上排队，GC 释放独占锁后引用校验发现候选 blob 已删，返回 400。校验后
的状态（任一进程查询都一致）：

```bash
$ curl -s http://127.0.0.1:8010/v2/demo/app/tags/list
{"name":"demo/app","tags":["release"]}                    # 没有产生新标签

$ curl -sI http://127.0.0.1:8010/v2/demo/app/manifests/release | grep -i docker-content-digest
docker-content-digest: sha256:a9c3b4a1dfeb9e52ae4c9991997478c8a6d00843a84d268b2ee6a74849c1cdd5
                                                          # 标签仍指向旧 manifest（未移动）

$ curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8011/v2/demo/app/manifests/release
200                                                       # 旧 manifest 两个进程都可读
$ curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8010/v2/demo/app/blobs/sha256:5734bc39...22bc40
200                                                       # 标签引用的旧 layer 仍可拉取
$ curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8010/v2/demo/app/blobs/sha256:13f1d3bb...64f41e
404                                                       # 候选 blob 已被 GC 回收
```

进程 A 的 GC 报告（候选确认删除）：

```text
PLAN=52a28acc14ea4ed3a31eb63e67016039
DELETED=sha256:13f1d3bb4ac6eb0b9b9c24e3a08be635a78bc32ef5c0525178af27542a64f41e,
        sha256:d808d30f0fd6d4a357a5f4f67ddb3d2373d23e790fd7c7ce48c122b1f1994f34
SKIPPED=
```

##### 实测二：发布先完成 → GC 复核跳过已可达候选

同样两个进程。GC 已原子认领计划（`pending→executing` 已提交），但在进入复核
临界区之前有一个调度窗口；进程 B 在该窗口内完整完成发布（`201`），随后 GC
才复核与删除：

```bash
$ curl -s -D - -o /dev/null -X PUT \
    -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
    --data-binary @rescue.json \
    http://127.0.0.1:8031/v2/demo/app/manifests/v1
HTTP/1.1 201 Created
Docker-Content-Digest: sha256:e19b24f7a605fd97604ce858ca1f5817bc6c6ccaba079f56df5a7513aab79945
```

GC 重算可达图后两个计划内候选都重新可达，进入 `skipped` 而不是被删：

```text
PLAN=a8e32e89bb644668b30e93951ac09362
CANDIDATES=sha256:0d772d80...2ccacf,sha256:75c1a7ab...22bc40
DELETED=
SKIPPED=sha256:0d772d80...2ccacf:reachable again: referenced by a tagged manifest,
        sha256:75c1a7ab...22bc40:reachable again: referenced by a tagged manifest
```

复核后标签与全部引用在两个进程上都可拉取：

```bash
$ curl -s http://127.0.0.1:8030/v2/demo/app/tags/list
{"name":"demo/app","tags":["v1"]}
$ for p in 8030 8031; do
    curl -s -o /dev/null -w "manifest v1 via $p: %{http_code}\n" http://127.0.0.1:$p/v2/demo/app/manifests/v1
  done
manifest v1 via 8030: 200
manifest v1 via 8031: 200
$ curl -s -o /dev/null -w 'layer blob via 8031: %{http_code}\n' \
    http://127.0.0.1:8031/v2/demo/app/blobs/sha256:0d772d80...2ccacf
layer blob via 8031: 200
```

#### 复核窗口内新产生的候选不会被误删

在上面 dry-run 与执行之间，若发生标签移动或悬空推送而产生**新的**不可达对象
（即使它已老过宽限期），执行时它**不在计划内，因而保留**；它只会出现在**下一次**
dry-run 的候选中，等待新一轮人工复核：

```bash
# 执行后：计划内旧垃圾已删，dry-run 之后才出现的新 blob 仍在
$ curl -s -o /dev/null -w '%{http_code}\n' \
    http://localhost:5000/v2/demo/app/blobs/sha256:c97adf18...
200

# 下一次 dry-run 才把它列为候选
$ curl -s -X POST -H 'Content-Type: application/json' \
    -d '{"dry_run": true, "grace_seconds": 0}' http://localhost:5000/gc
# {"plan_id": "23338789...", "delete": {"blobs": [{"digest": "sha256:c97adf18..."}]}, ...}
```

GC 语义：

- **两阶段复核**：dry-run 快照候选为持久化计划（`gc_plans` 等表），执行必须凭
  `plan_id`；计划缺失/未知/超过 1 小时/已执行一律拒绝，不删任何数据；计划跨
  重启有效，且只能执行一次。
- **跨进程读写锁（发布/GC/拉取协调）**：数据目录中的 `fcntl.flock` 锁文件
  （`.registry.lock`）把发布的「引用校验→标签提交」与 GC 的「认领后可达性
  复核→元数据/文件删除」在所有共享该目录的进程间串行化——所有路径统一
  「先跨进程锁、再进程内 RLock」。发布先提交则 GC 复核必见新标签、候选进入
  `skipped`；GC 先删除则发布校验返回 `400 MANIFEST_BLOB_UNKNOWN`、标签不创建
  不移动；拉取在「引用检查→打开句柄」窗口内持共享锁，句柄打开后锁外流式发送，
  GC 删除不影响进行中的下载。
- **跨进程原子认领**：执行的第一步是在 SQLite 写事务内
  `UPDATE gc_plans SET state='executing' WHERE id=? AND state='pending'
  AND expires_at>?` 并立即提交。WAL + `busy_timeout` 保证跨进程写串行，因此
  两个进程同时执行同一计划时只有一个 `rowcount==1` 的赢家；输家收到
  409 `GC_PLAN_IN_PROGRESS`，不读取候选、不做删除。进程内 RLock 只负责同进程
  内与发布/上传互斥，跨进程安全不依赖它。
- **认领即不可重放**：认领提交后若执行方异常或崩溃，计划永久停留在
  `executing`（`claimed_at`/`claimer` 记录认领时刻与 `host:pid` 实例）。重试
  同一 `plan_id` 一律 409，已删部分不会重复执行；剩余候选重新 dry-run 后用
  新计划审阅、回收。全部删除成功提交后状态才变为 `executed`。
- **执行时复核（TOCTOU 防护）**：认领成功后以**计划记录的宽限期**重新
  计算可达图与上传租约——计划内且仍不可达/仍超宽限/租约仍过期的才删除；
  重新可达、续租或已不存在的进入 `skipped`。
- **计划外不碰**：dry-run 之后因标签移动、删除、悬空推送新产生的候选不在计划内，
  本次执行绝不删除，留待下次 dry-run 审阅（上面已实测）。
- **根集合**：所有 tag 指向的 manifest；沿 `config`/`layers`/`manifests[]`/
  `subject` 遍历出全部可达 blob 与 manifest。
- **回收对象**：不可达且超出宽限期的 blob、不可达（无标签）的 manifest、
  租约已过期的上传会话。
- **保护机制**：未过期上传租约不会被清理；宽限期内的“新 blob”不会被清理
  （覆盖“已推送 blob、尚未发布 manifest”的并发窗口）；GC 与发布/上传由跨进程
  独占锁串行（见上），拉取在共享锁内打开文件句柄后再流式发送，文件随后被删也
  不影响进行中的下载。
- 删除顺序为先提交元数据删除、再删文件（发布方向同样是引用校验通过后才提交
  manifest 与标签），崩溃残留为无元数据的孤儿文件，由启动时的孤儿清扫兜底。

## 目录结构

```
app/
  main.py       # FastAPI 路由（/v2/*、/gc 两阶段复核与跨进程原子认领）
  registry.py   # 核心逻辑：上传、manifest CAS、GC 可达图与计划原子认领/复核执行；两层锁协调
  db.py         # SQLite 元数据（WAL + busy_timeout；gc_plans 支持 pending/executing/executed 三态原子认领）
  lock.py       # 跨进程读写锁：fcntl.flock（发布 EX / GC EX / 拉取 SH），同线程同模式可重入
  storage.py    # 本地文件存储：blobs/sha256/<hex> 内容寻址 + uploads/<uuid>
  digest.py     # digest/tag/repo 校验与 sha256 计算
  errors.py     # OCI 错误模型（含 GC_PLAN_*，认领失败为 GC_PLAN_IN_PROGRESS 409）
  config.py     # 环境变量配置（含锁文件路径 <data>/.registry.lock）
tests/test_registry.py   # 38 个端到端测试
Dockerfile / docker-compose.yml
```

## 本地开发与测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

测试覆盖：分块上传与续传、摘要校验失败、幂等重试、跨仓库去重与挂载、
manifest 引用校验、digest 不可变性、标签 CAS 冲突、GC dry-run/执行、
宽限期与租约保护、GC 与并发拉取/发布竞争、重启后恢复上传，以及 GC 复核计划：
dry-run 后新增候选不被误删、重新可达/续租跳过、计划缺失/未知/过期(410)/已执行
(409)拒绝、计划跨重启有效且只能执行一次；两个独立进程（各自的连接、锁
与 Registry 实例，fork 子进程）同时执行同一 `plan_id` 时恰好一个成功删除、竞争
方 409 `GC_PLAN_IN_PROGRESS` 且不碰候选；认领后模拟崩溃，计划保留为不可重放的
`executing`、重试 409、剩余候选可重新 dry-run 回收；长期未完成的僵死计划
`stale:true` 提示；旧两态（pending/executed）数据库平滑迁移。**新增跨进程
GC/发布竞态（5 个测试）：GC 先删除时另一进程的 PUT（新建标签与 CAS 移动已有
标签两种）收到 400 `MANIFEST_BLOB_UNKNOWN`、标签不创建不移动、旧引用两个进程
都可拉取；发布先提交时 GC 复核把候选判为 `reachable again` 并跳过、标签与引用
保持可拉取；进程内多线程同样不产生“标签可读而 blob 404”；跨进程读写锁本身
EX/SH 互斥与“句柄打开后删除不影响流式读取”的单测（去掉 flock 后这些测试会
失败）**。

## 设计说明与限制

- **共享数据目录与多进程**：SQLite 以 WAL 打开并设置 15s `busy_timeout`，
  多个进程可以挂载同一个数据目录（例如同一卷上的两个容器、滚动升级时的新旧
  实例并存）。跨进程一致性由两层机制共同保证：GC 执行通过 `gc_plans` 的原子
  状态认领互斥（同一计划只会被一个进程执行）；发布/GC/拉取的临界区通过
  `.registry.lock` 上的 `fcntl.flock` 读写锁串行化（发布与 GC 为独占、拉取为
  共享），其它元数据写事务由 SQLite 自身的写锁串行化。该锁依赖 POSIX
  `flock`（Linux/macOS），因此共享目录必须是支持劝告式文件锁的文件系统
  （本地磁盘、Docker named volume 均可；某些 NFS/CIFS 挂载需确认锁语义）。
  compose 默认仍以单实例运行；水平扩展到多节点仍需替换为外部数据库与共享存储。
- **命名空间隔离**：blob 物理上全局去重，但 `GET /v2/<repo>/blobs/<digest>`
  要求该 blob 已在 `<repo>` 命名空间内关联，跨仓库不可直接读。
- **manifest 不可变**：同一 digest 的内容恒定；digest 寻址的 PUT 会校验内容
  与摘要一致；标签只是指向 digest 的可变指针，且移动必须走 CAS。
