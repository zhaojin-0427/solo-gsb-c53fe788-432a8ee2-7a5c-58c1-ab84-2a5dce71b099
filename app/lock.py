"""跨进程读写锁（基于 fcntl.flock 的劝告式文件锁，Linux/macOS）。

背景：多个进程挂载同一个数据目录（例如共享同一卷的两个容器、滚动升级时
新旧实例并存）时，进程内的 threading.RLock 互不可见——GC 的「可达性复核
→ 元数据删除 → 文件删除」与发布的「引用校验 → manifest 入库 → 标签 CAS
提交」可能交错，产生“标签可读而 blob 已 404”的悬空引用。

本锁以数据目录中的一个锁文件（.registry.lock）为跨进程仲裁点：

- 共享锁（SH）：拉取 blob 时，在“引用检查 → 打开文件句柄”的窗口内持有。
  GC 删除（EX）必须等所有 SH 释放，保证句柄一定在删除前打开；句柄打开后
  立即释放并在锁外流式发送（POSIX 下已打开的 fd 不受随后 unlink 影响）。
- 独占锁（EX）：manifest 发布的引用校验→标签提交全程、GC 认领成功后的
  可达性复核→元数据/文件删除→标记 executed 全程、GC dry-run 快照、
  blob 提交/挂载、上传会话写操作以及启动恢复。

flock 与“打开文件描述（open file description）”绑定：同一进程用不同 fd
获取的锁互相独立（因此同进程的不同线程之间同样互斥）。这里每次加锁都
新开一个 fd，并在线程本地维护递归计数：同一线程重复获取同模式锁时只
增加深度、不重复系统调用；深度归零才 LOCK_UN 并关闭 fd。

加锁顺序约定：所有路径一律“先 flock、再进程内 RLock”，避免
（flock EX 等 RLock，持 RLock 等 flock SH）的环形等待。
"""
from __future__ import annotations

import fcntl
import os
import threading
from pathlib import Path


class CrossProcessRWLock:
    SHARED = "shared"
    EXCLUSIVE = "exclusive"

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self._local = threading.local()

    def _depth(self) -> int:
        return getattr(self._local, "depth", 0)

    def _mode(self):
        return getattr(self._local, "mode", None)

    def _acquire(self, exclusive: bool):
        mode = self.EXCLUSIVE if exclusive else self.SHARED
        depth = self._depth()
        if depth > 0:
            # 线程内重入：仅支持同模式嵌套（当前调用图中不存在锁升级）。
            if self._mode() != mode:
                raise RuntimeError(
                    f"cannot re-enter cross-process lock as {mode!r}"
                    f" while holding {self._mode()!r} in the same thread"
                )
            self._local.depth = depth + 1
            return
        fd = os.open(self.path, os.O_RDWR)
        try:
            # 阻塞等待；SQLite busy_timeout 负责元数据写锁，这里负责
            # 跨进程的发布/GC/拉取临界区顺序。
            fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        except BaseException:
            os.close(fd)
            raise
        self._local.fd = fd
        self._local.mode = mode
        self._local.depth = 1

    def _release(self):
        depth = self._depth()
        if depth <= 0:
            raise RuntimeError("cross-process lock released without being held")
        depth -= 1
        self._local.depth = depth
        if depth == 0:
            fd = self._local.fd
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
            self._local.fd = None
            self._local.mode = None

    def shared(self) -> "Locked":
        return Locked(self, exclusive=False)

    def exclusive(self) -> "Locked":
        return Locked(self, exclusive=True)


class Locked:
    __slots__ = ("_lock", "_exclusive", "_held")

    def __init__(self, cross_lock: CrossProcessRWLock, exclusive: bool):
        self._lock = cross_lock
        self._exclusive = exclusive
        self._held = False

    def __enter__(self):
        self._lock._acquire(self._exclusive)
        self._held = True
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._held:
            self._lock._release()
            self._held = False
        return False
