# CLAUDE.md — Lake Development Guide

**新会话第一步：`mise run doctor`**（用户只需自装 mise，其余工具 `mise install`
接管；Rust 例外，由 rustup + `rust-toolchain.toml` 管）。

## Communication
- 用中文与用户交流

## 渐进式披露

本文件和 `AGENT.md` 都是**目录**：按需读对应文档，不要把全部内容塞进上下文。
每个 folder 有自己的 `AGENT.md` 目录卡片；探索代码优先用 code-review-graph
MCP 工具（索引：`mise run codegraph`）。

- `AGENT.md` — 硬规则 + 完整文档目录（按任务查表）
- `goal.md` — 北极星：what lake is / is NOT；任何新需求先拿它做门禁
- `docs/architecture.md` — 系统设计、读路径、commit 协议、架构不变量、crate 划分
- `specs/README.md` — lane 1（spec 驱动、BDD 绑定测试）vs lane 2（轻量 chore）triage
- `harness/roles/*.md` — 角色契约（spec-author / implementer / reviewer / verifier）；`.claude/agents/*.md` 是薄包装
- `docs/guides/*.md` — workflow / rust-style / commit-style / code-comments / anti-patterns

## 硬规则

- 改代码先建 workspace：`jj workspace add .worktrees/issue-N-<slug>`，禁止在主
  checkout 上开发（`.claude/hooks/guard-main-branch.ts` 强制）。
- jj 不触发 git hooks：push 前必须 `mise run gate`（lane 1 另加
  `mise run spec-lifecycle <spec>`）；CI 是兜底。
- Conventional Commits，由 CI 和 reviewer 强制。
- 所有变更走 issue → workspace → PR → merge，无例外。

## 常用命令

```bash
mise run doctor          # 新会话第一步：环境健康检查
mise run gate            # 质量门禁：hooks + test + e2e
mise run e2e             # 端到端自检：ingest -> commit -> SQL
mise run spec-lifecycle specs/issue-N-<slug>.spec.md   # lane-1 BDD 验证
mise tasks               # 全部任务列表（定义在 mise.toml）
jj st / jj log           # 工作副本状态 / 历史
jj commit -m "type(scope): msg (#N)"
```
