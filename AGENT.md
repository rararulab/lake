# AGENT.md — catalog

lake：具身智能 lakehouse（LanceDB 思路）。用户是 Ryan（资深后端/数据库工程师，
用中文交流）。本文件是**目录**，不是手册——按需读对应文档，不要指望这一个文件
说清一切（渐进式披露）。

## 硬规则（必须先读）

- 禁止在主 checkout 上改代码：先 `jj workspace add .worktrees/issue-N-<slug>`。
- VCS 是 jj（colocated git backend）：变更走 jj，只读 git 命令允许。
- **Local-first**：本地门禁比 CI 全（本地有 Docker，能跑 integration/localstack、
  e2e、全部慢测试）。CI 只是 **`main` 上的 post-merge 兜底**（catch Linux-vs-macOS
  平台差异），不在 PR/feature 分支跑。真正的门禁在本地。
- jj 不触发 git hooks（无暂存区）：
  - `jj push`（全局 alias → jj-pre-push）**自动**在推送前跑 `.pre-commit-config.yaml`
    的快门禁（fmt+clippy），不过不推。原生 `jj git push` 可绕过。
  - 完整门禁用 **`mise run ship`** = `mise run ci`（gate+doc+spec+**integration**）
    + conventional-commit 检查 + push。lane 1 另加 `mise run spec-lifecycle <spec>`。
- Conventional Commits：`<type>(<scope>): <description> (#N)`。
- 工具链只装 mise，其余 `mise install` 接管（Rust 例外：rustup +
  `rust-toolchain.toml`）。
- 架构不变量见 `docs/architecture.md`，违反需要显式决策。

## 渐进式披露的两条腿

- **每个 folder 有自己的 `AGENT.md`**（目录卡片：用途、不变量、布局）——
  进哪个目录先读哪张卡片，不要全局灌上下文。
- **codegraph**：探索代码优先用 code-review-graph MCP 工具
  （semantic_search_nodes / query_graph / get_impact_radius），比
  Grep/Read 省 token 且带结构上下文；索引重建 `mise run codegraph`，
  增量更新 `mise run codegraph-update`。

## 按需读什么

| 你要做的事 | 读 |
|---|---|
| 理解产品方向、判断需求该不该做 | `goal.md` |
| 理解系统设计、架构不变量、crate 划分 | `docs/architecture.md` |
| 开 issue / 写 spec（lane 1 vs lane 2 triage） | `specs/README.md`，继承约束在 `specs/project.spec` |
| 实现 / 评审 / 验证一个 issue（角色契约） | `harness/roles/{spec-author,implementer,reviewer,verifier}.md` |
| 端到端流程（issue → workspace → PR → merge） | `docs/guides/workflow.md` |
| 改 mise / Bun scripts / hooks / GitHub CI | `docs/guides/mise-ci.md` |
| 改 local deploy / localstack (Docker) | `docs/guides/local-deploy.md` |
| 改 `lake-cli` 命令行体验 | `docs/guides/cli.md`，`crates/lake-cli/AGENT.md` |
| Rust 风格 | `docs/guides/rust-style.md` |
| 提交信息规范 | `docs/guides/commit-style.md` |
| 注释规范 / 反模式清单 | `docs/guides/code-comments.md` / `docs/guides/anti-patterns.md` |
| 可用命令 | `mise tasks`（定义在 `mise.toml`） |

## 常用命令

```bash
mise run doctor          # 新会话第一步：环境健康检查
mise run gate            # push 前质量门禁：hooks + Rust tests + e2e + site
mise tasks               # 列出全部任务
```
