# Repository instructions

## Product context

- Read `README.md`, `portfolio.yaml`, and `docs/architecture.md` before changing architecture.
- Read active decisions in `docs/decisions/` before replacing an established choice.
- Keep `memory/STATUS.md` and `memory/FUTURE.md` current when those files exist.

## Code discovery

- If `.codegraph/` exists, use CodeGraph before text search for functions, classes, routes, and call paths.
- Use text search for literals, error messages, environment variables, documentation, and configuration.
- Do not index or search dependency directories, virtual environments, generated output, databases, or secrets.

## Development rules

- Preserve standalone operation and integrate through public contracts.
- Define or update contracts before changing public API, CLI, event, or configuration behavior.
- Prefer additive, backward-compatible interface changes.
- Validate external input and provider responses at boundaries.
- Keep business logic independent from provider SDKs and storage implementation.
- Add or update tests with every behavior change.
- Run `make check` before claiming completion.
- Preserve public API compatibility and update changelog/migration notes when behavior changes.
- Treat accessibility, performance, security, privacy, and documentation as acceptance criteria for user-facing work.

## Configuration and secrets

- Use `.env.example` for names and safe defaults; never commit `.env`.
- Use `OPENROUTER_API_KEY` for OpenRouter. Never place an OpenRouter key in `OPENAI_API_KEY`.
- Use direct provider variables only when the product calls that provider directly.
- Never print secrets, authorization headers, full environments, raw prompts, or sensitive documents.
- Live AI tests require both credentials and `ALLOW_LIVE_AI_TESTS=true`.

## Repository hygiene

- Never commit `node_modules`, virtual environments, caches, build output, runtime databases, WAL/SHM files, generated indexes, logs, uploads, `.autoclaw`, or `.codegraph` data.
- Preserve unrelated user changes in a dirty worktree.
- Do not use destructive Git or filesystem operations without explicit authorization.
- Keep runtime versions, lockfiles, README requirements, and CI aligned.

## Documentation and decisions

- Document significant architectural exceptions as ADRs in `docs/decisions/`.
- README claims must distinguish verified, implemented, experimental, and planned behavior.
- Update `CHANGELOG.md` for user-visible behavior.
- Follow `CONTRIBUTING.md`, `SECURITY.md`, `GOVERNANCE.md`, and the pull-request template.
- Do not weaken CI, branch, dependency, or release-security controls to make a change pass.
- New release dependencies require a license and maintenance-risk review.
