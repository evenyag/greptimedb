# Repository Guidelines

## Project Structure & Module Organization
- `Cargo.toml` defines a multi-crate Rust workspace. Primary code lives under `src/` (each subdirectory is a crate/module group such as `src/frontend`, `src/sql`, `src/store-api`).
- Tests are split by type:
  - Unit tests live alongside code with `#[cfg(test)]`.
  - Integration tests live in `tests/` and `tests-integration/`.
  - Fuzz targets live in `tests-fuzz/`.
- Configuration examples and docs are in `config/` and `docs/`.
- Tooling and automation scripts live in `scripts/` and `Makefile`.

## Build, Test, and Development Commands
- `make build` or `cargo build`: debug build of the workspace.
- `make RELEASE=true build` or `cargo build --release`: optimized build.
- `make fmt` / `make fmt-check`: format Rust code or verify formatting.
- `make clippy`: run clippy with warnings treated as errors.
- `make check`: type-check all targets without building.
- `make test`: run the full test suite via `cargo nextest`.
- `make sqlness-test`: run SQL integration tests.
- `make check-udeps` / `make fix-udeps`: detect and clean unused dependencies.
- If you change sample configs in `config/`, run `make config-docs` (requires Docker).

## Coding Style & Naming Conventions
- Rust formatting is enforced with `cargo fmt --all` (`rustfmt.toml`).
- Linting uses clippy; warnings are treated as errors.
- Follow the Rust style guide referenced in `docs/style-guide.md` and the PingCAP Rust style guide linked in `CONTRIBUTING.md`.
- Keep module naming consistent with existing crate layout (e.g., `src/mito2`, `src/meta-srv`).

## Testing Guidelines
- Preferred runner is `cargo nextest` (via `make test`).
- SQL tests use the sqlness framework with `.sql`/`.result` files.
- Fuzzing is available via `cargo fuzz` targets in `tests-fuzz/`.

## Commit & Pull Request Guidelines
- Commit messages and PR titles follow Conventional Commits (e.g., `feat: add foo` or `fix: handle bar`).
- PR descriptions should explain motivation and design for non-trivial changes, and call out breaking/API changes.
- Ensure license headers are present and run all required checks before opening a PR.
- Do not add AI tool signatures or co-author lines to commit messages.

## Security & Configuration Tips
- For security issues, follow the process described in `SECURITY.md`.
- Keep configuration docs in sync with `config/` changes using `make config-docs`.
