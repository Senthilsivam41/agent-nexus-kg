# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- **Interactive Showcase Dashboard (`scale_kg.showcase`):**
  - Mounted FastAPI `/showcase` dashboard UI and `/showcase/api/*` endpoints (`/showcase/api/snapshot`, live simulation, guardrail probe, hot-path promo, hybrid RAG, and ramp endpoints) with ETag caching and concurrency backpressure.
  - Zero-build 5-tab static dashboard (`index.html`, `styles.css`, `app.js`, `charts.js`, `views.js`) packaged via `[tool.setuptools.package-data]`.
  - Static showcase bundle exporter (`python -m scale_kg.showcase.build`) generating `index.html`, `snapshot.json`, `history.json`, `assets/*`, and benchmark delta Markdown summaries.
  - Local Makefile targets `make showcase-build` and `make showcase`.
  - Netlify configuration (`netlify.toml`) with security/CSP headers and cache rules, plus `.github/workflows/showcase.yml` for automated PR previews and production deploys.
- Updated `portfolio.yaml` REST endpoint inventory with `/showcase` and `/showcase/api/snapshot`.
