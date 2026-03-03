# PLAN.md — cpapacket Technical Implementation Plan

---

## 1. Overview & Goals

### 1.1 What Is cpapacket?

`cpapacket` is a **single-user CLI tool** that generates a structured, CPA-ready tax reporting packet for a specified tax year. It pulls financial data from **QuickBooks Online (QBO)** and **Gusto**, transforms it into normalized models, and produces PDFs, CSVs, and raw JSON files organized into a standardized directory hierarchy. The final output is a zip archive ready for accountant handoff.

### 1.2 Core Goals

| # | Goal | Rationale |
|---|------|-----------|
| G1 | Automate CPA packet assembly | Eliminate manual export/rename/organize cycle each tax season |
| G2 | Ensure data completeness | Validation system flags missing deliverables without blocking |
| G3 | Maintain auditability | Raw JSON preserved alongside derived PDF/CSV for traceability |
| G4 | Support iterative workflow | Overwrite/Copy/Abort prompts; idempotent re-runs; easy post-CPA updates |
| G5 | Stay within reporting scope | No tax calculations, no filings, no auto-corrections — reporting only |
| G6 | Resumable and incremental | Partial failures can be resumed; unchanged deliverables can be skipped |

### 1.3 Core Behavioral Invariants

- Never deletes source folders.
- Never blocks zip creation due to missing items; warns and records.
- Creates packet folders lazily, except `_meta/` which always exists.
- Deterministic output naming and stable folder structure.
- Atomic file writes: never leave partial outputs with the expected filename.
- Deliverables record their inputs + schema versions so re-runs can safely skip or regenerate.
- All output filenames include the tax year for unambiguous identification when extracted individually.

### 1.4 Target User Profile

- Single S-Corp owner-operator
- Uses QBO for bookkeeping, Gusto for payroll (Gusto optional — see ADR-13)
- Contractors paid via Stripe/Zelle (not Gusto)
- Files 1120-S + personal returns via CPA
- Delaware incorporation, New York resident
- Wants a "push-button" packet each tax season

### 1.5 Non-Goals (Explicit)

- Multi-user / multi-company support
- Web UI or dashboard
- Tax calculation or liability estimation
- Automatic reclassification of transactions
- Calendar/reminder integrations
- Receipt tracking or document management

---

## 2. High-Level Architecture & Tech Stack Justification

### 2.1 Architecture Overview

```
┌──────────────────────────────────────────────────┐
│                  CLI Entry Point                  │
│       (click command router + RunContext)          │
└──────────┬───────────────────────────┬───────────┘
           │                           │
     ┌─────▼──────────────────────────────────────┐
     │          Deliverable Registry               │
     │  (Protocol-based; ordered by dependency)    │
     └─────┬───────────────────────────┬───────────┘
           │                           │
     ┌─────▼──────────────────────────────────────┐
     │            Session Data Store               │
     │  (in-memory + _meta/cache/ disk per year)   │
     └─────┬───────────────────────────┬───────────┘
           │                           │
     ┌─────▼──────────────────────────────────────┐
     │     Shared Retry + Rate Limiter Layer       │
     └─────┬───────────────────────────┬───────────┘
           │                           │
     ┌─────▼──────┐            ┌──────▼───────┐
     │  QBO Client │            │ Gusto Client │
     │ (httpx+OAuth)│           │(httpx+OAuth) │
     │ [required]  │            │ [optional]   │
     └─────┬──────┘            └──────┬───────┘
           │                           │
     ┌─────▼───────────────────────────▼───────┐
     │          Normalization Layer             │
     │  (Raw API → Pydantic domain models)     │
     └─────┬───────────────────────────┬───────┘
           │                           │
     ┌─────▼──────┐            ┌──────▼───────┐
     │ CSV Writer  │            │  PDF Writer  │
     │ (atomic)    │            │  (atomic)    │
     └─────┬──────┘            └──────┬───────┘
           │                           │
     ┌─────▼───────────────────────────▼───────┐
     │        Packet Assembler + Metadata       │
     │  (dir mgmt, validation, fingerprint, zip)│
     └─────────────────────────────────────────┘
```

### 2.2 Tech Stack

| Layer | Choice | Justification |
|-------|--------|---------------|
| **Language** | Python 3.11+ | Rich ecosystem for PDF/CSV; rapid iteration; modern type syntax |
| **CLI Framework** | `click` + `rich` | `click` for subcommand routing, prompts, testable invocations; `rich` for progress bars, tables, panels, colored output |
| **Data Modeling** | `pydantic` v2 | Parse-time validation for financial data, JSON serialization for metadata, schema generation; frozen models ensure immutability |
| **HTTP Client** | `httpx` | Connection pooling, first-class timeouts (connect=10s, read=90s), HTTP/2 support, async-ready; pairs with `respx` for testing |
| **QBO Integration** | Thin client on `httpx` + OAuth2 | Direct control over report endpoints; no SDK abstraction layer needed for Reports API |
| **Gusto Integration** | Thin client on `httpx` + OAuth2 | Same HTTP foundation; consistent retry/timeout behavior; optional — tool degrades gracefully without it |
| **PDF Generation** | `reportlab` | Precise CPA-grade layout control; headers, footers, page numbers, indentation |
| **CSV** | stdlib `csv` | No dependencies needed; supports streaming writes for large datasets |
| **JSON** | stdlib `json` | Raw API responses stored as-is |
| **OAuth Token Storage** | `keyring` + local encrypted JSON fallback | Secure credential storage without plaintext files |
| **Config Directory** | `platformdirs` | Cross-platform user config path (`~/.config/cpapacket/`) for persistent local state (tax tracker, cached tokens fallback) |
| **Testing** | `pytest` + `respx` | Mock `httpx` transports for API tests; deterministic concurrency testing |
| **Packaging** | `pyproject.toml` + `pip install -e .` | Standard Python packaging; `[project.scripts]` for CLI entry point; target PyPI + `pipx install cpapacket` |
| **Linting** | `ruff` | Fast, comprehensive Python linter/formatter |
| **Type Checking** | `mypy` (strict) | Catch normalization model errors at dev time |

### 2.3 Project Structure

```
cpapacket/
├── pyproject.toml
├── README.md
├── PLAN.md
├── src/
│   └── cpapacket/
│       ├── __init__.py
│       ├── cli/
│       │   ├── __init__.py
│       │   ├── main.py              # click group, global flags, RunContext construction, --year inference
│       │   ├── pnl.py               # thin wrapper → PnlDeliverable
│       │   ├── balance_sheet.py     # thin wrapper → BalanceSheetDeliverable
│       │   ├── prior_balance_sheet.py
│       │   ├── general_ledger.py
│       │   ├── payroll_summary.py
│       │   ├── contractor_summary.py
│       │   ├── tax_tracker.py       # `cpapacket tax init/update/status/mark-paid`
│       │   ├── retained_earnings.py
│       │   ├── distributions.py
│       │   ├── payroll_recon.py
│       │   ├── check.py             # `cpapacket check` (QBO data health pre-check)
│       │   ├── doctor.py            # `cpapacket doctor` (environment/auth health)
│       │   └── build.py             # `cpapacket build` — iterates deliverable registry
│       ├── deliverables/
│       │   ├── __init__.py
│       │   ├── base.py              # Deliverable Protocol + DeliverableResult
│       │   ├── registry.py          # DELIVERABLE_REGISTRY: ordered list with dependency resolution
│       │   ├── pnl.py
│       │   ├── balance_sheet.py
│       │   ├── prior_balance_sheet.py
│       │   ├── general_ledger.py
│       │   ├── payroll_summary.py
│       │   ├── contractor_summary.py
│       │   ├── tax_tracker.py
│       │   ├── payroll_recon.py
│       │   ├── retained_earnings.py
│       │   └── distributions.py
│       ├── core/
│       │   ├── __init__.py
│       │   ├── context.py           # RunContext: immutable config object for entire run
│       │   ├── filesystem.py        # atomic_write, safe directory creation, path sanitization
│       │   ├── metadata.py          # DeliverableMetadata read/write + input fingerprinting
│       │   ├── retry.py             # Shared retry policy: 429 backoff, 5xx retry, 4xx fail-fast
│       │   └── limiter.py           # Bounded concurrency semaphore; per-service limits
│       ├── data/
│       │   ├── __init__.py
│       │   ├── store.py             # SessionDataStore: in-memory + disk cache
│       │   ├── keys.py              # Cache key definitions (endpoint + param tuples)
│       │   └── providers.py         # High-level data accessors (get_pnl, get_balance_sheet, etc.)
│       ├── clients/
│       │   ├── __init__.py
│       │   ├── qbo.py               # QBO API client (httpx + OAuth)
│       │   ├── gusto.py             # Gusto API client (httpx + OAuth)
│       │   └── auth.py              # Shared OAuth token management
│       ├── models/
│       │   ├── __init__.py
│       │   ├── normalized.py        # NormalizedRow (Pydantic, shared by P&L and Balance Sheet)
│       │   ├── general_ledger.py    # GeneralLedgerRow (Pydantic)
│       │   ├── payroll.py           # Payroll domain models (Pydantic)
│       │   ├── contractor.py        # ContractorRecord (Pydantic)
│       │   ├── tax_tracker.py       # EstimatedTaxPayment, TaxDeadline (Pydantic)
│       │   ├── retained_earnings.py # RE rollforward models (Pydantic)
│       │   └── distributions.py     # Distribution + miscoding candidate models (Pydantic)
│       ├── writers/
│       │   ├── __init__.py
│       │   ├── csv_writer.py        # CSV output (batch + streaming modes, via atomic_write)
│       │   ├── pdf_writer.py        # CPA-grade PDF (reportlab, via atomic_write)
│       │   └── json_writer.py       # Raw JSON dump (via atomic_write)
│       ├── packet/
│       │   ├── __init__.py
│       │   ├── structure.py         # Directory naming, creation, path resolution
│       │   ├── validator.py         # Completeness checks (uses metadata + filesystem + registry)
│       │   ├── health_check.py      # QBO data quality pre-check
│       │   └── zipper.py            # Zip archive creation
│       ├── reconciliation/
│       │   ├── __init__.py
│       │   ├── payroll_recon.py     # Gusto ↔ QBO payroll check
│       │   ├── retained_earnings.py # RE rollforward calculation logic
│       │   └── miscode_detector.py  # Unified distribution miscoding detection engine
│       └── utils/
│           ├── __init__.py
│           ├── logging.py           # Logging config (console RichHandler + file handler)
│           ├── prompts.py           # Overwrite/Copy/Abort logic (respects RunContext)
│           ├── dates.py             # Fiscal year helpers, --year inference
│           ├── formatting.py        # Currency formatting, indentation
│           └── constants.py         # Folder names, patterns, thresholds, schema versions, tolerances
├── tests/
│   ├── conftest.py
│   ├── fixtures/                    # Sample QBO/Gusto API responses
│   ├── test_cli/
│   ├── test_core/                   # RunContext, retry, limiter, filesystem, metadata tests
│   ├── test_deliverables/           # Deliverable implementations (unit-testable without CLI)
│   ├── test_clients/
│   ├── test_data/                   # SessionDataStore + cache tests
│   ├── test_models/
│   ├── test_writers/
│   ├── test_packet/
│   └── test_reconciliation/
└── docs/
    └── auth_setup.md               # OAuth setup instructions
```

### 2.4 Key Architectural Decisions

**ADR-1: Flat normalization into Pydantic models.**
QBO reports return deeply nested JSON. We flatten into frozen Pydantic `BaseModel` subclasses immediately after API pull. This decouples writers from API structure, validates data at parse time (enforcing types, non-negative levels, valid Decimal amounts), and provides free JSON serialization for metadata. A single `NormalizedRow` model is shared by both P&L and Balance Sheet — the balance sheet normalizer validates that sections are constrained to `{"Assets", "Liabilities", "Equity"}` at parse time.

**ADR-2: Lazy directory creation.**
Deliverable folders are created only when a command runs and produces output. The `_meta/` folder is the only exception (always created by `build`).

**ADR-3: Minimal persistent state.**
Interactive prompts (contractor account confirmation, owner keyword entry) happen per-run — no persistent config for these. However, two categories of data *are* persisted in `~/.config/cpapacket/`:
- **Tax tracker & deadlines** (by year): must survive across sessions since the user edits them over weeks/months
- **OAuth tokens** (via `keyring` primary, config dir fallback): must persist across sessions

All other state is derived from API calls and cached in `_meta/` per ADR-7.

**ADR-4: Monthly slicing for General Ledger.**
QBO report API has undocumented size limits. Pulling 12 monthly slices and merging is more reliable than a single annual pull. Slices are cached individually for resumability.

**ADR-5: Raw JSON always preserved.**
Every deliverable saves the raw API response as `*_raw.json` or `*_data.json`. This provides an audit trail and allows re-generation without re-fetching.

**ADR-6: Warnings never block.**
Missing deliverables, reconciliation mismatches, and structural flags produce warnings and exit code 2, but never prevent zip creation. The CPA packet is always producible.

**ADR-7: Session-scoped data cache with disk persistence.**
A `SessionDataStore` sits between API clients and business logic. On first request for a given (endpoint, parameters) tuple, it calls the API, caches the raw JSON in memory, and writes it to `_meta/cache/{endpoint_hash}.json`. On subsequent requests within the same session or a later session for the same tax year, it returns the cached response. During `build`, a single `SessionDataStore` instance is shared across all deliverable generators, ensuring zero duplicate API calls. The `--force` flag bypasses the cache and re-fetches everything. The General Ledger's 12 monthly slices are cached individually so that a partial failure can resume from the last successful month.

**ADR-8: Structured logging with dual output.**
All operations log to both console (via `rich.logging.RichHandler` at INFO level by default) and a persistent file (`_meta/cpapacket.log` via `logging.FileHandler` at DEBUG level always). The file log captures: timestamps, API endpoints called, response status codes, cache hits/misses, row counts for each deliverable, validation results, and any warnings or errors. Console verbosity is controlled by `--verbose` (DEBUG to console) and `--quiet` (WARNING only). The log file is included in the zip archive alongside the validation report.

**ADR-9: Per-deliverable metadata with input fingerprinting.**
Each deliverable command writes `_meta/{deliverable}_metadata.json` after successful generation. The metadata records the exact inputs, a deterministic fingerprint (sha256 of canonicalized input dict), the list of generated artifacts, schema versions, and any warnings. The build command's `--incremental` mode uses these fingerprints to skip regeneration when inputs haven't changed and outputs pass integrity checks (file exists + size > 0). The `--force` flag overrides this and regenerates unconditionally.

**ADR-10: Atomic file writes.**
All output files (PDF, CSV, JSON, metadata) are written atomically: content is written to a `.tmp` sibling file, then renamed to the final path via `os.replace()`. This ensures that the expected filename never contains partial or corrupt content. The validation engine can safely treat "file exists" as "file is complete." Writers accept a `Path` and call `atomic_write()` from `core/filesystem.py`.

**ADR-11: Shared retry and rate-limit infrastructure.**
All HTTP requests flow through a shared retry decorator (`core/retry.py`) and a shared concurrency limiter (`core/limiter.py`). The retry policy: 429 → exponential backoff with jitter, honoring the `Retry-After` header if present, up to `RETRY_MAX_429` retries; 5xx → `RETRY_MAX_5XX` retries with backoff; other 4xx → fail immediately with descriptive error. The limiter enforces per-service concurrency bounds (QBO: `QBO_MAX_CONCURRENCY`, Gusto: `GUSTO_MAX_CONCURRENCY`) regardless of how many deliverables are generating simultaneously. This prevents rate-limit storms when `build` runs concurrent steps that each make multiple API calls. Explicit timeouts: connect 10s, read 90s.

**ADR-12: Deliverable Protocol with registry-driven build.**
Every deliverable implements a `Deliverable` protocol defining `key`, `folder`, `required`, `dependencies`, `gather_prompts()`, and `generate()`. A `DELIVERABLE_REGISTRY` in `deliverables/registry.py` lists all deliverables in dependency order. The build command iterates the registry rather than hardcoding steps. CLI modules are thin wrappers that parse arguments and delegate to the deliverable implementation, keeping all business logic in `deliverables/` and making it unit-testable without `CliRunner`. Adding a new deliverable means implementing the protocol, registering it, and adding a thin CLI wrapper.

**ADR-13: Graceful Gusto degradation.**
Gusto connectivity is optional. If Gusto auth is not configured (no tokens in keyring, no credentials), the build command auto-skips Gusto-dependent deliverables (Payroll Summary and Payroll Reconciliation) with a clear warning. The validation report marks them as "Skipped (Gusto not connected)" rather than "Missing." This allows the tool to produce a useful packet for users who don't use Gusto, without requiring code changes or special flags. QBO is always required.

---

## 3. Feature Breakdown

### 3.1 Epic Map

| Epic | Deliverable(s) | Priority | Data Source | MVP? |
|------|----------------|----------|-------------|------|
| E1 – Core Packet Infrastructure | `build`, packet structure, validation, zip, health check, doctor | P0 | N/A | Yes |
| E2 – Profit & Loss | 01_Year-End_Profit_and_Loss | P0 | QBO | Yes |
| E3 – Balance Sheets | 02_Year-End_Balance_Sheet (current + prior) | P0 | QBO | Yes |
| E4 – General Ledger | 03_Full-Year_General_Ledger | P0 | QBO | Yes |
| E5 – Payroll Summary | 04_Annual_Payroll_Summary | P0 | Gusto (optional) | Yes |
| E6 – Contractor Summary | 07_Contractor_1099_Summary | P0 | QBO | Yes |
| E7 – Estimated Tax Tracker | 08_Estimated_Tax_Payments | P1 | Local JSON | Yes |
| E8 – Payroll Reconciliation | 10_Payroll_Reconciliation | P0 | QBO + Gusto (optional) | Yes |
| E9 – Retained Earnings Rollforward | 09_Retained_Earnings_Rollforward | P0 | QBO (cross-deliverable) | Yes |
| E10 – Distributions Summary | 06_Shareholder_Distributions | P0 | QBO | Yes |
| E11 – Authentication & Token Mgmt | OAuth flows for QBO + Gusto + doctor | P0 | N/A | Yes |

### 3.2 Epic Details

---

#### E1 – Core Packet Infrastructure

**Components:**

**1. Packet Structure Manager** (`packet/structure.py`)
- Derives `{CompanyName}_{TaxYear}_CPA_Packet/` from QBO Company Info API
- Sanitizes company name for filesystem: replace spaces with `_`, remove `/ \ : * ? " < > |`, collapse multiple underscores
- Creates directories lazily on first write
- Always creates `_meta/` on `build`

**2. Validation Engine** (`packet/validator.py`)
- Iterates the `DELIVERABLE_REGISTRY` to determine expected files per deliverable
- Cross-references per-deliverable metadata in `_meta/` for fingerprint and artifact checks
- Uses regex patterns to match naming conventions
- Classifies each deliverable as Present / Missing / Incomplete / Skipped
- Writes `_meta/validation_report.txt`
- Returns structured validation result for exit code determination

**3. Zip Archiver** (`packet/zipper.py`)
- Creates `<base_dir>/<PacketName>.zip`
- Includes entire packet folder + validation report
- Handles existing zip: Overwrite / Copy (timestamped) / Abort (respects `RunContext.on_conflict`)
- Does NOT delete source folder

**4. Build Command** (`cli/build.py`)
- Default mode: authenticate → check Gusto availability → collect interactive inputs upfront → generate all deliverables via registry in dependency order → validate → write report → zip
- Validate-only mode (`--validate-only`): skip generation, validate existing files → report → zip
- Incremental mode (`--incremental`): skip deliverables whose input fingerprint matches and outputs pass integrity checks
- Force mode (`--force`): ignore all caches and metadata, re-fetch and regenerate everything
- Pre-flight auth check:
  - QBO: **required**. If not authenticated, exit 1 with `cpapacket auth qbo login` instructions.
  - Gusto: **optional**. If not authenticated, log warning, auto-skip Payroll Summary (E5) and Payroll Reconciliation (E8). Validation marks them as "Skipped (Gusto not connected)."
- Build derives P&L parameters from `--year YYYY`:
  - `start_date` = `YYYY-01-01`, `end_date` = `YYYY-12-31`
  - `accounting_method` = value of `--method` flag (default: `accrual`, since S-Corp 1120-S typically requires accrual basis)
- Generation is driven by the `DELIVERABLE_REGISTRY`:

  ```python
  registry = get_ordered_registry()  # topological sort by dependencies
  prompts = {d.key: d.gather_prompts(ctx) for d in registry if needs_prompt(d)}
  for deliverable in registry:
      if deliverable.key in ctx.skip:
          continue
      if ctx.incremental and deliverable.is_current(ctx):
          continue
      result = deliverable.generate(ctx, store, prompts.get(deliverable.key, {}))
      results.append(result)
  ```

- Steps 2–5 (P&L, BS current, BS prior, GL) can run concurrently (no interdependencies) via `concurrent.futures.ThreadPoolExecutor`, subject to the shared rate limiter
- If any step fails: log error, continue to next step, flag in validation
- Interactive prompts (contractor accounts, owner keywords, file conflict preference) are collected upfront before generation begins via `gather_prompts()`, so the user isn't interrupted mid-run
- Selective skip: `--skip payroll,contractor` omits specific deliverables from generation
- Exit code precedence (highest wins):
  - **1** — any deliverable experienced a hard failure (API error, auth failure, filesystem error)
  - **2** — zip created, no hard failures, but one or more required deliverables are missing or failed
  - **0** — zip created, all required deliverables present, no hard failures
- A zip is always created if at least one deliverable succeeded, regardless of exit code. The validation report inside the zip documents which deliverables failed and why.

**CLI surface:**
```
cpapacket build --year YYYY --out <base_dir>                       # generate + validate + zip (accrual)
cpapacket build --year YYYY --out <base_dir> --method cash         # generate with cash-basis P&L
cpapacket build --year YYYY --out <base_dir> --validate-only       # validate + zip (no API calls)
cpapacket build --year YYYY --out <base_dir> --incremental         # skip unchanged deliverables
cpapacket build --year YYYY --out <base_dir> --force               # re-fetch and regenerate everything
cpapacket build --year YYYY --out <base_dir> --skip payroll        # generate all except payroll
```

**5. File Exists Handler** (`utils/prompts.py`)
- Reusable Overwrite / Copy / Abort prompt
- Copy suffix: `__copy_YYYYMMDD_HHMMSS`
- Behavior determined by `RunContext.on_conflict`:
  - Interactive (default): prompt Overwrite / Copy / Abort
  - Non-interactive without explicit `--on-conflict`: **Abort** (safe default — prevents silent data destruction in headless scripts)
  - `--on-conflict overwrite|copy|abort` flag allows explicit control without prompting

**6. QBO Data Health Pre-Check** (`packet/health_check.py`)
- Runs as first step of `build` (can also be invoked standalone: `cpapacket check --year YYYY`)
- Checks (non-blocking warnings, never prevents packet generation):
  - **Uncategorized transactions**: queries QBO for transactions posted to "Uncategorized Income" or "Uncategorized Expense" during the tax year. If count > 0, warns with count and dollar total.
  - **Undeposited funds balance**: checks if "Undeposited Funds" account has a non-zero balance as of 12/31.
  - **Suspense/Ask My Accountant balance**: checks for non-zero balance in any account named "Ask My Accountant" or "Suspense."
  - **Open items from prior years**: counts unpaid invoices/bills dated before 01/01/YYYY.
  - **Payroll sync status**: if Gusto is connected, checks that the most recent payroll run's QBO sync completed.
- Output: warnings to console + `_meta/data_health_check.txt`
- If any issues found in interactive mode: prompts "Data quality issues detected. Continue anyway? (y/N)"
- In non-interactive mode: logs warnings and continues
- Does NOT affect exit codes (these are upstream data issues, not tool failures)

**7. Doctor Command** (`cli/doctor.py`)
- Invoked standalone: `cpapacket doctor`
- Checks environment and auth health (complementary to `check` which validates data quality):
  1. **Filesystem**: `--out` directory writable (if provided), `~/.config/cpapacket/` writable
  2. **QBO token**: token exists in keyring, not expired, refresh succeeds (dry-run)
  3. **Gusto token**: same as QBO (reports "not configured" rather than "failed" if absent)
  4. **QBO connectivity**: fetch Company Info (lightweight API call, confirms auth + network)
  5. **Gusto connectivity**: fetch company identity (lightweight; skipped if token absent)
  6. **Python environment**: confirm version >= 3.11, required packages present
- Output: green checkmarks or red X for each check, with actionable guidance on failures

**8. Per-Deliverable Metadata** (`core/metadata.py`)
- Each deliverable writes `_meta/{deliverable}_metadata.json` after successful generation
- Metadata schema (Pydantic model):
  ```python
  class DeliverableMetadata(BaseModel):
      deliverable: str                   # e.g., "pnl", "balance_sheet"
      generated_at: datetime
      inputs: dict[str, Any]             # deliverable-specific input parameters
      input_fingerprint: str             # "sha256:{hex}" of canonicalized inputs
      schema_versions: dict[str, str]    # e.g., {"csv": "1.0"}
      artifacts: list[str]               # relative paths from packet root
      warnings: list[str]
      data_sources: dict[str, str]       # {"qbo_pnl": "api" | "cache"} — traceability
  ```
- `input_fingerprint` = `sha256(json.dumps(sorted(inputs.items()), sort_keys=True))`
- Same inputs → same hash → incremental mode can safely skip

**Validation Rules Table:**

| Folder | Required? | Rationale | Files Expected | Pattern |
|--------|-----------|-----------|----------------|---------|
| 01_Year-End_Profit_and_Loss | Yes | Core financial statement | 1 PDF + 1 CSV | `Profit_and_Loss_*_YYYY.pdf/csv` |
| 02_Year-End_Balance_Sheet (current) | Yes | Core financial statement | 1 PDF + 1 CSV | `Balance_Sheet_YYYY-12-31.*` |
| 02_Year-End_Balance_Sheet (prior) | No | Comparison only; new companies won't have one | 1 PDF + 1 CSV | `Balance_Sheet_(YYYY-1)-12-31.*` |
| 03_Full-Year_General_Ledger | Yes | CPA needs for detailed review | 1 CSV | `General_Ledger_YYYY.csv` |
| 04_Annual_Payroll_Summary | Yes (skippable if Gusto absent) | Required for 1120-S payroll deductions | Company PDF+CSV + employee folders | See naming patterns |
| 05_Officer_W2_Equivalent | No (future) | — | — | — |
| 06_Shareholder_Distributions | Yes | Required for 1120-S Schedule M-2 / basis tracking | PDF + CSV | `distributions_summary_YYYY.*` |
| 07_Contractor_1099_Summary | Yes | Required for 1099 compliance review | PDF + CSV | `contractor_summary_YYYY.*` |
| 08_Estimated_Tax_Payments | No | Local tracking aid; not a CPA deliverable | tracker + deadlines CSV | `estimated_tax_tracker_YYYY.*` |
| 09_Retained_Earnings_Rollforward | Yes | Required for M-2 reconciliation | PDF + CSV | `Retained_Earnings_Rollforward_YYYY.*` |
| 10_Payroll_Reconciliation | Yes (skippable if Gusto absent) | CPA verifies payroll sync integrity | PDF + CSV | `payroll_reconciliation_YYYY.*` |
| _meta | Always exists | Internal | validation_report.txt | — |

---

#### E2 – Profit & Loss

**Command:** `cpapacket pnl --start YYYY-MM-DD --end YYYY-MM-DD --method accrual|cash --out <base_dir> [--incremental] [--force]`

The standalone `pnl` command requires explicit `--start`, `--end`, and `--method` (no defaults) since it supports arbitrary date ranges. When invoked via `build`, the build command derives these from `--year` and `--method` (default `accrual`).

**Flow:**
1. Construct `RunContext` from CLI flags
2. Request Company Info + P&L report through `SessionDataStore` (fetches or returns cached)
3. Save raw JSON → `*_raw.json` (via `atomic_write`)
4. Normalize into `list[NormalizedRow]` (Pydantic validation at parse time)
5. Write CSV via `atomic_write` (section, level, row_type, label, amount, path)
6. Write PDF via `atomic_write` (CPA-grade layout with header, indented body, footer)
7. Write `_meta/pnl_metadata.json` with input fingerprint, schema version, artifact list
8. Handle file-exists per `RunContext.on_conflict`

**Edge Cases:**
- QBO returns empty report (no transactions in period) → generate files with $0 totals, add note
- Report contains `0.00` amounts → include them (they're meaningful for CPA)
- Very long account names → PDF must handle wrapping or truncation
- Date range crosses tax years → allowed (quarterly P&L use case)

**Normalization Logic:**
QBO P&L response is a nested `Rows` → `Row` → `ColData` structure. The normalizer must:
- Recursively walk rows
- Track current section (Income, COGS, Expenses, Other Income, Other Expense)
- Track nesting level for indentation
- Classify each row as `header`, `account`, `subtotal`, or `total`
- Build a `path` string like `Income > Sales > Product Sales`
- Handle `Summary` rows (subtotals) vs `Data` rows (accounts)

---

#### E3 – Balance Sheets (Current + Prior Year)

**Commands:**
- `cpapacket balance-sheet --year YYYY --out <base_dir> [--incremental] [--force]` → as of 12/31/YYYY
- `cpapacket prior-balance-sheet --year YYYY --out <base_dir> [--incremental] [--force]` → as of 12/31/(YYYY-1)

**Both output to:** `02_Year-End_Balance_Sheet/`

**Key Difference from P&L:**
- Always accrual basis (no `--method` flag)
- Uses the same `NormalizedRow` model; normalizer validates sections are in `{"Assets", "Liabilities", "Equity"}`
- Balance equation validation: Assets = Liabilities + Equity (tolerance: `BALANCE_EQUATION_TOLERANCE`)
- Mismatch → warning in console + `validation_report.txt` + deliverable metadata

**Edge Cases:**
- Prior year has no data (new company) → generate with $0, note in validation
- Balance equation off by rounding → tolerate within `BALANCE_EQUATION_TOLERANCE`, flag anything larger
- Prior-year balance sheet is optional for packet validation but recommended

**Metadata:** `_meta/balance_sheet_YYYY_metadata.json` and `_meta/prior_balance_sheet_YYYY_metadata.json`

---

#### E4 – General Ledger

**Command:** `cpapacket general-ledger --year YYYY --out <base_dir> [--incremental] [--force]`

**Critical Implementation: Monthly Slicing with Resumable Caching**
```
for month in 1..12:
    start = YYYY-MM-01
    end = YYYY-MM-{last_day}
    response = data_store.get_or_fetch("GeneralLedgerDetail", start, end)
    slices.append(normalize(response))
```

Each monthly slice is cached individually at `_meta/cache/qbo/general_ledger/YYYY/MM_raw.json`. A partial failure (e.g., timeout on month 7) can resume from the last successful month without re-fetching months 1–6. Slice pulls use bounded concurrency (`GUSTO_MAX_CONCURRENCY`) via the shared limiter.

**Merge & Write Strategy:**
Monthly slices are merged and written to CSV in streaming fashion via `atomic_write`. The writer processes one month at a time: for each normalized row, it checks `txn_id` against `seen_ids: set[str]` for deduplication, then writes the row immediately to the temp file. This avoids holding the full year's ledger in memory. The `seen_ids` set (storing only transaction ID strings) is the only persistent memory allocation.

**Deduplication:** By `txn_id` (always present in QBO GeneralLedgerDetail responses). A transaction straddling a month boundary will appear in both slices — the merge keeps the first occurrence by chronological month. If `txn_id` is unexpectedly absent, fall back to a composite hash of `date + transaction_type + document_number + account_name + debit + credit + payee + memo`.

**Output:** CSV + raw JSON only (no PDF — ledger is too large for useful PDF output)

**Validation:**
- Row count > 0
- Optional: sum of all `signed_amount` approximately equals 0 (balanced ledger check)
- Signed amount convention: `Debit - Credit` (positive debits)

**Edge Cases:**
- Month with zero transactions → empty slice, skip in merge
- QBO API timeout on large month → retry via shared retry policy
- Transaction with missing fields → include with empty strings, don't skip
- Unicode in memos/payees → preserve as-is in CSV (UTF-8 encoding)

**Metadata:** `_meta/general_ledger_YYYY_metadata.json` with cached months list and input fingerprint.

---

#### E5 – Annual Payroll Summary (Gusto)

**Command:** `cpapacket payroll-summary --year YYYY --out <base_dir> [--incremental] [--force]`

**Gusto dependency:** This deliverable requires Gusto auth. If Gusto is not connected, `build` auto-skips this deliverable and marks it as "Skipped (Gusto not connected)" in validation. The standalone command exits with a clear error and instructions to run `cpapacket auth gusto login`.

**Directory Structure:**
```
04_Annual_Payroll_Summary/
  00_Company_Summary/
    Annual_Payroll_Summary_YYYY.pdf
    Annual_Payroll_Summary_YYYY.csv
    Annual_Payroll_Summary_YYYY_raw.json
  Employees/
    {Last}_{First}_emp_{id}/
      Payroll_Breakdown_{EmployeeName}_YYYY.pdf
      Payroll_Breakdown_{EmployeeName}_YYYY.csv
      Payroll_Breakdown_{EmployeeName}_YYYY_raw.json
```

**Data Flow:**
1. Fetch all payroll runs for YYYY from Gusto (via `SessionDataStore`)
2. Aggregate by employee, then by category (wages, EE taxes, ER taxes, retirement)
3. Generate company-level summary (all employees combined)
4. Generate per-employee breakdowns
5. **Critical: 401(k) employee deferrals vs employer contributions must be separate line items**

**Company Summary Aggregation:**
```
Total Payroll Cost = Gross Wages + Employer Payroll Taxes + Employer 401(k) Contributions
```
(Employee withholdings are NOT included — they're liabilities, not expenses.)

**Soft Flags (non-blocking):**
- Officer wages = $0
- Retirement contributions present but gross wages unusually low
- Negative totals detected

**Edge Cases:**
- No payroll runs found → still generate output with $0 and a note
- Employee name contains special characters → sanitize for folder name, preserve in display
- Mid-year hire/termination → still include with prorated amounts

**Metadata:** `_meta/payroll_summary_YYYY_metadata.json`

---

#### E6 – Contractor Summary + 1099 Flags

**Command:** `cpapacket contractor-summary --year YYYY --out <base_dir> [--incremental] [--force]`

**Account Detection (Interactive Per-Run):**
1. Scan QBO accounts: type = Expense or COGS, name contains "contract"/"contractor"/"subcontract"
2. Present detected accounts → user confirms (Y/n) or manually selects
3. No persistent config; in non-interactive mode, all detected accounts are auto-confirmed

**Payment Method Classification:**
- `card_processor_total` = payments from accounts with subtype "Credit Card" or name containing "Stripe", "PayPal", "Credit Card"
- `non_card_total` = `total_paid` - `card_processor_total`
- `requires_1099_review` = (`non_card_total` >= `CONTRACTOR_1099_THRESHOLD`)

**Reconciliation Safeguard:**
- Sum of contractor report totals vs sum of confirmed contractor account balances
- Mismatch → warning + metadata flag (doesn't block)

**Edge Cases:**
- No contractor accounts detected → generate empty report with note
- Vendor with only card payments → included in report, `non_card_total` = 0, not flagged
- Vendor with $599.99 non-card → not flagged (threshold is >= `CONTRACTOR_1099_THRESHOLD`)
- Journal entries to contractor accounts → included if vendor-linked

**Metadata:** `_meta/contractor_summary_YYYY_metadata.json` — includes selected accounts for traceability.

---

#### E7 – Estimated Tax Tracker + Deadlines

**Commands:**
- `cpapacket tax init --year YYYY` → interactive setup of payment schedule
- `cpapacket tax update --year YYYY` → edit existing entries
- `cpapacket tax mark-paid --jurisdiction DE|NY --due MM/DD/YY` → quick status update
- `cpapacket tax status --year YYYY` → dashboard view

**Storage:**
- Persistent source of truth: `~/.config/cpapacket/tax_tracker_{year}.json` and `~/.config/cpapacket/tax_deadlines_{year}.json` (via `platformdirs.user_config_dir()`)
- On `build` or when `--out` is provided: data is copied into both `_meta/` and the `08_Estimated_Tax_Payments/` deliverable folder
- This decouples the tracker from the packet lifecycle — `tax init`, `tax update`, `tax mark-paid`, and `tax status` all work without QBO auth or an `--out` directory

**Default Deadlines (auto-generated on first `status`):**

| Jurisdiction | Date | Category |
|-------------|------|----------|
| Federal | 03/15/YYYY | 1120-S Filing |
| Federal | 04/15/YYYY | Individual Filing |
| Federal | 04/15/YYYY | Q1 Estimated |
| Federal | 06/16/YYYY | Q2 Estimated |
| Federal | 09/15/YYYY | Q3 Estimated |
| Federal | 01/15/YYYY+1 | Q4 Estimated |
| NY | Matching quarterly dates | NY Estimated |
| DE | 03/01/YYYY | Franchise Tax |

**Deadline Awareness Rules:**
- Past due: `today > due_date AND NOT completed` → flag as "PAST DUE"
- Upcoming: `due_date` within 30 days → flag as "UPCOMING"
- No exit code changes; pure informational

**Packet Output:** `08_Estimated_Tax_Payments/` with tracker CSV/PDF + deadlines CSV/PDF.

**Metadata:** `_meta/tax_tracker_YYYY_metadata.json`

---

#### E8 – Payroll Reconciliation (Gusto vs QBO)

**Command:** `cpapacket payroll-recon --year YYYY --out <base_dir> [--incremental] [--force]`

**Gusto dependency:** Same as E5 — auto-skipped if Gusto not connected.

**Logic:**
```
gusto_total = gross_wages + employer_payroll_taxes + employer_401k_contributions
qbo_total = sum(Expense/COGS accounts matching "Payroll"|"Salary"|"Wages"|"Employer Tax"|"401(k)")
variance = qbo_total - gusto_total
status = "RECONCILED" if abs(variance) <= PAYROLL_RECON_TOLERANCE else "MISMATCH"
```

**Output:** Simple one-table PDF and CSV in `10_Payroll_Reconciliation/`.

**Edge Cases:**
- QBO has manual payroll journals → inflates QBO total → mismatch expected
- Gusto sync lag → minor variance
- No matching QBO accounts → variance = full Gusto amount → obvious mismatch warning

**Metadata:** `_meta/payroll_reconciliation_YYYY_metadata.json`

---

#### E9 – Retained Earnings Rollforward

**Command:** `cpapacket retained-earnings --year YYYY --out <base_dir> [--incremental] [--force]`

**Core Formula:**
```
Expected Ending RE = Beginning RE + Net Income - Distributions
Difference = Expected Ending RE - Actual Ending RE
Status = "Balanced" if abs(Difference) <= RETAINED_EARNINGS_TOLERANCE else "Mismatch"
```

**Data Sources (cross-deliverable):**
- Beginning RE → Prior Year Balance Sheet (12/31/YYYY-1) Equity section
- Net Income → Current Year P&L bottom line
- Distributions → Equity accounts matching "distribution"/"draw" in GL
- Actual Ending RE → Current Year Balance Sheet (12/31/YYYY) Equity section

**Data access:** All data is requested through the `SessionDataStore`. The RE rollforward command never checks for the existence of other deliverables' output files — it always works through the data layer.

**Structural Checks (always run):**
1. Distributions > Net Income → flag basis risk
2. Negative ending RE → flag
3. Direct postings to Retained Earnings (GL scan) → flag

**Miscoded Distribution Detection:**
Delegates to the shared `MiscodeDetector` engine (see E10 for the unified rule table). The RE rollforward PDF reports the count of flagged items and references `06_Shareholder_Distributions/likely_miscoded_distributions_YYYY.csv`. Whichever of E9 or E10 runs first writes this CSV; the second reuses the existing result.

**Metadata:** `_meta/retained_earnings_YYYY_metadata.json` — records which data came from cache vs. fresh API.

---

#### E10 – Shareholder Distributions Summary

**Command:** `cpapacket distributions --year YYYY --out <base_dir> [--incremental] [--force]`

**Additional flags:** `--owner-keywords "Alex,Smith"` (comma-separated; skips interactive prompt)

**Primary Detection:** Equity accounts matching "distribution"/"draw"/"shareholder"

**Unified Miscoding Detection** (via `reconciliation/miscode_detector.py`):

Owner keyword(s) are collected once per session (prompted interactively, or passed via `--owner-keywords`). The detector scans the General Ledger and applies a single consolidated rule set:

| Rule | Signal | Points |
|------|--------|--------|
| R1: Owner/shareholder payee + expense account | Owner paid from non-equity | +3 |
| R2: Memo keyword match ("distribution", "owner draw", "reimbursement", "personal", "transfer") on expense account | Text suggests equity treatment | +2 |
| R3: Transfer from business bank → non-equity, amount > `MISCODE_HIGH_AMOUNT_THRESHOLD` | Structural pattern of owner draw | +2 |
| R4: Round-number amount (multiple of `MISCODE_ROUND_NUMBER_DIVISOR`) + owner payee | Heuristic for informal draws | +1 |
| R5: Amount > `MISCODE_HIGH_AMOUNT_THRESHOLD` (any flagged transaction) | Size signal | +1 |

Confidence: High (`>= MISCODE_CONFIDENCE_HIGH`), Medium (`>= MISCODE_CONFIDENCE_MEDIUM`), Low (`>= MISCODE_CONFIDENCE_LOW`)

Both E9 and E10 call `MiscodeDetector.scan(gl_rows, owner_keywords)` and receive the same `list[MiscodedDistributionCandidate]`. E10 writes the detailed CSV to `06_Shareholder_Distributions/likely_miscoded_distributions_YYYY.csv`; E9 references the summary count. One CSV, one location, one source of truth.

**Cross-Reference:** `distribution_total` from this deliverable feeds into E9 (Retained Earnings Rollforward). Any discrepancy → metadata warning.

**Metadata:** `_meta/distributions_YYYY_metadata.json`

---

#### E11 – Authentication & Token Management

**QBO OAuth 2.0:**
- Authorization Code flow with PKCE
- Tokens stored via `keyring` (macOS Keychain / Linux Secret Service)
- Refresh token rotation handled automatically
- Token expiry check before each request
- Scopes: `com.intuit.quickbooks.accounting`

**Gusto OAuth 2.0:**
- Similar Authorization Code flow
- Separate token storage
- Scopes: `payrolls:read employees:read companies:read`

**Auth Commands:**
- `cpapacket auth qbo login` → launches browser for QBO OAuth consent
- `cpapacket auth qbo status` → shows token status (valid/expired/missing)
- `cpapacket auth qbo logout` → clears stored tokens
- `cpapacket auth gusto login|status|logout` → same for Gusto

**Token Refresh Strategy:**
- Check expiry before each request (handled by `core/retry.py`)
- If expired, attempt refresh
- If refresh fails, prompt user to re-authenticate
- All auth errors → exit code 1

---

## 4. Data Models / APIs / Schemas

### 4.1 QBO API Endpoints Used

| Endpoint | Used By | Parameters |
|----------|---------|------------|
| `GET /v3/company/{id}/companyinfo/{id}` | All (company name), doctor | — |
| `GET /v3/company/{id}/reports/ProfitAndLoss` | E2 | `start_date`, `end_date`, `accounting_method` |
| `GET /v3/company/{id}/reports/BalanceSheet` | E3 | `as_of_date` |
| `GET /v3/company/{id}/reports/GeneralLedgerDetail` | E4, E9, E10 | `start_date`, `end_date` |
| `GET /v3/company/{id}/query` (Account) | E6, E8, Health Check | SQL-like query for account list |
| `GET /v3/company/{id}/query` (Invoice/Bill) | Health Check | Open items query |

### 4.2 Gusto API Endpoints Used

| Endpoint | Used By | Parameters |
|----------|---------|------------|
| `GET /v1/companies/{id}/payrolls` | E5 | `start_date`, `end_date`, `processed=true` |
| `GET /v1/companies/{id}/employees` | E5 | — |
| `GET /v1/companies/{id}` | Doctor | — |

### 4.3 Domain Models (Pydantic)

```python
# --- Deliverable Protocol ---

class DeliverableResult(BaseModel):
    """Returned by every deliverable's generate() method."""
    deliverable_key: str
    success: bool
    artifacts: list[str]           # relative paths of files written
    warnings: list[str]
    error: str | None = None       # populated on failure

@runtime_checkable
class Deliverable(Protocol):
    key: str                                           # e.g., "pnl"
    folder: str                                        # from DELIVERABLE_FOLDERS
    required: bool                                     # for validation classification
    dependencies: list[str]                            # keys of deliverables that must run first
    requires_gusto: bool                               # if True, auto-skipped when Gusto absent
    
    def gather_prompts(self, ctx: RunContext) -> dict[str, Any]:
        """Collect any interactive inputs needed. Called during upfront prompt phase."""
        ...
    
    def is_current(self, ctx: RunContext) -> bool:
        """Check if metadata fingerprint matches and outputs exist. For --incremental."""
        ...
    
    def generate(self, ctx: RunContext, store: SessionDataStore, 
                 prompts: dict[str, Any]) -> DeliverableResult:
        """Fetch data, normalize, write outputs, write metadata. All writes atomic."""
        ...

# --- Financial Models ---

class NormalizedRow(BaseModel):
    """Shared by P&L and Balance Sheet. Balance sheet normalizer validates
    that section is in {"Assets", "Liabilities", "Equity"} at parse time."""
    model_config = ConfigDict(frozen=True)

    section: str
    label: str
    amount: Decimal = Field(decimal_places=2)
    row_type: Literal["header", "account", "subtotal", "total"]
    level: int = Field(ge=0)
    path: str = ""

class GeneralLedgerRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    txn_id: str               # QBO Transaction ID — primary key for dedup and cross-reference
    date: date
    transaction_type: str
    document_number: str = ""
    account_name: str
    account_type: str
    payee: str = ""
    memo: str = ""
    debit: Decimal = Field(ge=0, decimal_places=2)
    credit: Decimal = Field(ge=0, decimal_places=2)
    signed_amount: Decimal     # debit - credit

class ContractorRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    vendor_id: str
    display_name: str
    tax_id_on_file: bool
    total_paid: Decimal
    card_processor_total: Decimal
    non_card_total: Decimal
    requires_1099_review: bool
    flags: list[str]

class EstimatedTaxPayment(BaseModel):
    jurisdiction: Literal["DE", "NY", "Federal"]
    due_date: date
    amount: Decimal
    status: Literal["paid", "not_paid"]
    paid_date: date | None = None
    last_updated: datetime

class TaxDeadline(BaseModel):
    jurisdiction: str
    name: str
    due_date: date
    category: Literal["estimated_tax", "filing", "extension"]
    completed: bool = False

class RetainedEarningsRollforward(BaseModel):
    model_config = ConfigDict(frozen=True)

    beginning_re: Decimal
    net_income: Decimal
    distributions: Decimal
    expected_ending_re: Decimal
    actual_ending_re: Decimal
    difference: Decimal
    status: Literal["Balanced", "Mismatch"]
    flags: list[str]

class MiscodedDistributionCandidate(BaseModel):
    model_config = ConfigDict(frozen=True)

    txn_id: str           # QBO transaction ID for CPA to locate in QBO
    date: date
    transaction_type: str
    payee: str
    memo: str
    account: str
    amount: Decimal
    reason_codes: list[str]
    confidence: Literal["High", "Medium", "Low"]
    score: int = Field(ge=0)

# --- Infrastructure Models ---

class RunContext(BaseModel):
    """Immutable context object constructed at CLI entry, passed to all operations."""
    model_config = ConfigDict(frozen=True)

    year: int
    year_source: Literal["explicit", "inferred", "default"]  # how --year was resolved
    out_dir: Path
    method: Literal["accrual", "cash"] = "accrual"            # P&L accounting method (build only)
    non_interactive: bool
    on_conflict: Literal["prompt", "overwrite", "copy", "abort"]
    incremental: bool = False
    force: bool = False
    verbose: bool = False
    quiet: bool = False
    plain: bool = False
    skip: list[str] = Field(default_factory=list)
    owner_keywords: list[str] = Field(default_factory=list)
    gusto_available: bool = True                               # set during pre-flight auth check

class DeliverableMetadata(BaseModel):
    deliverable: str
    generated_at: datetime
    inputs: dict[str, Any]
    input_fingerprint: str
    schema_versions: dict[str, str]
    artifacts: list[str]
    warnings: list[str] = Field(default_factory=list)
    data_sources: dict[str, str] = Field(default_factory=dict)
```

### 4.4 Financial Precision Rule

**All monetary values MUST use `decimal.Decimal`, never `float`.**
- QBO returns string amounts → parse directly to Decimal (Pydantic handles this via the `Decimal` type annotation)
- CSV output: no currency symbols, plain numeric with 2 decimal places
- PDF output: formatted with `$` and commas
- Comparisons use `Decimal` with explicit rounding
- All tolerance thresholds are named constants in `constants.py` (see Appendix G)

### 4.5 Output Schema Versioning

Each deliverable declares schema versions in `constants.py`:

```python
SCHEMA_VERSIONS = {
    "pnl":              {"csv": "1.0"},
    "balance_sheet":    {"csv": "1.0"},
    "general_ledger":   {"csv": "1.0"},
    "payroll_summary":  {"csv": "1.0"},
    "contractor":       {"csv": "1.0"},
    "estimated_tax":    {"csv": "1.0"},
    "payroll_recon":    {"csv": "1.0"},
    "retained_earnings":{"csv": "1.0"},
    "distributions":    {"csv": "1.0"},
}
```

**Rules:**
- A breaking change (column removal/rename or semantic change) requires a major bump (e.g., 2.0).
- Additive columns can be a minor bump (e.g., 1.1), but keep column order stable.
- Schema version is recorded in each deliverable's metadata JSON.
- CI test: assert that each deliverable's actual CSV column order matches the declared schema version.

### 4.6 Global CSV Rules

- UTF-8 encoding
- Newline: `\n`
- Quote minimally
- Numeric columns: plain numbers (no `$`, no commas)
- Dates: ISO `YYYY-MM-DD`

---

## 5. Non-Functional Requirements

### 5.1 Performance

| Metric | Target | Strategy |
|--------|--------|----------|
| Full packet build time | < 3 minutes | SessionDataStore eliminates duplicate API calls; steps 2–5 concurrent; bounded concurrency for GL slices; streaming CSV |
| Single deliverable | < 60 seconds | Efficient normalization; data cache avoids re-fetches |
| Incremental rebuild | < 30 seconds | Fingerprint-based skip for unchanged deliverables |
| PDF generation | < 5 seconds per file | reportlab is fast for structured reports |
| Zip creation | < 10 seconds | Standard zipfile library |

**API Rate Limiting:**
- QBO: 500 requests/minute — shared limiter caps concurrent requests at `QBO_MAX_CONCURRENCY`
- Gusto: 60 requests/minute — shared limiter caps concurrent requests at `GUSTO_MAX_CONCURRENCY`
- Retry policy via `core/retry.py` with exponential backoff + jitter + `Retry-After` honoring
- Explicit timeouts: connect 10s, read 90s

### 5.2 Security

| Concern | Mitigation |
|---------|------------|
| OAuth tokens at rest | `keyring` for OS-level secure storage; fallback to encrypted local file in `~/.config/cpapacket/` |
| Client secrets | Environment variables or `.env` (gitignored) |
| API responses | Raw JSON stored locally (user's machine only); no network transmission |
| Financial data | Never logged to console beyond summary totals; no telemetry |
| Token in memory | Cleared after use; not stored in global state |
| Error messages | Include endpoint/report name + parameters but never secrets/tokens |

### 5.3 Reliability & Error Handling

**Error Hierarchy:**
1. **Auth Failure** → exit code 1, clear error message with re-auth instructions
2. **API Error (429)** → retry up to `RETRY_MAX_429` times with backoff + Retry-After honoring
3. **API Error (5xx)** → retry `RETRY_MAX_5XX` times with backoff; exit code 1 on persistent failure
4. **API Error (other 4xx)** → fail fast with descriptive error; exit code 1
5. **Filesystem Error** → exit code 1 (permissions, disk full)
6. **Validation Warning** → exit code 2 (missing items)
7. **Clean Run** → exit code 0

**All errors must:**
- Print human-readable message to stderr
- Include actionable guidance (e.g., "Run `cpapacket auth qbo login` to re-authenticate")
- Never expose raw stack traces to console unless `--verbose` flag is set
- Always log full stack traces to `_meta/cpapacket.log` regardless of verbosity
- Log API request/response metadata (endpoint, status code, duration) at DEBUG level

### 5.4 Accessibility & Usability

- CLI output uses `rich` for structured visual hierarchy:
  - `rich.progress.Progress` for multi-step build operations (shows deliverable name + ETA)
  - `rich.table.Table` for validation summaries, tax status dashboard, reconciliation results
  - `rich.panel.Panel` for warnings and error blocks
  - `rich.tree.Tree` for packet directory listing after build
- Warnings rendered as yellow panels; errors as red panels; success as green checkmarks
- Respects `NO_COLOR` environment variable (rich handles this natively)
- All prompts have sensible defaults
- `--help` on every command with usage examples
- `--plain` flag disables rich formatting for piping/scripting
- `--non-interactive` flag: auto-detected via `isatty()` if not explicit
- `--version` flag on root command
- PDFs use readable fonts, consistent headings, and page numbering

### 5.5 Deployment & Distribution

- Publish to PyPI; recommend `pipx install cpapacket`
- Entry point: `cpapacket` CLI command
- `cpapacket --version` reports current version
- No containerization needed (local tool)
- Python 3.11+ required (for `tomllib`, modern type syntax)
- Dependencies pinned in `pyproject.toml` with version ranges
- CI tests on macOS and Linux (Windows: best-effort)

### 5.6 Extensibility Points

| Extension | Design Hook |
|-----------|-------------|
| New deliverable | Implement `Deliverable` protocol in `deliverables/`; register in `DELIVERABLE_REGISTRY`; add thin CLI wrapper; declare schema version |
| New data source | Add client in `clients/`; register with `SessionDataStore` and limiter; implement same fetch/normalize interface |
| Alternative output format | Add writer in `writers/` (e.g., Excel via openpyxl) |
| Multi-company | Parameterize company ID in `RunContext`; currently assumed single |
| Persistent config | Extend `~/.config/cpapacket/` with a `config.toml` for contractor accounts, owner identity |

---

## 6. Testing Strategy

### 6.1 Test Pyramid

| Level | Count (Target) | Tools | Coverage |
|-------|----------------|-------|----------|
| Unit | ~200 | pytest | Models, normalization, formatting, calculations, fingerprinting, deliverable logic |
| Integration | ~50 | pytest + respx | API client → data store → normalization → writer pipelines |
| CLI | ~30 | click.testing.CliRunner | Command invocation, prompts, exit codes, RunContext |
| Snapshot | ~20 | pytest-snapshot | PDF content verification (text extraction), CSV exact match |

### 6.2 Test Categories

**Unit Tests:**
- Pydantic model validation (valid construction, rejection of invalid data)
- `NormalizedRow` construction from various QBO response shapes
- Balance sheet section validation (only Assets/Liabilities/Equity accepted)
- Balance equation validation using `BALANCE_EQUATION_TOLERANCE`
- Signed amount calculation (debit - credit)
- Contractor 1099 threshold logic using `CONTRACTOR_1099_THRESHOLD`
- Card processor detection (Stripe, PayPal, Credit Card patterns)
- Unified miscoding detection scoring (each rule individually, combined scores, confidence levels using named constants)
- Date utilities (last day of month, fiscal year derivation)
- File naming (company name sanitization, timestamp suffixes, year always present)
- Currency formatting (Decimal → display string)
- Input fingerprint stability (same inputs → same hash; different inputs → different hash)
- Schema version declarations exist for each deliverable
- Retry/backoff logic (pure functions) and limiter concurrency bounds
- SessionDataStore cache key generation and lookup
- General Ledger deduplication by `txn_id`
- Atomic write: crash simulation leaves no partial files
- RunContext construction: on_conflict defaults, year inference, method defaults
- Deliverable Protocol: implementations satisfy protocol contract
- `--year` inference from directory name
- `--year` default derivation from current date

**Integration Tests (mocked HTTP via respx):**
- Full P&L pipeline: mock QBO response → data store → normalized rows → CSV content check → metadata written
- General Ledger monthly merge: 12 mock responses → data store cache → streaming CSV → dedup verified
- Payroll aggregation: mock Gusto payroll runs → company summary + per-employee breakdowns
- Retained Earnings cross-deliverable: mock all sources via data store → rollforward calculation
- Data store cache hit: second request for same data returns cached without HTTP call
- Data store `--force` flag: forces fresh fetch even with warm cache
- Incremental skip: metadata fingerprint matches → deliverable not regenerated
- Bounded concurrency: concurrent GL slice pulls don't exceed limiter bounds
- Rate limit simulation: 429 with Retry-After honored correctly
- Gusto-absent build: Gusto-dependent deliverables skipped, non-Gusto deliverables succeed, exit code reflects skips

**CLI Tests:**
- Each command with valid args → exit code 0
- Missing required args → error message
- File-exists prompt: Overwrite, Copy, Abort
- Non-interactive mode without `--on-conflict` → default Abort
- `--on-conflict overwrite` → overwrite without prompting
- `build` with complete packet → exit code 0
- `build` with missing deliverables → exit code 2
- `build` with partial failure (one deliverable errors) → exit code 1 + zip still created
- `build --validate-only` → no API calls made
- `build --incremental` → unchanged deliverables skipped
- `build --skip payroll` → payroll deliverables skipped, flagged in validation
- `build --method cash` → P&L generated with cash basis
- Auth failure → exit code 1
- `--owner-keywords` bypasses interactive prompt for distributions
- `doctor` reports all checks with pass/fail; Gusto shows "not configured" rather than "failed"
- `--version` prints version string
- `--year` omitted → inferred or defaulted correctly

**Snapshot Tests:**
- CSV output for each deliverable against golden files (column order matches schema version; year in filename)
- PDF text content (extracted via `pdfplumber`) against expected strings
- Validation report format (including "Skipped" status)
- Metadata JSON structure

### 6.3 Test Fixtures

```
tests/fixtures/
  qbo/
    company_info.json
    profit_and_loss_annual.json
    profit_and_loss_empty.json
    balance_sheet_2025.json
    balance_sheet_2024.json
    general_ledger_jan.json ... general_ledger_dec.json
    accounts_list.json
    uncategorized_transactions.json
  gusto/
    payroll_runs_2025.json
    employees.json
    payroll_empty.json
```

### 6.4 CI Pipeline

```yaml
# GitHub Actions
- lint: ruff check + ruff format --check
- typecheck: mypy --strict
- test: pytest --cov=cpapacket --cov-fail-under=85
- schema: pytest tests/test_schema_versions.py  # CSV column order matches declared versions
- build: pip install . && cpapacket --version && cpapacket --help
- platforms: ubuntu-latest, macos-latest
```

---

## 7. Risks, Mitigations & Assumptions

### 7.1 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| QBO report API returns unexpected structure | Medium | High | Pydantic validation catches at parse time; raw JSON preserved for debugging; version-pin API calls |
| QBO General Ledger too large even with monthly slicing | Low | Medium | Streaming CSV writer + bounded concurrency; implement pagination within slices if needed |
| Gusto API deprecation or breaking change | Low | High | Pin API version; abstract behind client interface; Gusto is optional so tool still works |
| OAuth token refresh race condition | Low | Medium | Serialize token refresh; file-level locking on token store |
| PDF layout breaks with very long account names | Medium | Low | Truncate with ellipsis at 60 chars; preserve full name in CSV |
| Company name contains filesystem-unsafe characters | Medium | Low | Sanitization function strips unsafe chars, collapses underscores |
| QBO sandbox vs production data discrepancies | High | Medium | Test against production-like fixtures; validate with real data early |
| Rounding errors in financial calculations | Medium | High | `Decimal` everywhere with Pydantic enforcement; named tolerance constants; CI tests against thresholds |
| Tax deadline dates change (holidays, weekends) | Medium | Low | Default deadlines are starting points; user can edit; document that dates are approximate |
| Data cache serves stale data across sessions | Medium | Medium | Cache entries stamped with `fetched_at`; `--force` re-fetches everything; cache auto-expires after `CACHE_TTL_HOURS` |
| Dirty QBO books produce technically correct but useless output | High | Medium | Health pre-check catches top 5 data quality issues before generation begins |
| Incremental mode serves stale outputs | Medium | Medium | Fingerprint-based; `--incremental` is opt-in; `--force` is escape hatch; default mode always regenerates |
| Schema drift breaks downstream consumers | Low | High | Schema versions declared per deliverable; CI test enforces column order matches version |
| Partial file corruption from crash | Low | High | Atomic writes (temp → rename) eliminate this class of failure |
| API rate-limit storms during concurrent build | Medium | Medium | Shared limiter + shared retry policy with Retry-After honoring |
| User doesn't use Gusto | Medium | Low | Graceful degradation: payroll deliverables auto-skipped with clear messaging |

### 7.2 Assumptions

| # | Assumption | Consequence if Wrong |
|---|-----------|---------------------|
| A1 | Single QBO company connected | Multi-company would require company selector in RunContext |
| A2 | Gusto used for payroll (optional) | If Gusto absent, payroll deliverables skipped; packet still produced |
| A3 | S-Corp entity type (single shareholder) | Distribution logic and RE rollforward assume single-owner |
| A4 | Gusto handles only employees (no contractors) | Contractor data comes from QBO only |
| A5 | QBO payroll sync from Gusto is complete | Payroll reconciliation depends on this |
| A6 | Delaware incorporation + New York residency | Tax deadlines are hardcoded for these jurisdictions |
| A7 | Calendar year = fiscal year | All date logic assumes Jan 1 – Dec 31 |
| A8 | QBO report API preserves account hierarchy | Normalization depends on nested row structure |
| A9 | Internet connectivity available during data pull | Data cache provides partial offline capability for previously fetched data |
| A10 | QBO GeneralLedgerDetail always includes TxnId | Deduplication depends on this; fallback to composite hash if missing |

### 7.3 Trade-offs

| Decision | Pro | Con |
|----------|-----|-----|
| Deliverable Protocol + registry | Engineers add deliverables without reading codebase; build is data-driven | Slightly more abstraction upfront |
| Minimal persistent config | Simplicity; no config versioning | User re-confirms contractor accounts each run |
| reportlab for PDF (not LaTeX) | No system dependency; pure Python | Less typographic control |
| Monthly GL slicing (not chunked pagination) | Predictable API behavior; 12 bounded calls | Slightly more complex merge logic |
| Warnings never block zip | CPA always gets a packet | Incomplete packet could be sent accidentally |
| Conservative miscoding detection | Low false positive rate | May miss some miscoded transactions |
| Limited parallelism in `build` | Steps 2–5 concurrent; remaining sequential | Moderate speedup (~40%); full async would be overkill |
| `rich` for CLI output | Professional progress bars, tables, panels | Additional dependency |
| Non-interactive default: Abort | Prevents silent data destruction in headless scripts | Automation must explicitly pass `--on-conflict overwrite` |
| Pydantic over dataclasses | Parse-time validation, serialization, schema gen | Heavier dependency |
| httpx over requests | Timeouts, HTTP/2, async-ready, connection pooling | Less universally known |
| Atomic writes | Eliminates corrupt partial outputs | Minor temp file overhead |
| Input fingerprinting | Smart incremental skips | Small hash computation overhead |
| Gusto optional | Tool works for non-Gusto users | Payroll deliverables may be missing |
| Year in all filenames | Self-documenting files; no ambiguity when extracted | Slightly longer filenames |

---

## 8. Success Metrics & Roadmap

### 8.1 Success Metrics

| Metric | Target | How to Measure |
|--------|--------|----------------|
| Full packet generation time | < 3 min | Time `cpapacket build` end-to-end |
| Incremental rebuild time | < 30 sec | Time `cpapacket build --incremental` when inputs unchanged |
| Deliverable completeness | All required items present | Exit code 0 on `build` |
| CPA acceptance | No re-work requests for formatting | Qualitative — CPA feedback |
| Balance sheet equation valid | 100% of runs | Automated validation |
| Retained earnings reconciled | Balanced status on clean books | Automated check |
| Payroll reconciliation variance | < `PAYROLL_RECON_TOLERANCE` on synced books | Automated check |
| Zero manual file renaming | 100% | All files named correctly by tool |
| Zero duplicate API calls during build | 0 duplicates | SessionDataStore cache hit rate = 100% |
| GL pulls succeed on large ledgers | 100% reliability | Monthly slicing + bounded concurrency + retry |
| Headless mode works deterministically | No surprise aborts or overwrites | CI end-to-end test with `--non-interactive` |
| New deliverable integration time | < 1 day | Protocol-based; implement, register, test |

### 8.2 Implementation Roadmap

#### Phase 0 — Foundations (Weeks 1-2)
- [ ] Project scaffolding (pyproject.toml, src layout, test structure)
- [ ] `RunContext` Pydantic model + CLI global flag wiring + `--year` inference logic
- [ ] `Deliverable` protocol + `DeliverableResult` + `DELIVERABLE_REGISTRY` scaffold
- [ ] `core/filesystem.py`: atomic_write, path sanitization
- [ ] `core/retry.py` + `core/limiter.py`: shared retry and rate-limit infrastructure
- [ ] `core/metadata.py`: DeliverableMetadata model + input fingerprint computation
- [ ] Named constants + tolerance thresholds in `constants.py`
- [ ] QBO OAuth client + token management (httpx + keyring)
- [ ] QBO Company Info fetch + company name sanitization
- [ ] Packet directory structure manager
- [ ] File-exists prompt utility with RunContext-based on_conflict
- [ ] SessionDataStore: in-memory cache + `_meta/cache/` disk persistence
- [ ] Structured logging setup (RichHandler console + FileHandler to `_meta/cpapacket.log`)
- [ ] PDF writer base (reportlab CPA template: header, body, footer, via atomic_write)
- [ ] CSV writer base (batch + streaming modes, via atomic_write)
- [ ] JSON writer (raw dump, via atomic_write)
- [ ] Schema version constants

#### Phase 1 — Core Financial Statements (Weeks 3-4)
- [ ] `PnlDeliverable`: data store fetch → Pydantic normalization → CSV + PDF + JSON + metadata
- [ ] `BalanceSheetDeliverable` (current year) — shared `NormalizedRow` model
- [ ] `PriorBalanceSheetDeliverable`
- [ ] Balance equation validation using `BALANCE_EQUATION_TOLERANCE`
- [ ] `GeneralLedgerDeliverable`: monthly slicing → bounded concurrency → streaming CSV + JSON + metadata
- [ ] Thin CLI wrappers for all Phase 1 deliverables

#### Phase 2 — Payroll (Weeks 5-6)
- [ ] Gusto OAuth client + token management (httpx + keyring)
- [ ] Gusto availability detection for graceful degradation
- [ ] `PayrollSummaryDeliverable`: company + per-employee breakdowns + metadata
- [ ] 401(k) separation (employee deferral vs employer contribution)
- [ ] `PayrollReconDeliverable` (Gusto vs QBO) + metadata

#### Phase 3 — Contractor & Distributions (Weeks 7-8)
- [ ] `ContractorSummaryDeliverable`: account detection → vendor aggregation → 1099 flags + metadata
- [ ] Card processor detection logic
- [ ] Unified `MiscodeDetector` engine (`reconciliation/miscode_detector.py`)
- [ ] `DistributionsDeliverable`: equity account scan + unified miscoding detection + metadata
- [ ] `RetainedEarningsDeliverable`: cross-deliverable integration via data store + shared miscode results + metadata
- [ ] Single `likely_miscoded_distributions_YYYY.csv` in 06_ folder

#### Phase 4 — Tax Tracker, Build & Supportability (Weeks 9-10)
- [ ] `TaxTrackerDeliverable`: init, update, mark-paid, status (persisted in `~/.config/cpapacket/`)
- [ ] Default deadline generation + deadline awareness display with `rich.table`
- [ ] QBO Data Health Pre-Check (`cpapacket check`)
- [ ] `cpapacket doctor` command (environment + auth health)
- [ ] `build` command: registry-driven generation → concurrent steps 2–5 → validate → report → zip
- [ ] `--validate-only`, `--incremental`, `--force`, `--skip`, `--method`, `--owner-keywords` flags
- [ ] Exit code precedence logic (1 > 2 > 0)
- [ ] End-to-end integration testing

#### Phase 5 — Polish & Hardening (Weeks 11-12)
- [ ] `rich.progress` progress bars for build steps
- [ ] Error messages and user guidance
- [ ] `--verbose` / `--quiet` / `--plain` / `--version` flags
- [ ] Documentation (README, auth setup guide)
- [ ] Test coverage to 85%+
- [ ] Schema version CI tests
- [ ] Real-data validation with actual QBO/Gusto accounts
- [ ] PyPI packaging + pipx install instructions

### 8.3 Future Considerations (Post-MVP)

- **Multi-year comparison** reports (year-over-year P&L, balance sheet trends)
- **Officer W2 Equivalent** deliverable (folder 05)
- **Quarterly P&L breakdowns** within same packet
- **Persistent config file** (`~/.config/cpapacket/config.toml`) for contractor accounts, owner identity, custom deadlines
- **Excel output** option (openpyxl)
- **Gusto contractor support** (if Gusto adds contractor data)
- **Multi-state tax deadlines** beyond DE/NY
- **Audit log** of all packet generations
- **Pre-built report templates** for common CPA software formats
- **Cover page / table of contents PDF** for CPA orientation
- **Structured JSON logs** (machine-parseable, for monitoring integration)
- **`--no-raw` flag** to skip raw JSON generation (saves disk space)

---

## Appendix A: Canonical Folder Mapping

```python
DELIVERABLE_FOLDERS = {
    "pnl":              "01_Year-End_Profit_and_Loss",
    "balance_sheet":    "02_Year-End_Balance_Sheet",
    "general_ledger":   "03_Full-Year_General_Ledger",
    "payroll_summary":  "04_Annual_Payroll_Summary",
    "officer_w2":       "05_Officer_W2_Equivalent",        # Future
    "distributions":    "06_Shareholder_Distributions",
    "contractor":       "07_Contractor_1099_Summary",
    "estimated_tax":    "08_Estimated_Tax_Payments",
    "retained_earnings":"09_Retained_Earnings_Rollforward",
    "payroll_recon":    "10_Payroll_Reconciliation",
    "meta":             "_meta",
}
```

> **Numbering rationale:** Folders 01–04 match the PRD Section 2 structure. Folder 05 is reserved for the future Officer W2 deliverable. Folders 06–10 are assigned by accounting workflow dependency order: Distributions (06) feeds into the Retained Earnings Rollforward (09), Contractors (07) are independent, Estimated Taxes (08) are standalone, and Payroll Reconciliation (10) is a cross-source integrity check that runs last. This numbering is the single source of truth and lives in `constants.py`.

## Appendix B: Exit Code Reference

| Code | Meaning | When |
|------|---------|------|
| 0 | Full success | Zip created, all required deliverables present, no errors |
| 1 | Hard failure occurred | At least one deliverable hit an unrecoverable error (API, auth, filesystem); zip may still be created with partial content |
| 2 | Partial success | Zip created, no hard failures, but one or more required deliverables missing |

**Precedence:** 1 > 2 > 0 (highest wins).

## Appendix C: File Naming Patterns

All output filenames include the tax year for unambiguous identification when extracted individually.

| Deliverable | PDF Pattern | CSV Pattern | JSON Pattern |
|-------------|-------------|-------------|--------------|
| P&L | `Profit_and_Loss_{start}_to_{end}_{method}.pdf` | Same `.csv` | Same `_raw.json` |
| Balance Sheet | `Balance_Sheet_{YYYY-MM-DD}.pdf` | Same `.csv` | Same `_raw.json` |
| General Ledger | — | `General_Ledger_{YYYY}.csv` | `General_Ledger_{YYYY}_raw.json` |
| Payroll (Company) | `Annual_Payroll_Summary_{YYYY}.pdf` | Same `.csv` | Same `_raw.json` |
| Payroll (Employee) | `Payroll_Breakdown_{Name}_{YYYY}.pdf` | Same `.csv` | Same `_raw.json` |
| Contractor | `contractor_summary_{YYYY}.pdf` | `contractor_summary_{YYYY}.csv` | `contractor_summary_{YYYY}.json` |
| Flagged Contractors | — | `flagged_for_review_{YYYY}.csv` | — |
| Estimated Tax | `estimated_tax_tracker_{YYYY}.pdf` | `estimated_tax_tracker_{YYYY}.csv` | — |
| Deadlines | `tax_deadlines_{YYYY}.pdf` | `tax_deadlines_{YYYY}.csv` | — |
| Payroll Recon | `payroll_reconciliation_{YYYY}.pdf` | `payroll_reconciliation_{YYYY}.csv` | `payroll_reconciliation_{YYYY}.json` |
| Retained Earnings | `Retained_Earnings_Rollforward_{YYYY}.pdf` | Same `.csv` | Same `_data.json` |
| Distributions | `distributions_summary_{YYYY}.pdf` | `distributions_summary_{YYYY}.csv` | `distributions_summary_{YYYY}.json` |
| Likely Miscoded | — | `likely_miscoded_distributions_{YYYY}.csv` (in 06_ folder; includes `txn_id`) | — |

## Appendix D: Global CLI Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--version` | bool | false | Print version and exit |
| `--verbose` / `-v` | bool | false | Set console log level to DEBUG |
| `--quiet` / `-q` | bool | false | Set console log level to WARNING only |
| `--plain` | bool | false | Disable `rich` formatting; plain text output for piping/scripting |
| `--non-interactive` | bool | auto-detect via `isatty()` | Accept all defaults; skip prompts requiring freeform input |
| `--on-conflict` | choice | prompt (interactive) / abort (non-interactive) | `overwrite`, `copy`, or `abort` — controls file-exists behavior without prompting |
| `--incremental` | bool | false | Skip deliverable regeneration if input fingerprint matches and outputs exist |
| `--force` | bool | false | Ignore all caches and metadata; re-fetch API data and regenerate all outputs |
| `--owner-keywords` | string | none | Comma-separated owner name keywords for miscoding detection |

**Build-specific flags:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--year` | int | inferred (see below) | Tax year for the packet |
| `--method` | choice | accrual | `accrual` or `cash` — controls P&L accounting method |
| `--skip` | string | none | Comma-separated deliverable keys to skip |
| `--validate-only` | bool | false | Skip generation; validate existing files + zip |

**`--year` resolution order:**
1. Explicit `--year YYYY` flag (always wins)
2. Inferred from existing packet directory name in `--out` (if matches `*_{YYYY}_CPA_Packet`)
3. Default: previous calendar year (`current_year - 1`) if current month is Jan–Sep; current year if Oct–Dec
4. Console always logs the resolved year: `"Tax year: 2025 (inferred from directory)"` or `"Tax year: 2025 (default: prior year)"`

## Appendix E: SessionDataStore Cache Specification

**Cache Location:** `_meta/cache/` within the packet directory

**Cache Key:** `sha256(json.dumps(sorted({"source": "qbo"|"gusto", "endpoint": str, "params": dict}.items())))`

**Cache Entry Format:**
```json
{
  "key": "abc123...",
  "source": "qbo",
  "endpoint": "reports/ProfitAndLoss",
  "params": {"start_date": "2025-01-01", "end_date": "2025-12-31", "accounting_method": "Accrual"},
  "fetched_at": "2025-03-15T10:30:00Z",
  "ttl_hours": 24,
  "data": { ... }
}
```

**Behavior:**
- On cache hit with valid TTL: return `data` directly, log cache hit at DEBUG
- On cache miss or expired TTL: call API, store result, return `data`
- On `--force` flag: always call API regardless of cache state
- General Ledger: 12 separate cache entries (one per monthly slice), stored at `_meta/cache/qbo/general_ledger/YYYY/MM_raw.json`
- Company Info: cached indefinitely within a session (company name doesn't change)
- Thread safety: `threading.Lock` per cache key for concurrent `build` steps 2–5

## Appendix F: CLI Command Summary

| Command | Purpose | Key Outputs |
|---------|---------|-------------|
| `build --year --out` | Generate + validate + zip | All deliverables + `_meta/validation_report.txt` + zip |
| `build --year --out --validate-only` | Validate + zip (no API calls) | `_meta/validation_report.txt` + zip |
| `build --year --out --incremental` | Smart rebuild (skip unchanged) | Only stale/missing deliverables regenerated |
| `build --year --out --method cash` | Generate with cash-basis P&L | All deliverables (P&L uses cash method) |
| `pnl --start --end --method --out` | P&L | PDF + CSV + raw JSON + metadata |
| `balance-sheet --year --out` | Balance sheet | PDF + CSV + raw JSON + metadata |
| `prior-balance-sheet --year --out` | Prior balance sheet | PDF + CSV + raw JSON + metadata |
| `general-ledger --year --out` | General ledger | CSV + raw JSON + metadata |
| `payroll-summary --year --out` | Payroll package (requires Gusto) | Company + per-employee outputs + metadata |
| `contractor-summary --year --out` | 1099 review summary | PDF + CSV + JSON (+ flagged CSV) + metadata |
| `tax init/update/mark-paid/status --year` | Tracker + deadlines | Local state JSON + packet PDFs/CSVs |
| `payroll-recon --year --out` | Payroll cost check (requires Gusto) | PDF + CSV + JSON + metadata |
| `distributions --year --out` | Distributions + flags | PDF + CSV + JSON (+ flagged CSV) + metadata |
| `retained-earnings --year --out` | Rollforward | PDF + CSV + JSON + metadata |
| `check --year` | QBO data quality | Console warnings + `_meta/data_health_check.txt` |
| `doctor` | Environment/auth health | Console report |
| `auth qbo\|gusto login\|status\|logout` | Auth flows | Token store changes + console report |

## Appendix G: Named Constants and Tolerance Thresholds

All magic numbers are defined in `constants.py` as named constants. Tests reference these constants to ensure consistency.

```python
from decimal import Decimal

# --- Financial Tolerances ---
BALANCE_EQUATION_TOLERANCE = Decimal("1.00")       # Assets vs Liabilities + Equity (E3)
RETAINED_EARNINGS_TOLERANCE = Decimal("1.00")       # Expected vs Actual ending RE (E9)
PAYROLL_RECON_TOLERANCE = Decimal("5.00")            # Gusto vs QBO payroll total (E8)

# --- Regulatory Thresholds ---
CONTRACTOR_1099_THRESHOLD = Decimal("600.00")        # Non-card payment threshold for 1099 review (E6)

# --- Miscoding Detection ---
MISCODE_HIGH_AMOUNT_THRESHOLD = Decimal("500.00")    # "high amount" signal (E10 R3, R5)
MISCODE_ROUND_NUMBER_DIVISOR = Decimal("500.00")     # round-number heuristic (E10 R4)
MISCODE_CONFIDENCE_HIGH = 6                           # score >= this → High confidence
MISCODE_CONFIDENCE_MEDIUM = 4                         # score >= this → Medium confidence
MISCODE_CONFIDENCE_LOW = 2                            # score >= this → Low confidence (below = not flagged)

# --- API Limits ---
QBO_MAX_CONCURRENCY = 4                               # max simultaneous QBO API requests
GUSTO_MAX_CONCURRENCY = 2                             # max simultaneous Gusto API requests
RETRY_MAX_429 = 5                                     # max retries on 429 Too Many Requests
RETRY_MAX_5XX = 3                                     # max retries on 5xx Server Error

# --- Cache ---
CACHE_TTL_HOURS = 24                                  # API response cache time-to-live
```
