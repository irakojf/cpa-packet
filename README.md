# cpapacket

`cpapacket` is a Python CLI for generating CPA-ready tax packet artifacts from QuickBooks Online
(required) and Gusto (optional) data.

It supports single-deliverable runs, full packet builds, health checks, privacy scans, and tax
tracker workflows. Output artifacts are structured for repeatable review and automation.

## Requirements

- Python `3.11+`
- QuickBooks OAuth app credentials (required for QBO-backed workflows)
- Gusto OAuth app credentials (optional; payroll-specific workflows auto-skip when unavailable)

## Installation

### Install with pip

```bash
pip install .
```

### Install with pipx (recommended for CLI usage)

```bash
pipx install .
```

After install, verify the command:

```bash
cpapacket --help
```

Upgrade/reinstall during active development:

```bash
pipx reinstall cpapacket
```

Remove:

```bash
pipx uninstall cpapacket
```

## Authentication and Environment

Set required OAuth environment variables before auth/login commands:

```bash
export CPAPACKET_QBO_CLIENT_ID="..."
export CPAPACKET_QBO_CLIENT_SECRET="..."
export CPAPACKET_QBO_REDIRECT_URI="http://localhost:8000/callback"
export CPAPACKET_QBO_REALM_ID="..."
# Optional: use sandbox API host for sandbox companies
# export CPAPACKET_QBO_API_BASE_URL="https://sandbox-quickbooks.api.intuit.com/v3/company"

# Optional (only needed for Gusto features)
export CPAPACKET_GUSTO_CLIENT_ID="..."
export CPAPACKET_GUSTO_CLIENT_SECRET="..."
export CPAPACKET_GUSTO_REDIRECT_URI="http://localhost:8000/callback"
```

Authenticate providers:

```bash
cpapacket auth qbo login --state my-state
cpapacket auth qbo status

# Optional
cpapacket auth gusto login --state my-state
cpapacket auth gusto status
```

## Quick Start

Run from the output packet directory (or pass `--year` explicitly):

```bash
cpapacket --year 2025 --non-interactive build
```

Useful build flags:

- `--skip <key>`: skip one or more deliverables
- `--validate-only`: run validation/reporting only
- `--continue-on-failure` / `--fail-fast`
- `--incremental`: skip up-to-date deliverables
- `--force`: bypass incremental/cache checks
- `--on-conflict {overwrite,copy,abort,prompt}`

## Command Reference

Core commands:

- `cpapacket build`
- `cpapacket check`
- `cpapacket doctor`
- `cpapacket privacy`
- `cpapacket context-debug`

Deliverable commands:

- `cpapacket pnl`
- `cpapacket general-ledger`
- `cpapacket payroll-summary`
- `cpapacket payroll-recon`
- `cpapacket contractor-summary`
- `cpapacket distributions`
- `cpapacket retained-earnings`

Tax tracker commands:

- `cpapacket tax init`
- `cpapacket tax update`
- `cpapacket tax status`
- `cpapacket tax mark-paid`

For detailed flags and examples on any command:

```bash
cpapacket <command> --help
```

## Output Structure

Typical build output includes:

- Deliverable CSV/PDF/raw artifacts
- `_meta/` metadata and logs
- Validation report
- Packet summary
- Packet manifest
- Final zip archive

`cpapacket` keeps packet creation resilient:

- Missing optional deliverables produce warnings and validation signals
- Missing required data can raise non-zero exit codes without corrupting outputs
- Zip creation is preserved with best-effort behavior

## Development

Install dev dependencies:

```bash
pip install -e ".[dev]"
```

Run quality gates:

```bash
ruff check .
ruff format --check .
mypy --strict
pytest -q
```

## Contributing

1. Make small, scoped changes.
2. Add/adjust tests for behavior changes.
3. Run lint/type/test checks before opening a PR.
4. Keep CLI output and metadata behavior consistent with existing contract.
