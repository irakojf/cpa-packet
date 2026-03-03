# AGENTS.md
> Guidelines and rules for ALL AI coding agents working in this project.  
> Read this file **completely and carefully** at the start of EVERY session.  
> Re-read it immediately after any conversation compaction or reset.  
> You MUST follow EVERY rule below unless I explicitly override it.

## RULE 0 — THE FUNDAMENTAL OVERRIDE PREROGATIVE

If **I** (the human) tell you to do something — even if it contradicts the rules below — **YOU MUST OBEY ME IMMEDIATELY**.  
I am in charge. You are not.

## RULE 1 — NO FILE DELETION (EVER — WITHOUT EXPLICIT PERMISSION)

**YOU ARE PERMANENTLY FORBIDDEN FROM DELETING ANY FILE OR DIRECTORY** — even one you created yourself — **unless I give clear, explicit, written permission in the same message**.

- Phrase requests like: “May I delete file X? It is no longer used because Y.”
- Wait for my unambiguous YES before acting.
- Document my exact permission text in your response / commit message.

You have a history of catastrophic deletions. This rule is permanent.

## RULE 2 — IRREVERSIBLE / DESTRUCTIVE ACTIONS — DO NOT BREAK GLASS

Never run commands that can cause irreversible data loss or major damage without my explicit, verbatim approval of the **exact command** and acknowledgment of risks.

**Strictly forbidden without my per-command permission:**

- `rm -rf`, `git reset --hard`, `git clean -fdx`, `: > file`, destructive redirects
- Any bulk delete, overwrite, or force operation
- Database drops, schema changes, container/image rm/prune without backup
- Package uninstalls that break dependencies

**Safer alternatives first:** Use `git status`, `git diff`, `git stash`, backups, dry-runs, `--dry-run` flags, temporary copies.

**Even after approval:**
- Restate the exact command
- List exactly what will be affected
- Wait for my final confirmation
- Log the authorizing message + timestamp

## Git Branch Policy

- Default branch is **main** (NEVER reference or use `master`).
- All work happens on **main** or short-lived feature branches that merge to main.
- If `master` exists for legacy reasons: keep it synchronized after main pushes (`git push origin main:master`).

## General Code Editing Discipline

- **Never proliferate files** just to avoid editing existing ones.  
  No `main_v2.py`, `script_old.sh`, `backup_2026.js`, etc.  
  Edit in place unless the new file adds genuinely distinct, non-overlapping responsibility.

- **No brittle script-based mass edits** (sed/regex across codebase).  
  Make changes manually or with parallel sub-agents. For large simple refactors, plan carefully and verify.

- **Backwards compatibility** — we usually do NOT care in early stages.  
  Prefer the **right** way now over shims/legacy support. Fix directly.

## Testing — The Safety Net

Tests are **mandatory** for agent autonomy. Comprehensive tests let agents explore, refactor, fix bugs, and polish unsupervised.

**Minimum requirements (adapt to your language/stack):**

- Unit tests for new/changed functions, classes, modules
- Cover happy path + edge cases (empty/null/max/min/zero/overflow) + error/failure modes
- Integration / end-to-end tests for core workflows (preferably realistic, minimal mocks)
- Deterministic when possible (seed random, virtual time, fixed inputs)

**Quality gates before any commit:**

- Run full test suite
- Lint / format check
- Type check (if applicable)
- Fix anything broken

**When in doubt:** Add a test that would have caught the bug you just fixed.

**Prompts to trigger test work:**

- “Do we have full unit test coverage without heavy mocks? What about complete E2E scripts with detailed logging? If not, create beads for this.”
- After fixes: “Rerun tests and confirm nothing broke.”

## Fresh Eyes & Peer Review Loops

After any meaningful change:

1. **Self-review** — “Read your new/modified code with fresh eyes. Hunt for bugs, inefficiencies, security issues, reliability problems, confusion. Fix anything found. Use ultrathink.”
2. **Peer review** — “Review code written by fellow agents. Diagnose root causes. Fix if necessary. Cast a wide net.”
3. Repeat until clean.

## Daily/Continuous Polishing Flywheel

Regularly run variants of:

- “Randomly explore code files. Deeply understand flows. Perform careful fresh-eyes bug hunt. Fix issues while complying with all AGENTS.md rules.”
- “Scrutinize workflow/implementation for anything suboptimal, wrong, or user-unfriendly. Improve UX intuitiveness, polish, reliability.”

## Toolchain & Agent Workflow Expectations

- Use **beads** (`br`, `bv`) for task tracking, dependency graphs, prioritization.
- Coordinate via **MCP Agent Mail** — register, introduce yourself, respond promptly, avoid purgatory.
- Search past sessions/logs with **cass** when needed.
- Prioritize beads with `bv` (robot flags when appropriate).
- Commit logically grouped changes with **detailed messages** — never commit junk/ephemeral files.

**Subagents (Claude Code / similar tools only)**  
- You MAY spawn subagents for narrow, mechanical, formulaic, or highly parallelizable subtasks (e.g., quick bug fixes, targeted explorations, simple refactors, verification loops) when it would save tokens, preserve context, or accelerate progress.  
- Do NOT feel required to use them — full first-class agents (this instance + swarm via MCP mail) are preferred for most complex or creative work.  
- If you spawn subagents:  
  - Keep them focused and short-lived.  
  - Coordinate results back to the main task clearly.  
  - Avoid over-delegation that fragments reasoning.  
- The decision to use subagents is **yours** — use your judgment (ultrathink) on whether it improves efficiency without losing quality or coherence.  
- Subagents do NOT replace peer review from other full agents via MCP Agent Mail.

**Initialization prompt pattern (use this often):**

> First read ALL of AGENTS.md and README.md super carefully and understand ALL of both!  
> Then use your code investigation mode to fully understand the architecture and purpose.  
> Register with MCP Agent Mail and introduce yourself.  
> Check mail and respond if needed.  
> Use bv to find the most impactful bead(s) you can usefully work on now.  
> Proceed meticulously, mark beads, communicate via mail. Use ultrathink.  
> **If a subtask is clearly mechanical/narrow and would benefit from delegation, feel free to spawn a subagent — but prefer full-agent swarm coordination via MCP mail when possible.**


**After compaction:**

> Reread AGENTS.md so it is fresh in your mind. 

## Project-Specific Section — Customize Here

- Language / primary stack: Python 3.11+, src-layout (`src/cpapacket/`), `pyproject.toml` packaging
- Key frameworks/libraries: click (CLI), pydantic v2 (models), httpx (HTTP), reportlab (PDF), rich (CLI output), keyring (token storage), platformdirs (config paths), respx (test mocking)
- Test runner & coverage target: pytest, 85% coverage minimum (`--cov-fail-under=85`), mypy --strict for type checking
- Linter/formatter commands: `ruff check .` and `ruff format --check .`
- Core invariants/business rules to always preserve:  
  1. Never delete source folders; never block zip creation — warnings and exit code 2, never hard-fail on missing deliverables  
  2. All monetary values use `decimal.Decimal`, never `float`; all tolerance comparisons use named constants from `constants.py` (e.g., `BALANCE_EQUATION_TOLERANCE`, `PAYROLL_RECON_TOLERANCE`)  
  3. Atomic file writes everywhere — write to `.tmp`, then `os.replace()` to final path; no output file may ever contain partial content  
  4. Every deliverable writes `_meta/{key}_metadata.json` with `input_fingerprint` (sha256 of canonicalized inputs), `schema_versions`, and artifact list — this is the source of truth for `--incremental` skip decisions  
  5. All API calls go through `SessionDataStore` — no direct HTTP calls from deliverable code; zero duplicate API calls during a single `build` run  
- Forbidden patterns or legacy gotchas: No `float` for money (Decimal only). No `requests` library (httpx only). No `localStorage`/`sessionStorage` in any artifacts. No hardcoded magic numbers for thresholds — use named constants. No business logic in `cli/*.py` modules — those are thin wrappers that delegate to `deliverables/*.py` implementations. Never log tokens or secrets. Never use bare `open()` for output files — always go through `atomic_write()`. `--on-conflict` defaults to **abort** (not overwrite) in non-interactive mode. Gusto is optional — never hard-fail if Gusto auth is absent; auto-skip dependent deliverables.
- Preferred output style (CLI, logging, errors): `rich` for all console output — `Progress` bars for build steps, `Table` for summaries/dashboards, `Panel` (yellow=warning, red=error, green=success). Dual logging: `RichHandler` to console at INFO (or DEBUG with `--verbose`, WARNING with `--quiet`), `FileHandler` to `_meta/cpapacket.log` at DEBUG always. Errors to stderr with actionable guidance (e.g., "Run `cpapacket auth qbo login`"); no raw stack traces unless `--verbose`; full traces always in log file. Respect `NO_COLOR` env var and `--plain` flag. No emoji in warnings — text labels only.

**All agents must comply with the above rules religiously.**  
Failure to do so degrades trust and autonomy — the opposite of our goal.