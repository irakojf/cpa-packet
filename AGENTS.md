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

**Permission prompts / approval prompts / confirmation prompts:** In 1-2 sentences, explain the the reasoning why a command needs permission or approval. Name any risks or tradeoffs to be aware of. 

## Git & Workspace Hygiene Rules

These rules exist to reduce commit noise, concurrent-change pauses, and git sandbox friction in multi-agent swarms.

0. General rules: 
- Default branch is **main** (NEVER reference or use `master`).
- All work happens on **main** or short-lived feature branches that merge to main.
- If `master` exists for legacy reasons: keep it synchronized after main pushes (`git push origin main:master`).

1. Before any git add / commit operation:
   - Always run `git reset` first (unstages everything).
   - Then explicitly stage **only** the files relevant to the current bead/task.
   - Verify with `git diff --staged` or `git status --short` that **only** intended files are staged.
   - Never rely on `git commit -a` or auto-staging — it picks up unrelated changes.

2. Before committing:
   - Run `git status` and confirm no unexpected pre-staged files or modified files outside your scope.
   - If unexpected staged/modified files appear → pause, mail the swarm ("Detected pre-staged files not belonging to this bead: [list]"), and ask Human Overseer for direction (keep mixed commit / unstage & separate / revert extras).
   - Commit message MUST reference the bead ID (e.g., "bd-2gy.19: add accounts_list.json fixture + tests").

3. Sandbox / permission-aware git behavior:
   - If any git command fails with "Operation not permitted", "index.lock", or permission error inside .git/:
     - Do NOT retry the command.
     - Immediately pause the current bead.
     - Send MCP Agent Mail to Human Overseer: "Git write blocked by sandbox (.git/index.lock or permission denied). Request external fix."
   - Never attempt to create, modify, or delete files inside .git/ directory.
   - Human is responsible for git operations that require .git/ writes (checkout, commit, rebase, etc.) when sandbox blocks them.

4. Concurrent change detection & pause:
   - On detecting concurrent/inconsistent workspace state (untracked files appearing, .beads changes not authored by self, git status shows unexpected diffs):
     - Pause immediately per concurrent-change rule.
     - Mail swarm: "Detected concurrent changes: [describe]. Pausing for guidance."
     - Do NOT proceed with destructive actions (rm, overwrite, commit) without explicit Human approval.
   - Prefer releasing your own file reservations early if blocked, to avoid blocking others.

5. Beads DB & reset safety:
   - NEVER run `rm -rf .beads/` without explicit Human approval in the same message.
   - Before reset: always check `git ls-files .beads/issues.jsonl` and `git log -- .beads/issues.jsonl` to confirm the file is tracked and has history.
   - After reset sequence (rm -rf .beads/ → git checkout → br init → br sync --import-only):
     - Immediately run `br doctor` and `bv --robot-priority` (or `bv --robot-next`).
     - If 0 issues imported → mail Human: "issues.jsonl empty or missing after sync — beads appear lost. Request recovery."
   - Use ONLY `br` and `bv` commands — legacy `bd` is forbidden.

6. Commit frequency & granularity:
   - Commit after completing a logical sub-task within a bead (small, frequent commits preferred).
   - Avoid large, multi-file commits unless the entire change set belongs to one bead.
   - If a bead spans many files → break it into smaller child beads (e.g., bd-2aq.10, bd-2aq.11).

7. General principle:
   - Minimize git noise and staging surprises — the goal is clean, auditable history per bead.
   - When in doubt → pause, mail swarm/Human, and wait for direction rather than risk mixed commits or overwrites.
   - Human override (RULE 0) always applies — explicit instructions supersede these rules.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## General Code Editing Discipline

- **Never proliferate files** just to avoid editing existing ones.  
  No `main_v2.py`, `script_old.sh`, `backup_2026.js`, etc.  
  Edit in place unless the new file adds genuinely distinct, non-overlapping responsibility.

- **No brittle script-based mass edits** (sed/regex across codebase).  
  Make changes manually or with parallel sub-agents. For large simple refactors, plan carefully and verify.

- **Backwards compatibility** — we usually do NOT care in early stages.  
  Prefer the **right** way now over shims/legacy support. Fix directly.

- **Concurent changes** 
    Before modifying any file outside your current bead, or if you see concurrent changes:
    1. Send Agent Mail message: "Detected concurrent changes on <files>. Pausing. Waiting for guidance."
    2. Stop all work and ask human how to proceed.

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

### Beads and Beads Viewer 
- This project uses [beads_viewer](https://github.com/Dicklesworthstone/beads_viewer) for issue tracking. Issues are stored in `.beads/` and tracked in git.
- Use **beads** (`br`, `bv`) for task tracking, dependency graphs, prioritization.
- Use ONLY br (beads_rust) and bv. Legacy bd is forbidden.
- On any DB error (not initialized, migrate required, cache rebuild fail, null constraint): 
  1. rm -rf .beads/
  2. git checkout HEAD -- .beads/
  3. br init && br sync --import-only
  4. Verify with br doctor / bv
- Never run bd migrate or bd update on broken DB — escalate to human via Agent Mail.

```bash
# View issues (launches TUI - avoid in automated sessions)
bv

# CLI commands for agents (use these instead)
bd ready              # Show issues ready to work (no blockers)
bd list --status=open # All open issues
bd show <id>          # Full issue details with dependencies
bd create --title="..." --type=task --priority=2
bd update <id> --status=in_progress
bd close <id> --reason="Completed"
bd close <id1> <id2>  # Close multiple issues at once
bd sync               # Commit and push changes
```

### Workflow Pattern

1. **Start**: Run `bd ready` to find actionable work
2. **Claim**: Use `bd update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `bd close <id>`
5. **Sync**: Always run `bd sync` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `bd ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
- **Blocking**: `bd dep add <issue> <depends-on>` to add dependencies

### Session Protocol

**Before ending any session, run this checklist:**

```bash
git status              # Check what changed
git add <files>         # Stage code changes
bd sync                 # Commit beads changes
git commit -m "..."     # Commit code
bd sync                 # Commit any new beads changes
git push                # Push to remote
```

### Best Practices

- Check `bd ready` at session start to find available work
- Update status as you work (in_progress → closed)
- Create new issues with `bd create` when you discover tasks
- Use descriptive titles and set appropriate priority/type
- Always `bd sync` before ending session

<!-- end-bv-agent-instructions -->

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

"First read ALL of AGENTS.md and README.md super carefully and understand ALL of both!  
Then use your code investigation mode to fully understand the architecture and purpose.  
Register with MCP Agent Mail and introduce yourself.  
Check mail and respond if needed.  
Use bv to find the most impactful bead(s) you can usefully work on now.  
Proceed meticulously, mark beads, communicate via mail. Use ultrathink.  
**If a subtask is clearly mechanical/narrow and would benefit from delegation, feel free to spawn a subagent — but prefer full-agent swarm coordination via MCP mail when possible.** " 


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