# CPA Output Build Spec

## Goal

Make the QBO export packet faster for a CPA to review, especially for an LLC taxed as an S Corp.

The packet should answer these questions with minimal interpretation:

1. Do the core financial statements tie exactly to QBO?
2. Are owner-related transactions classified correctly as distributions, contributions, shareholder loans/receivables, or direct equity postings?
3. Which items are mechanically tied out versus still requiring CPA judgment?

## Decision

Phase 1 should **not** introduce a brand-new top-level deliverable key.

Keep the existing deliverable key `retained_earnings` and the existing slot `09_Retained_Earnings_Rollforward` for build/registry stability, but redesign the artifacts inside that folder into an **Equity Review** package.

Reason:

- This avoids immediate churn across `registry.py`, CLI wiring, validator expectations, manifest output, and existing packet numbering.
- The CPA-facing filenames and PDF titles can be corrected immediately without waiting on a broader key/folder rename.
- A later cleanup pass can rename the deliverable key/folder to `equity_review` once the new outputs are proven.

## Phase 1 Scope

Phase 1 includes:

- redesigning the current retained-earnings deliverable into a CPA-facing equity review package
- improving the distributions package so it explicitly bridges GL activity to balance-sheet movement
- improving the contractor package labeling so it reads as a review workpaper rather than a final tax conclusion
- adding a packet-level CPA review dashboard

Phase 1 does not include:

- shareholder stock-basis calculations
- AAA / OAA / tax-basis schedules
- filing-ready 1099 determination logic
- payroll work beyond current optional/skipped behavior

## Deliverable Changes

### 1. Packet-Level Review Dashboard

Add a new packet-level artifact written under the packet root:

- `00_REVIEW_DASHBOARD.md`
- `00_REVIEW_DASHBOARD.pdf`

Purpose:

- first file a CPA opens
- one-page summary of tied values and review flags

Required sections:

- Company and year
- QBO source reports present
- Net income
- Ending cash
- Ending liabilities
- Distributions total
- Contributions total
- Shareholder receivable ending balance
- Contractor/1099 review count
- Open review flags
- Explicit statement: `Book review only; not shareholder basis or AAA`

Primary inputs:

- `pnl`
- `balance_sheet`
- `prior_balance_sheet`
- `distributions`
- `contractor`
- redesigned `retained_earnings`

Implementation note:

- This can be a packet-level helper, not a new numbered deliverable folder.
- Metadata should still be written under `_meta/private/deliverables/review_dashboard_metadata.json`.

### 2. Redesign `09_Retained_Earnings_Rollforward` as Equity Review

Keep folder:

- `09_Retained_Earnings_Rollforward`

Replace current CPA-facing outputs with:

- `Book_Equity_Rollforward_{year}.csv`
- `Book_Equity_Rollforward_{year}.pdf`
- `Equity_Tie_Out_to_QBO_{year}.csv`
- `Distribution_Bridge_{year}.csv`
- `Shareholder_Receivable_Activity_{year}.csv`
- `Direct_Equity_Postings_{year}.csv`
- `CPA_NOTES.md`

Replace current dev output with:

- `Equity_Review_{year}_data.json`

Do not present the main PDF as “Retained Earnings Rollforward”.
Use:

- PDF title: `Book Equity Rollforward and QBO Tie-Out`

#### 2.1 `Book_Equity_Rollforward_{year}.csv`

Purpose:

- CPA-facing book equity rollforward
- not tax-basis, not AAA

Required columns:

- `year`
- `beginning_book_equity_bucket`
- `current_year_net_income`
- `current_year_distributions_gl`
- `current_year_distributions_bs_change`
- `current_year_contributions`
- `other_direct_equity_postings`
- `expected_ending_book_equity_bucket_gl_basis`
- `expected_ending_book_equity_bucket_bs_basis`
- `actual_ending_book_equity_bucket`
- `gl_basis_difference`
- `bs_basis_difference`
- `status`
- `flags`

Definitions:

- `beginning_book_equity_bucket`
  - prior-year bucket formed from QBO equity lines that economically represent accumulated book equity
  - include retained earnings and prior net income
  - exclude opening balance equity, contributions, and shareholder receivable
- `current_year_net_income`
  - QBO P&L bottom line for the year
- `current_year_distributions_gl`
  - total GL activity from distribution-style equity rows
- `current_year_distributions_bs_change`
  - absolute change in balance-sheet distributions balance from prior year to current year
- `current_year_contributions`
  - GL activity from contribution-style equity rows
- `other_direct_equity_postings`
  - direct postings to equity accounts not classified as retained earnings, net income, distributions, or contributions
- `actual_ending_book_equity_bucket`
  - current-year bucket formed from QBO retained earnings and current-year net income
- `expected_ending_book_equity_bucket_gl_basis`
  - `beginning + net_income - distributions_gl + contributions + other_direct_equity_postings_effect`
- `expected_ending_book_equity_bucket_bs_basis`
  - `beginning + net_income - distributions_bs_change + contributions + other_direct_equity_postings_effect`

Status rules:

- `Balanced`
  - both differences within `RETAINED_EARNINGS_TOLERANCE`
- `Review`
  - one or both differences exceed tolerance

Flag rules:

- `distributions_gl_vs_bs_mismatch`
- `negative_ending_book_equity`
- `distributions_exceed_current_year_income`
- `direct_retained_earnings_postings_detected`
- `shareholder_receivable_present`

#### 2.2 `Book_Equity_Rollforward_{year}.pdf`

Purpose:

- one-page human review summary

Required sections:

- `Summary`
- `QBO Source Lines`
- `Rollforward`
- `Review Flags`

Required lines:

- Beginning book equity bucket
- Net income
- Distributions from GL
- Distributions from balance-sheet change
- Contributions
- Other direct equity postings
- Expected ending bucket on GL basis
- Expected ending bucket on BS basis
- Actual ending bucket
- GL basis difference
- BS basis difference
- Status

The PDF must contain a note:

- `This is a book-equity review schedule. It is not shareholder basis, AAA, or a tax-basis capital schedule.`

#### 2.3 `Equity_Tie_Out_to_QBO_{year}.csv`

Purpose:

- bridge the rollforward to exact QBO balance-sheet lines

Required columns:

- `year`
- `as_of_date`
- `source_statement`
- `line_label`
- `classification`
- `amount`
- `included_in_book_equity_bucket`
- `bucket_component`
- `review_note`

Required classifications:

- `retained_earnings`
- `current_net_income`
- `distribution_equity`
- `contribution_equity`
- `opening_balance_equity`
- `shareholder_receivable`
- `other_equity`

Rows required:

- every current-year and prior-year balance-sheet equity line used in the rollforward

#### 2.4 `Distribution_Bridge_{year}.csv`

Purpose:

- explain the difference between GL-classified distributions and balance-sheet movement

Required columns:

- `year`
- `distribution_total_gl`
- `distribution_total_bs_change`
- `difference`
- `difference_status`

Plus transaction-detail section written as a second CSV artifact:

- `Distribution_Bridge_Detail_{year}.csv`

Detail columns:

- `date`
- `txn_type`
- `doc_num`
- `payee`
- `account_name`
- `memo`
- `signed_amount`
- `bridge_bucket`
- `reason`

Bridge buckets:

- `in_gl_and_in_bs`
- `in_gl_only`
- `in_bs_only`
- `needs_review`

Phase 1 target:

- if a full transactional bridge is not possible from current source payloads, write the summary CSV and a best-effort detail CSV listing all candidate distribution/equity rows used in the comparison
- do not silently pick GL or BS basis as “correct”

#### 2.5 `Shareholder_Receivable_Activity_{year}.csv`

Purpose:

- surface owner-related items that may belong in distributions or reimbursements

Required columns:

- `date`
- `txn_type`
- `doc_num`
- `payee`
- `account_name`
- `memo`
- `debit`
- `credit`
- `signed_amount`
- `ending_balance_impact`
- `review_flag`

Selection rule:

- include any GL row whose account name contains `shareholder receivable`, `due from shareholder`, `loan to shareholder`, or configured equivalents

#### 2.6 `Direct_Equity_Postings_{year}.csv`

Purpose:

- show unusual direct journal activity in equity accounts

Required columns:

- `date`
- `txn_type`
- `doc_num`
- `account_name`
- `payee`
- `memo`
- `debit`
- `credit`
- `signed_amount`
- `posting_classification`
- `review_flag`

Selection rule:

- include equity-account GL rows that are not classified as:
  - retained earnings bucket line
  - net income closeout presentation
  - standard distributions
  - standard contributions

#### 2.7 `CPA_NOTES.md`

Purpose:

- give the CPA the interpretation guardrails in one minute

Required content:

- this is a book-equity review package
- it is not shareholder basis
- it is not AAA/OAA
- QBO may display net income and distributions as separate equity lines
- distributions are shown on both GL and balance-sheet-change bases when they do not agree
- shareholder receivable requires manual judgment

### 3. Improve `06_Shareholder_Distributions`

Keep current folder and deliverable key, but expand outputs.

Keep:

- `distributions_summary_{year}.csv`
- `distributions_summary_{year}.pdf`
- likely miscoded distributions CSV

Add:

- `distribution_activity_{year}.csv`
- `distribution_balance_bridge_{year}.csv`

#### 3.1 `distribution_activity_{year}.csv`

Required columns:

- `date`
- `txn_type`
- `doc_num`
- `payee`
- `account_name`
- `memo`
- `debit`
- `credit`
- `signed_amount`
- `classification`

Classification values:

- `distribution`
- `contribution`
- `owner_related_non_distribution`
- `needs_review`

#### 3.2 `distribution_balance_bridge_{year}.csv`

Required columns:

- `year`
- `prior_distribution_balance`
- `current_distribution_balance`
- `balance_sheet_change`
- `gl_distribution_total`
- `difference`
- `status`

This file is the lightweight bridge owned by the distributions deliverable.
The more complete CPA-facing explanation lives in `09_Retained_Earnings_Rollforward/Distribution_Bridge_{year}.csv`.

### 4. Reframe `07_Contractor_1099_Summary`

Keep deliverable key and folder.

Rename CPA-facing files from “summary” language to “review” language:

- `Contractor_1099_Review_{year}.csv`
- `Contractor_1099_Review_{year}.pdf`

Keep a dev JSON payload.

Required new columns in the CSV:

- `vendor_name`
- `total_paid`
- `card_processor_total`
- `non_card_total`
- `selected_source_accounts`
- `threshold`
- `flagged_for_1099_review`
- `review_note`

Required PDF note:

- `This is a QBO-account-based 1099 review schedule, not a final filing determination.`

## Data Extraction Rules

### Book Equity Bucket

Phase 1 book-equity bucket should use a stricter definition than the current retained-earnings bucket.

Include:

- `Retained Earnings`
- `Net Income` / `Net Loss`

Exclude:

- `Opening Balance Equity`
- contribution-style accounts
- distribution-style accounts
- shareholder receivable / due from shareholder accounts

Reason:

- the current bucket is useful for a mechanical QBO tie-out
- it is too broad to label as retained earnings in CPA-facing outputs

### Distributions

Compute and retain both values:

- GL basis
- balance-sheet-change basis

Never silently collapse them into a single authoritative number when they differ.

### Contributions

Compute separately from GL activity on contribution-style equity rows.

### Other Direct Equity Postings

Compute separately from equity-account rows not classified into standard buckets.

### Shareholder Receivable

Do not net this into distributions or book equity.
Show it explicitly and flag it for CPA review.

## Metadata Contract

All new or changed artifacts must continue following the project metadata invariant:

- atomic writes only
- `_meta/{key}_metadata.json` or canonical private metadata path
- `input_fingerprint`
- `schema_versions`
- full artifact list

For the redesigned `retained_earnings` deliverable, metadata inputs must include at least:

- `year`
- `beginning_book_equity_bucket`
- `net_income`
- `distribution_total_gl`
- `distribution_total_bs_change`
- `contributions_total`
- `other_direct_equity_postings_total`
- `actual_ending_book_equity_bucket`
- `shareholder_receivable_ending_balance`
- `gl_row_count`
- `no_raw`

Schema version updates required:

- bump `retained_earnings` CSV schema from `1.0` to `2.0`
- bump `distributions` CSV schema from `1.0` to `2.0`
- bump `contractor` CSV schema from `1.0` to `2.0`
- add `review_dashboard` schema entry if implemented as packet-level metadata

## CLI / Build Behavior

Phase 1 keeps the current CLI command names:

- `cpapacket retained-earnings`
- `cpapacket distributions`
- `cpapacket contractor-summary`

No behavior change:

- packet builds must still succeed even when some noncritical review files are empty
- missing optional data must produce warnings, not corrupt outputs
- zip creation must remain best-effort

Phase 1 may change the printed labels and PDF titles to CPA-facing wording.

## Test Plan

### Unit Tests

Add or update tests for:

- book-equity bucket extraction excluding distributions, contributions, opening balance equity, and shareholder receivable
- dual distribution calculations: GL basis and BS-change basis
- contributions extraction
- direct equity postings extraction
- shareholder receivable activity extraction
- new CSV row writers and PDF section generation
- contractor output column changes and note text

### Integration Tests

Add fixture-driven tests that build the redesigned artifacts from representative payloads:

- clean S-corp style books with matching GL/BS distributions
- books with GL vs BS distribution mismatch
- books with shareholder receivable balance
- books with direct equity postings
- books with negative ending book equity

Assertions:

- expected files exist
- metadata lists every artifact
- input fingerprint changes when source values change
- validation passes without false missing-artifact errors

### Regression Tests

Protect:

- existing packet build flow
- zip creation
- warning-only behavior when optional deliverables are absent
- Decimal-only money handling

## Implementation Order

1. Add reusable equity classification helpers in `reconciliation/retained_earnings.py` or a new equity-review helper module.
2. Redesign the `retained_earnings` deliverable outputs and writers around the new equity-review artifacts.
3. Expand the `distributions` deliverable with activity and BS bridge artifacts.
4. Reframe the contractor deliverable output labels and columns.
5. Add the packet-level review dashboard.
6. Update README command/output documentation if needed.
7. Run quality gates:
   - `ruff check .`
   - `ruff format --check .`
   - `mypy --strict`
   - `pytest -q`

## Execution Recommendation

Use beads for implementation tracking, but a swarm is optional.

Recommended bead breakdown:

1. `equity classification + retained_earnings redesign`
2. `distributions bridge outputs`
3. `contractor review output relabeling`
4. `review dashboard + docs + tests`

Recommended staffing:

- one agent can do this sequentially
- use swarm only if parallelizing the four beads above
- do not use swarm just to draft the spec

Reason:

- the work is moderately broad but structurally separable
- the highest coordination risk is overlapping edits in `retained_earnings.py`, related tests, and `.beads/issues.jsonl`
- since those files already have concurrent changes in this workspace, swarm only makes sense if reservations and bead ownership are explicit

## Acceptance Criteria

The redesign is successful when a CPA can open the packet and, without reading code:

- tie net income to QBO
- tie ending equity presentation to QBO
- see both GL and BS-change distribution numbers when they differ
- identify contributions separately from distributions
- see shareholder receivable activity clearly
- see direct equity postings clearly
- understand that the package is book-equity review, not shareholder basis or AAA
- use the contractor schedule as a review workpaper rather than a final filing output
