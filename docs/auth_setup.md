# Authentication Setup (QBO + Gusto)

This guide walks through OAuth setup for `cpapacket`.

- QuickBooks Online (QBO) auth is required for QBO-backed commands and full builds.
- Gusto auth is optional; payroll-dependent workflows auto-skip if Gusto is not configured.

## 1. Prerequisites

- Python 3.11+
- `cpapacket` installed and available on PATH
- Access to:
  - Intuit Developer portal (for QBO app credentials)
  - Gusto developer app credentials (if using payroll workflows)

## 2. Configure Environment Variables

Set these before running auth/login commands.

### QBO (required)

## 2a. Applying for QuickBooks Production API Keys

To access real QuickBooks Online company data, `cpapacket` requires production API credentials from the Intuit Developer Portal. Production keys are issued after completing the Intuit App Assessment questionnaire.

---

### Required Integration URLs

During the application process you must provide several public URLs describing the integration. These pages exist solely for Intuit review and compliance.

`cpapacket` uses the following structure:

| Field | URL |
|---|---|
| Host domain | `apps.shapeshift.so` |
| Launch URL | `https://apps.shapeshift.so/cpapacket/connect/` |
| Connect / Reconnect URL | `https://apps.shapeshift.so/cpapacket/connect/` |
| Disconnect URL | `https://apps.shapeshift.so/cpapacket/disconnect/` |
| Privacy Policy | `https://apps.shapeshift.so/cpapacket/privacy/` |
| Terms / EULA | `https://apps.shapeshift.so/cpapacket/terms/` |

These pages explain the integration and provide support contact information.

---

### Integration Description

Use wording similar to the following in the Intuit developer portal:

> `cpapacket` is a private internal CLI tool used to generate CPA-ready reporting packets from QuickBooks Online data. The integration reads financial reports such as Profit & Loss, Balance Sheet, and General Ledger through the QuickBooks Accounting API and produces internal reports used for accounting review and tax preparation.

---

### Recommended Questionnaire Responses

The following answers were used successfully for approval.

| Question | Answer |
|---|---|
| App type | Private app |
| Platform | Desktop app connecting to QuickBooks Online |
| Data access | Reads data from Intuit products |
| API category | Accounting API |
| Call frequency | Seasonally (1–10 times per year) |
| QuickBooks versions supported | Simple Start, Essentials, Plus, Advanced |

---

### What Intuit Reviewers Check

During review, Intuit typically verifies that:

1. The privacy policy and terms pages load successfully
2. The launch URL explains the integration
3. The integration clearly relates to QuickBooks Online
4. The application does not sell or redistribute financial data
5. A support contact email is visible

Most reviews complete in **1–3 days** if the pages load correctly and the integration description is clear.


```bash
export CPAPACKET_QBO_CLIENT_ID="..."
export CPAPACKET_QBO_CLIENT_SECRET="..."
export CPAPACKET_QBO_REDIRECT_URI="https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl"
export CPAPACKET_QBO_REALM_ID="..." # this is the same as the company id in Quickbooks online and can be found under Settings > Additional Info
# Optional: set this for sandbox companies
# export CPAPACKET_QBO_API_BASE_URL="https://sandbox-quickbooks.api.intuit.com/v3/company"
```

### Gusto (optional)

```bash
export CPAPACKET_GUSTO_CLIENT_ID="..."
export CPAPACKET_GUSTO_CLIENT_SECRET="..."
export CPAPACKET_GUSTO_REDIRECT_URI="http://localhost:8000/callback"
```

## 3. QBO Login Flow

### Production redirect URI

Intuit production apps require HTTPS redirect URIs and do not allow `localhost`. Use the
Intuit OAuth2 Playground redirect URL as your registered redirect URI:

```
https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl
```

Add this URI under **Keys & credentials > Production > Redirect URIs** in the
[Intuit Developer portal](https://developer.intuit.com). Then set your env var to match:

```bash
export CPAPACKET_QBO_REDIRECT_URI="https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl"
```

After authorizing, the browser will redirect to the Playground page with the auth code and
realm ID visible in the URL. Copy those values for the token exchange step below.

### Start OAuth

```bash
cpapacket auth qbo login --state my-state
```

The command prints:

- an authorization URL to open in your browser
- a PKCE code verifier for token exchange

After the provider callback returns an auth code, exchange it:

```bash
cpapacket auth qbo login \
  --code "<AUTH_CODE>" \
  --code-verifier "<VERIFIER>" \
  --realm-id "<REALM_ID>"
```

Check status:

```bash
cpapacket auth qbo status
```

Log out (clear stored token):

```bash
cpapacket auth qbo logout
```

## 4. Gusto Login Flow (Optional)

Start OAuth:

```bash
cpapacket auth gusto login --state my-state
```

After callback, exchange code:

```bash
cpapacket auth gusto login \
  --code "<AUTH_CODE>" \
  --code-verifier "<VERIFIER>"
```

Check status:

```bash
cpapacket auth gusto status
```

Log out:

```bash
cpapacket auth gusto logout
```

## 5. Verify End-to-End

Run a lightweight command using authenticated providers:

```bash
cpapacket --year 2025 check
```

Or run full generation:

```bash
cpapacket --year 2025 --non-interactive build
```

## 6. Troubleshooting

### Missing environment variable errors

If you see:

- `Missing required environment variable: CPAPACKET_QBO_CLIENT_ID`
- `Missing required environment variable: CPAPACKET_GUSTO_CLIENT_ID`

Export the required values and rerun the command.

### QBO login requires code verifier

If you see:

- `--code-verifier is required when --code is provided`

Use the verifier printed in the initial login command output.

### QBO connectivity fails with 403

This usually indicates company/app environment mismatch:

- Ensure `CPAPACKET_QBO_REALM_ID` matches the authorized company from the OAuth callback.
- If using a sandbox company, set:

```bash
export CPAPACKET_QBO_API_BASE_URL="https://sandbox-quickbooks.api.intuit.com/v3/company"
```

Then re-run login and doctor:

```bash
cpapacket auth qbo login --state my-state
cpapacket doctor
```

### Gusto is not configured/authenticated

`cpapacket` treats Gusto as optional. Commands may report payroll deliverables as skipped when
Gusto credentials/token are unavailable.

### Non-interactive mode conflicts

In non-interactive mode, `--on-conflict prompt` is invalid. Use one of:

- `--on-conflict abort`
- `--on-conflict overwrite`
- `--on-conflict copy`