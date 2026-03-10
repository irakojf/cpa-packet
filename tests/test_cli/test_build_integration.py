from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from click.testing import CliRunner

from cpapacket.cli.main import cli
from cpapacket.core.metadata import (
    DeliverableMetadata,
    compute_input_fingerprint,
    write_deliverable_metadata,
)
from cpapacket.deliverables.registry import DELIVERABLE_REGISTRY
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS


class _HttpQboClient:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        return self._client.request(method, endpoint, params=params, json=json_body)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _stub_pdf_writer(
    self: object,
    output_path: str | Path,
    *,
    company_name: str,
    report_title: str,
    date_range_label: str,
    body_lines: list[Any],
) -> Path:
    del self, company_name, report_title, date_range_label, body_lines
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(b"%PDF-1.4\n")
    return destination


class _IntegrationPnlDeliverable:
    key = "pnl"
    folder = DELIVERABLE_FOLDERS["pnl"]
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(self, ctx: Any, providers: Any, prompts: dict[str, Any]) -> Any:
        del prompts
        payload = providers.get_pnl(ctx.year, ctx.method)
        deliverable_dir = ctx.out_dir / self.folder
        deliverable_dir.mkdir(parents=True, exist_ok=True)
        meta_dir = ctx.out_dir / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)

        csv_path = deliverable_dir / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.csv"
        pdf_path = deliverable_dir / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.pdf"
        raw_path = deliverable_dir / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual_raw.json"
        metadata_path = meta_dir / "pnl_metadata.json"

        csv_path.write_text("section,level,row_type,label,amount,path\n", encoding="utf-8")
        _stub_pdf_writer(
            self,
            pdf_path,
            company_name="Acme LLC",
            report_title="Profit and Loss",
            date_range_label="2025-01-01 to 2025-12-31",
            body_lines=[],
        )
        raw_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        metadata_inputs = {
            "year": ctx.year,
            "method": ctx.method,
        }
        metadata = DeliverableMetadata(
            deliverable=self.key,
            inputs=metadata_inputs,
            input_fingerprint=compute_input_fingerprint(metadata_inputs),
            schema_versions=SCHEMA_VERSIONS[self.key],
            artifacts=[
                csv_path.relative_to(ctx.out_dir).as_posix(),
                pdf_path.relative_to(ctx.out_dir).as_posix(),
                raw_path.relative_to(ctx.out_dir).as_posix(),
            ],
        )
        write_deliverable_metadata(metadata_path, metadata)

        from cpapacket.deliverables.base import DeliverableResult

        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(csv_path), str(pdf_path), str(raw_path)],
        )


class _SkipOnlyDeliverable:
    def __init__(self, *, key: str, folder: str, required: bool, requires_gusto: bool) -> None:
        self.key = key
        self.folder = folder
        self.required = required
        self.dependencies: list[str] = []
        self.requires_gusto = requires_gusto

    def gather_prompts(self, _ctx: Any) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: Any) -> bool:
        return False

    def generate(self, ctx: Any, providers: Any, prompts: dict[str, Any]) -> Any:
        del ctx, providers, prompts
        raise AssertionError(f"{self.key} should have been skipped in this test flow")


def test_build_full_flow_with_mocked_http_writes_outputs_manifest_and_zip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    fixture_path = _repo_root() / "tests" / "fixtures" / "qbo" / "profit_and_loss_annual_2025.json"
    fixture_payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    monkeypatch.setattr("cpapacket.cli.build._ensure_qbo_authenticated", lambda: None)
    monkeypatch.setattr("cpapacket.cli.build._build_optional_gusto_client", lambda: None)
    synthetic_ordered = [_IntegrationPnlDeliverable()]
    synthetic_ordered.extend(
        _SkipOnlyDeliverable(
            key=item.key,
            folder=item.folder,
            required=item.required,
            requires_gusto=item.requires_gusto,
        )
        for item in DELIVERABLE_REGISTRY
        if item.key != "pnl"
    )
    monkeypatch.setattr("cpapacket.cli.build.get_ordered_registry", lambda: synthetic_ordered)

    with httpx.Client(base_url="https://api.example.test") as http_client:
        monkeypatch.setattr(
            "cpapacket.cli.build._build_qbo_client",
            lambda: _HttpQboClient(http_client),
        )

        with respx.mock(assert_all_called=True) as router:
            pnl_route = router.get("https://api.example.test/reports/ProfitAndLoss").mock(
                return_value=httpx.Response(200, json=fixture_payload)
            )

            skip_keys = [item.key for item in DELIVERABLE_REGISTRY if item.key != "pnl"]
            args = ["--year", "2025", "--non-interactive", "build"]
            for key in skip_keys:
                args.extend(["--skip", key])

            result = runner.invoke(cli, args)

    assert result.exit_code == 0, result.output
    assert pnl_route.call_count == 1
    assert "Build complete." in result.output

    packet_dir = tmp_path
    pnl_dir = packet_dir / "01_Year-End_Profit_and_Loss"
    csv_path = pnl_dir / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.csv"
    pdf_path = pnl_dir / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual.pdf"
    raw_path = pnl_dir / "Profit_and_Loss_2025-01-01_to_2025-12-31_accrual_raw.json"
    metadata_path = packet_dir / "_meta" / "pnl_metadata.json"
    manifest_path = packet_dir / "_meta" / "public" / "packet_manifest.json"
    validation_report_path = packet_dir / "_meta" / "public" / "validation_report.txt"
    summary_path = packet_dir / "00_PACKET_SUMMARY.md"
    zip_path = packet_dir.parent / f"{packet_dir.name}.zip"

    assert csv_path.exists()
    assert pdf_path.exists()
    assert raw_path.exists()
    assert metadata_path.exists()
    assert manifest_path.exists()
    assert validation_report_path.exists()
    assert summary_path.exists()
    assert zip_path.exists()

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    deliverables = {entry["key"]: entry for entry in manifest_payload["deliverables"]}
    assert deliverables["pnl"]["status"] == "success"
    for key in skip_keys:
        assert deliverables[key]["status"] == "skipped"

    metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata_payload["deliverable"] == "pnl"
    assert csv_path.relative_to(packet_dir).as_posix() in metadata_payload["artifacts"]
    assert pdf_path.relative_to(packet_dir).as_posix() in metadata_payload["artifacts"]

    report_text = validation_report_path.read_text(encoding="utf-8")
    assert "Review Required: NO" in report_text
