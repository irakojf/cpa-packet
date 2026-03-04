from __future__ import annotations

from pathlib import Path

from cpapacket.packet.summary import (
    PacketSummary,
    PacketSummaryDeliverable,
    render_packet_summary,
    write_packet_summary,
)


def test_render_packet_summary_includes_required_sections() -> None:
    summary = PacketSummary(
        tool_version="0.1.0",
        year=2025,
        accounting_method="accrual",
        payroll_available=False,
        deliverables=(
            PacketSummaryDeliverable(key="pnl", status="generated"),
            PacketSummaryDeliverable(
                key="payroll_summary",
                status="skipped",
                reason="Gusto auth unavailable",
            ),
        ),
        validation_warnings=("Missing metadata for balance_sheet",),
        reconciliation_flags=("Undeposited funds balance non-zero",),
        notes=("No secrets are included in this summary.",),
    )

    markdown = render_packet_summary(summary)

    assert "# Packet Summary" in markdown
    assert "- Tool Version: 0.1.0" in markdown
    assert "- Tax Year: 2025" in markdown
    assert "- Accounting Method: accrual" in markdown
    assert "- Payroll Available: No" in markdown
    assert "- pnl: generated" in markdown
    assert "- payroll_summary: skipped (Gusto auth unavailable)" in markdown
    assert "## Validation Warnings" in markdown
    assert "Missing metadata for balance_sheet" in markdown
    assert "## Reconciliation Flags" in markdown
    assert "Undeposited funds balance non-zero" in markdown


def test_render_packet_summary_defaults_empty_sections_to_none() -> None:
    summary = PacketSummary(
        tool_version="0.1.0",
        year=2025,
        accounting_method="cash",
    )

    markdown = render_packet_summary(summary)

    assert "## Deliverables" in markdown
    assert "## Validation Warnings" in markdown
    assert "## Reconciliation Flags" in markdown
    assert markdown.count("- None") >= 3


def test_write_packet_summary_writes_packet_root_markdown(tmp_path: Path) -> None:
    summary = PacketSummary(
        tool_version="0.2.0",
        year=2024,
        accounting_method="cash",
        deliverables=(PacketSummaryDeliverable(key="pnl", status="generated"),),
    )

    output_path = write_packet_summary(output_root=tmp_path, summary=summary)

    assert output_path == tmp_path / "00_PACKET_SUMMARY.md"
    assert output_path.exists()
    content = output_path.read_text(encoding="utf-8")
    assert "Packet Summary" in content
    assert "pnl: generated" in content
