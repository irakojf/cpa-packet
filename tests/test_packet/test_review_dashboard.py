from __future__ import annotations

import json
from pathlib import Path

import pytest

from cpapacket.packet.review_dashboard import write_review_dashboard


def test_write_review_dashboard_creates_markdown_pdf_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_write_report(self, output_path, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4\n%stub\n")
        return path

    monkeypatch.setattr(
        "cpapacket.packet.review_dashboard.PdfWriter.write_report",
        _fake_write_report,
    )

    pnl_dir = tmp_path / "01_Year-End_Profit_and_Loss" / "cpa"
    pnl_dir.mkdir(parents=True)
    (pnl_dir / "Profit_and_Loss_2025.csv").write_text(
        "section,level,row_type,label,amount,path\n"
        "Uncategorized,0,total,Net Income,123.45,Net Income\n",
        encoding="utf-8",
    )

    bs_dir = tmp_path / "02_Year-End_Balance_Sheet" / "cpa"
    bs_dir.mkdir(parents=True)
    (bs_dir / "Balance_Sheet_2025-12-31.csv").write_text(
        "section,level,row_type,label,amount,path\n"
        "Assets,3,account,Operating Checking,1000.00,"
        "ASSETS > Current Assets > Bank Accounts > Operating Checking\n"
        "Assets,3,account,Shareholder Receivable,50.00,"
        "ASSETS > Current Assets > Other Current Assets > Shareholder Receivable\n"
        "Liabilities,1,total,Total Liabilities,400.00,LIABILITIES > Total Liabilities\n",
        encoding="utf-8",
    )

    equity_dir = tmp_path / "09_Retained_Earnings_Rollforward" / "cpa"
    equity_dir.mkdir(parents=True)
    (equity_dir / "Book_Equity_Rollforward_2025.csv").write_text(
        "year,beginning_book_equity_bucket,current_year_net_income,current_year_distributions_gl,"
        "current_year_distributions_bs_change,current_year_contributions,other_direct_equity_postings,"
        "expected_ending_book_equity_bucket_gl_basis,expected_ending_book_equity_bucket_bs_basis,"
        "actual_ending_book_equity_bucket,gl_basis_difference,bs_basis_difference,status,flags,"
        "miscoded_distribution_count\n"
        "2025,100.00,123.45,50.00,60.00,25.00,0.00,198.45,188.45,198.45,0.00,-10.00,Review,"
        "distributions_gl_vs_bs_mismatch|shareholder_receivable_present,0\n",
        encoding="utf-8",
    )

    contractor_dir = tmp_path / "07_Contractor_1099_Summary" / "cpa"
    contractor_dir.mkdir(parents=True)
    (contractor_dir / "Contractor_1099_Review_2025.csv").write_text(
        "vendor_id,vendor_name,tax_id_on_file,total_paid,card_processor_total,non_card_total,"
        "selected_source_accounts,threshold,flagged_for_1099_review,review_note,requires_1099_review,flags\n"
        "abc123,Alpha LLC,false,700.00,0.00,700.00,Contract Labor,600.00,true,"
        "Meets non-card threshold; CPA review required.,true,requires_1099_review\n",
        encoding="utf-8",
    )

    markdown_path, pdf_path = write_review_dashboard(output_root=tmp_path, year=2025)

    assert markdown_path == tmp_path / "00_REVIEW_DASHBOARD.md"
    assert pdf_path == tmp_path / "00_REVIEW_DASHBOARD.pdf"
    assert markdown_path.exists()
    assert pdf_path.exists()

    markdown = markdown_path.read_text(encoding="utf-8")
    assert "- Net Income: 123.45" in markdown
    assert "- Ending Cash: 1000.00" in markdown
    assert "- Shareholder Receivable Ending Balance: 50.00" in markdown
    assert "- Contractor/1099 Review Count: 1" in markdown
    assert "distributions_gl_vs_bs_mismatch" in markdown

    metadata_path = (
        tmp_path / "_meta" / "private" / "deliverables" / "review_dashboard_metadata.json"
    )
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload["deliverable"] == "review_dashboard"
    assert payload["schema_versions"] == {"md": "1.0", "pdf": "1.0"}
