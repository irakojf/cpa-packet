from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from cpapacket.models.contractor import ContractorRecord


def test_contractor_record_happy_path_quantizes_and_normalizes() -> None:
    record = ContractorRecord(
        vendor_id=" ven-001 ",
        display_name=" ACME Contracting ",
        tax_id_on_file=True,
        total_paid=Decimal("12000.125"),
        card_processor_total=Decimal("2000.126"),
        non_card_total=Decimal("10000.124"),
        requires_1099_review=True,
        flags=[" missing_tin ", " ", "threshold_exceeded"],
    )

    assert record.vendor_id == "ven-001"
    assert record.display_name == "ACME Contracting"
    assert record.total_paid == Decimal("12000.13")
    assert record.card_processor_total == Decimal("2000.13")
    assert record.non_card_total == Decimal("10000.12")
    assert record.flags == ["missing_tin", "threshold_exceeded"]


def test_contractor_record_rejects_blank_ids_and_negative_amounts() -> None:
    with pytest.raises(ValidationError):
        ContractorRecord(
            vendor_id=" ",
            display_name="ACME",
            tax_id_on_file=False,
            total_paid=Decimal("1.00"),
            card_processor_total=Decimal("0.00"),
            non_card_total=Decimal("1.00"),
            requires_1099_review=False,
            flags=[],
        )

    with pytest.raises(ValidationError):
        ContractorRecord(
            vendor_id="ven-2",
            display_name="ACME",
            tax_id_on_file=False,
            total_paid=Decimal("-1.00"),
            card_processor_total=Decimal("0.00"),
            non_card_total=Decimal("1.00"),
            requires_1099_review=False,
            flags=[],
        )


def test_contractor_record_defaults_flags_to_empty_list() -> None:
    record = ContractorRecord(
        vendor_id="ven-3",
        display_name="Vendor 3",
        tax_id_on_file=False,
        total_paid=Decimal("0"),
        card_processor_total=Decimal("0"),
        non_card_total=Decimal("0"),
        requires_1099_review=False,
    )

    assert record.flags == []
