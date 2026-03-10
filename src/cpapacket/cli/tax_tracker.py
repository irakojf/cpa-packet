"""CLI wrapper for estimated-tax tracker setup."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast

import click
from rich.console import Console
from rich.table import Table

from cpapacket.core.context import RunContext
from cpapacket.models.tax import EstimatedTaxPayment, Jurisdiction
from cpapacket.packet.tax_tracker import initialize_tax_tracker
from cpapacket.tax_tracker import TaxTrackerStorage


def _coerce_money(raw_value: str) -> Decimal:
    text = raw_value.strip()
    if not text:
        raise click.ClickException("Amount must not be blank.")
    try:
        amount = Decimal(text)
    except Exception as exc:
        raise click.ClickException(f"Invalid decimal amount: {raw_value}") from exc
    if amount < Decimal("0"):
        raise click.ClickException("Amount must be >= 0.")
    return amount.quantize(Decimal("0.01"))


def register_tax_tracker_command(cli_group: click.Group) -> None:
    """Register `cpapacket tax` commands on the provided click group."""

    @cli_group.group("tax", invoke_without_command=False, no_args_is_help=False)
    def tax_group() -> None:
        """Estimated tax tracker commands."""

    @tax_group.command("init")
    @click.option("--year", type=int, default=None, help="Tax year to initialize.")
    @click.option(
        "--federal-quarterly",
        type=str,
        default=None,
        help="Default Federal quarterly estimated payment amount.",
    )
    @click.option(
        "--ny-quarterly",
        type=str,
        default=None,
        help="Default NY quarterly estimated payment amount.",
    )
    @click.option(
        "--de-franchise",
        type=str,
        default=None,
        help="Default Delaware franchise tax payment amount.",
    )
    @click.option(
        "--overwrite",
        is_flag=True,
        help="Overwrite existing tax tracker files for the selected year.",
    )
    @click.pass_context
    def tax_init_command(
        ctx: click.Context,
        year: int | None,
        federal_quarterly: str | None,
        ny_quarterly: str | None,
        de_franchise: str | None,
        overwrite: bool,
    ) -> None:
        """Initialize estimated tax tracker defaults under global config."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

        target_year = year if year is not None else run_context.year
        non_interactive = run_context.non_interactive

        federal_amount = _resolve_amount(
            raw=federal_quarterly,
            non_interactive=non_interactive,
            prompt="Federal quarterly estimated payment",
        )
        ny_amount = _resolve_amount(
            raw=ny_quarterly,
            non_interactive=non_interactive,
            prompt="NY quarterly estimated payment",
        )
        de_amount = _resolve_amount(
            raw=de_franchise,
            non_interactive=non_interactive,
            prompt="Delaware franchise tax amount",
        )

        try:
            result = initialize_tax_tracker(
                year=target_year,
                federal_quarterly_amount=federal_amount,
                ny_quarterly_amount=ny_amount,
                de_franchise_amount=de_amount,
                overwrite=overwrite,
            )
        except FileExistsError as exc:
            if non_interactive:
                raise click.ClickException(str(exc)) from exc
            if not click.confirm(f"{exc}\nOverwrite existing files?", default=False):
                raise click.ClickException(
                    "Aborted by user; existing files were left unchanged."
                ) from exc
            result = initialize_tax_tracker(
                year=target_year,
                federal_quarterly_amount=federal_amount,
                ny_quarterly_amount=ny_amount,
                de_franchise_amount=de_amount,
                overwrite=True,
            )

        click.echo(f"Initialized estimated tax tracker for {target_year}.")
        click.echo(f"Payments: {result.payment_count} -> {result.tracker_path}")
        click.echo(f"Deadlines: {result.deadline_count} -> {result.deadlines_path}")

    @tax_group.command("mark-paid")
    @click.option(
        "--jurisdiction",
        type=click.Choice(("DE", "NY", "Federal"), case_sensitive=False),
        required=True,
        help="Tax jurisdiction to mark paid.",
    )
    @click.option(
        "--due",
        type=str,
        required=True,
        help="Due date in MM/DD/YY format.",
    )
    @click.option(
        "--paid-date",
        type=str,
        default=None,
        help="Optional paid date in MM/DD/YY format. Defaults to today (UTC).",
    )
    @click.option("--year", type=int, default=None, help="Tax year file to update.")
    @click.pass_context
    def tax_mark_paid_command(
        ctx: click.Context,
        jurisdiction: str,
        due: str,
        paid_date: str | None,
        year: int | None,
    ) -> None:
        """Mark a specific tax payment as paid in persistent tracker storage."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

        target_year = year if year is not None else run_context.year
        due_date = _parse_short_date(due, field_name="due")
        effective_paid_date = (
            _parse_short_date(paid_date, field_name="paid-date")
            if paid_date is not None
            else datetime.now(UTC).date()
        )

        storage = TaxTrackerStorage()
        payments = storage.load_payments(year=target_year)

        updated = _mark_payment_paid(
            payments=payments,
            jurisdiction=cast(Jurisdiction, jurisdiction),
            due_date=due_date,
            paid_date=effective_paid_date,
        )
        if not updated:
            raise click.ClickException(
                f"No payment found for jurisdiction={jurisdiction} due={due_date.isoformat()} "
                f"in tax_tracker_{target_year}.json"
            )

        storage.save_payments(year=target_year, payments=payments)
        click.echo(
            "Marked payment as paid: "
            f"jurisdiction={jurisdiction} due={due_date.isoformat()} "
            f"paid_date={effective_paid_date.isoformat()} year={target_year}"
        )

    @tax_group.command("update")
    @click.option("--year", type=int, default=None, help="Tax year file to update.")
    @click.option(
        "--jurisdiction",
        type=click.Choice(("DE", "NY", "Federal"), case_sensitive=False),
        default=None,
        help="Tax jurisdiction for the payment entry to update.",
    )
    @click.option(
        "--due",
        type=str,
        default=None,
        help="Current due date in MM/DD/YY for the payment entry to update.",
    )
    @click.option("--amount", type=str, default=None, help="Updated amount for the payment entry.")
    @click.option(
        "--status",
        type=click.Choice(("paid", "not_paid"), case_sensitive=False),
        default=None,
        help="Updated status for the payment entry.",
    )
    @click.option(
        "--paid-date",
        type=str,
        default=None,
        help="Updated paid date in MM/DD/YY (or blank when status=not_paid).",
    )
    @click.option(
        "--new-due",
        type=str,
        default=None,
        help="Updated due date in MM/DD/YY for the payment entry.",
    )
    @click.pass_context
    def tax_update_command(
        ctx: click.Context,
        year: int | None,
        jurisdiction: str | None,
        due: str | None,
        amount: str | None,
        status: str | None,
        paid_date: str | None,
        new_due: str | None,
    ) -> None:
        """Update an existing tax payment entry in persistent tracker storage."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

        target_year = year if year is not None else run_context.year
        target_jurisdiction = jurisdiction
        if target_jurisdiction is None:
            if run_context.non_interactive:
                raise click.ClickException("--jurisdiction is required in non-interactive mode.")
            target_jurisdiction = click.prompt(
                "Jurisdiction",
                type=click.Choice(("DE", "NY", "Federal"), case_sensitive=False),
            )

        due_raw = due
        if due_raw is None:
            if run_context.non_interactive:
                raise click.ClickException("--due is required in non-interactive mode.")
            due_raw = click.prompt("Current due date (MM/DD/YY)", type=str)

        due_date = _parse_short_date(due_raw, field_name="due")
        updated_due_date = (
            _parse_short_date(new_due, field_name="new-due") if new_due is not None else due_date
        )

        updated_amount: Decimal | None = _coerce_money(amount) if amount is not None else None
        updated_status = cast(str | None, status)
        updated_paid_date = (
            _parse_short_date(paid_date, field_name="paid-date")
            if paid_date is not None and paid_date.strip()
            else None
        )

        storage = TaxTrackerStorage()
        payments = storage.load_payments(year=target_year)
        updated = _update_payment(
            payments=payments,
            jurisdiction=cast(Jurisdiction, target_jurisdiction),
            due_date=due_date,
            amount=updated_amount,
            status=cast(str | None, updated_status),
            paid_date=updated_paid_date,
            new_due_date=updated_due_date,
        )
        if not updated:
            raise click.ClickException(
                f"No payment found for jurisdiction={target_jurisdiction} "
                f"due={due_date.isoformat()} "
                f"in tax_tracker_{target_year}.json"
            )

        storage.save_payments(year=target_year, payments=payments)
        click.echo(
            "Updated payment: "
            f"jurisdiction={target_jurisdiction} due={due_date.isoformat()} "
            f"year={target_year}"
        )

    @tax_group.command("status")
    @click.option("--year", type=int, default=None, help="Tax year file to inspect.")
    @click.pass_context
    def tax_status_command(ctx: click.Context, year: int | None) -> None:
        """Render estimated-tax payment status dashboard."""
        run_context = cast(RunContext | None, ctx.obj.get("run_context") if ctx.obj else None)
        if run_context is None:
            raise click.ClickException("Run context is not initialized.")

        target_year = year if year is not None else run_context.year
        storage = TaxTrackerStorage()
        payments = storage.load_payments(year=target_year)
        if not payments:
            click.echo(f"No tax tracker payments found for {target_year}.")
            return

        click.echo(_render_status_table(payments))


def _resolve_amount(*, raw: str | None, non_interactive: bool, prompt: str) -> Decimal:
    if raw is not None:
        return _coerce_money(raw)
    if non_interactive:
        return Decimal("0.00")
    prompted = click.prompt(prompt, type=str, default="0.00")
    return _coerce_money(prompted)


def _parse_short_date(raw: str, *, field_name: str) -> date:
    text = raw.strip()
    if not text:
        raise click.ClickException(f"{field_name} must not be blank.")
    try:
        return datetime.strptime(text, "%m/%d/%y").date()
    except ValueError as exc:
        raise click.ClickException(
            f"Invalid {field_name} value '{raw}'. Expected MM/DD/YY."
        ) from exc


def _mark_payment_paid(
    *,
    payments: list[EstimatedTaxPayment],
    jurisdiction: Jurisdiction,
    due_date: date,
    paid_date: date,
) -> bool:
    updated_any = False
    for idx, payment in enumerate(payments):
        if payment.jurisdiction != jurisdiction or payment.due_date != due_date:
            continue
        payments[idx] = payment.model_copy(
            update={
                "status": "paid",
                "paid_date": paid_date,
                "last_updated": datetime.now(UTC),
            }
        )
        updated_any = True
    return updated_any


def _update_payment(
    *,
    payments: list[EstimatedTaxPayment],
    jurisdiction: Jurisdiction,
    due_date: date,
    amount: Decimal | None,
    status: str | None,
    paid_date: date | None,
    new_due_date: date,
) -> bool:
    updated_any = False
    for idx, payment in enumerate(payments):
        if payment.jurisdiction != jurisdiction or payment.due_date != due_date:
            continue
        next_status = cast(str, status or payment.status)
        next_paid_date = (
            paid_date if status == "paid" or paid_date is not None else payment.paid_date
        )
        if next_status == "not_paid":
            next_paid_date = None
        payments[idx] = payment.model_copy(
            update={
                "amount": amount if amount is not None else payment.amount,
                "status": next_status,
                "paid_date": next_paid_date,
                "due_date": new_due_date,
                "last_updated": datetime.now(UTC),
            }
        )
        updated_any = True
    return updated_any


def _render_status_table(payments: list[EstimatedTaxPayment]) -> str:
    table = Table(title="Estimated Tax Payment Status", show_lines=False)
    table.add_column("Jurisdiction")
    table.add_column("Due Date")
    table.add_column("Amount", justify="right")
    table.add_column("Status")

    today = _today_utc_date()
    ordered = sorted(payments, key=lambda item: (item.due_date, item.jurisdiction))
    for payment in ordered:
        label = _status_label(payment=payment, today=today)
        table.add_row(
            payment.jurisdiction,
            payment.due_date.isoformat(),
            f"{payment.amount:.2f}",
            label,
        )

    console = Console(record=True, force_terminal=False, color_system=None, width=120)
    console.print(table)
    return console.export_text(clear=False).rstrip()


def _status_label(*, payment: EstimatedTaxPayment, today: date) -> str:
    if payment.status == "paid":
        return "[green]PAID[/green]"
    if payment.due_date < today:
        return "[red]PAST DUE[/red]"
    return "[yellow]UPCOMING[/yellow]"


def _today_utc_date() -> date:
    return datetime.now(UTC).date()
