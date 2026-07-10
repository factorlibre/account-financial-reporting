# Copyright 2020 ForgeFlow S.L. (https://www.forgeflow.com)
# Copyright 2025 Tecnativa - Carlos Dauden
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

from odoo import api, models
from odoo.tools import split_every

# Default batch size for chunked move-line processing.
DEFAULT_REPORT_CHUNK = 2000
# Floor: a misconfigured chunk size (e.g. 1) must not degrade the report into
# one query per line. The batch size is clamped to at least this value.
MIN_REPORT_CHUNK = 100


class AgedPartnerBalanceReport(models.AbstractModel):
    _name = "report.account_financial_report.abstract_report"
    _description = "Abstract Report"
    COMMON_ML_FIELDS = [
        "account_id",
        "partner_id",
        "journal_id",
        "date",
        "ref",
        "id",
        "move_id",
        "name",
    ]

    @api.model
    def _get_move_lines_domain_not_reconciled(
        self, company_id, account_ids, partner_ids, only_posted_moves, date_from
    ):
        domain = [
            ("account_id", "in", account_ids),
            ("company_id", "=", company_id),
            ("reconciled", "=", False),
        ]
        if partner_ids:
            domain += [("partner_id", "in", partner_ids)]
        if only_posted_moves:
            domain += [("move_id.state", "=", "posted")]
        else:
            domain += [("move_id.state", "in", ["posted", "draft"])]
        if date_from:
            domain += [("date", ">", date_from)]
        return domain

    @api.model
    def _get_new_move_lines_domain(
        self, new_ml_ids, account_ids, company_id, partner_ids, only_posted_moves
    ):
        domain = [
            ("account_id", "in", account_ids),
            ("company_id", "=", company_id),
            ("id", "in", new_ml_ids),
        ]
        if partner_ids:
            domain += [("partner_id", "in", partner_ids)]
        if only_posted_moves:
            domain += [("move_id.state", "=", "posted")]
        else:
            domain += [("move_id.state", "in", ["posted", "draft"])]
        return domain

    def _recalculate_move_lines(
        self,
        move_lines,
        debit_ids,
        credit_ids,
        debit_amount,
        credit_amount,
        ml_ids,
        account_ids,
        company_id,
        partner_ids,
        only_posted_moves,
        debit_amount_currency,
        credit_amount_currency,
    ):
        debit_ids = set(debit_ids)
        credit_ids = set(credit_ids)
        in_credit_but_not_in_debit = credit_ids - debit_ids
        reconciled_ids = list(debit_ids) + list(in_credit_but_not_in_debit)
        reconciled_ids = set(reconciled_ids)
        ml_ids = set(ml_ids)
        new_ml_ids = reconciled_ids - ml_ids
        new_ml_ids = list(new_ml_ids)
        new_domain = self._get_new_move_lines_domain(
            new_ml_ids, account_ids, company_id, partner_ids, only_posted_moves
        )
        company_currency = self.env["res.company"].browse(company_id).currency_id
        ml_fields = self._get_ml_fields()
        new_move_lines = self.env["account.move.line"].search_read(
            domain=new_domain, fields=ml_fields
        )
        move_lines = move_lines + new_move_lines
        for move_line in move_lines:
            ml_id = move_line["id"]
            if ml_id in debit_ids:
                if move_line.get("amount_residual", False):
                    move_line["amount_residual"] += debit_amount[ml_id]
                else:
                    move_line["amount_residual"] = debit_amount[ml_id]
                if move_line.get("amount_residual_currency", False):
                    move_line["amount_residual_currency"] += debit_amount_currency[
                        ml_id
                    ]
                else:
                    move_line["amount_residual_currency"] = debit_amount_currency[ml_id]
            if ml_id in credit_ids:
                if move_line.get("amount_residual", False):
                    move_line["amount_residual"] -= credit_amount[ml_id]
                else:
                    move_line["amount_residual"] = -credit_amount[ml_id]
                if move_line.get("amount_residual_currency", False):
                    move_line["amount_residual_currency"] -= credit_amount_currency[
                        ml_id
                    ]
                else:
                    move_line["amount_residual_currency"] = -credit_amount_currency[
                        ml_id
                    ]
            # Set amount_currency=0 to keep the same behaviour as in v13
            # Conditions: if there is no curency_id defined or it is equal
            # to the company's curency_id
            if "amount_currency" in move_line and (
                "currency_id" not in move_line
                or move_line["currency_id"] == company_currency.id
            ):
                move_line["amount_currency"] = 0
        return move_lines

    def _get_accounts_data(self, accounts_ids):
        accounts = self.env["account.account"].browse(accounts_ids)
        accounts_data = {}
        for account in accounts:
            accounts_data.update(
                {
                    account.id: {
                        "id": account.id,
                        "code": account.code,
                        "name": account.name,
                        "hide_account": False,
                        "group_id": account.group_id.id,
                        "currency_id": account.currency_id.id,
                        "currency_name": account.currency_id.name,
                        "centralized": account.centralized,
                    }
                }
            )
        return accounts_data

    def _get_journals_data(self, journals_ids):
        journals = self.env["account.journal"].browse(journals_ids)
        journals_data = {}
        for journal in journals:
            journals_data.update({journal.id: {"id": journal.id, "code": journal.code}})
        return journals_data

    def _get_ml_fields(self):
        return self.COMMON_ML_FIELDS + [
            "amount_residual",
            "reconciled",
            "currency_id",
            "credit",
            "date_maturity",
            "amount_residual_currency",
            "debit",
            "amount_currency",
        ]

    def _report_line_chunk_size(self):
        """Batch size for chunked move-line processing.

        Configurable via the ``account_financial_report.report_chunk_size``
        system parameter; clamped to a minimum floor so a misconfigured value
        cannot turn the report into one query per line.
        """
        raw = (
            self.env["ir.config_parameter"]
            .sudo()
            .get_param(
                "account_financial_report.report_chunk_size", DEFAULT_REPORT_CHUNK
            )
        )
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = DEFAULT_REPORT_CHUNK
        return max(value, MIN_REPORT_CHUNK)

    def _iter_move_lines(self, move_line_ids, chunk_size=None):
        """Yield ``account.move.line`` records in bounded, cache-invalidated chunks.

        ``move_line_ids`` must be the already-resolved, ordered id list (from a
        single ``search(...).ids``), never the result of ``LIMIT``/``OFFSET``
        pagination over a non-unique ``ORDER BY``: paginating a tied order can
        silently skip or duplicate rows between pages. Each chunk is browsed,
        yielded, and then its ORM cache is invalidated, so the field cache stays
        O(chunk) instead of O(total lines) -- the growth that exhausts worker
        memory on full-year, high-volume reports.
        """
        # prefetch_fields=False: load only the accessed columns per chunk, not
        # every column of the (fat) account.move.line model.
        aml = self.env["account.move.line"].with_context(prefetch_fields=False)
        if chunk_size is None:
            chunk_size = self._report_line_chunk_size()
        for chunk_ids in split_every(chunk_size, move_line_ids):
            # exists() drops lines deleted between the initial search and now
            # (single transaction) so field access on the chunk can't raise.
            chunk = aml.browse(chunk_ids).exists()
            yield chunk
            chunk.invalidate_recordset()

    def _get_report_values(self, docids, data):
        wizard = self.env[data["wizard_name"]].browse(data["wizard_id"])
        res = {f"{c.expression_label}_visible": c.is_visible for c in wizard.column_ids}
        res.update(
            {
                f"{c.expression_label}_limit": c.limit
                for c in wizard.column_ids
                if c.field_type == "string"
            }
        )
        # Pass function to be called in report
        res["limit_text"] = wizard._limit_text
        return res
