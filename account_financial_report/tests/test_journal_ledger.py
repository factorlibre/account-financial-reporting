# Copyright 2017 ACSONE SA/NV
# Copyright 2019-20 ForgeFlow S.L. (https://www.forgeflow.com)
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).

from datetime import datetime
from unittest.mock import patch

from dateutil.relativedelta import relativedelta

from odoo.fields import Date
from odoo.tests import tagged
from odoo.tests.common import Form

from odoo.addons.account.tests.common import AccountTestInvoicingCommon
from odoo.addons.account_financial_report.report.abstract_report import MIN_REPORT_CHUNK


@tagged("post_install", "-at_install")
class TestJournalReport(AccountTestInvoicingCommon):
    @classmethod
    def setUpClass(cls, chart_template_ref=None):
        super().setUpClass(chart_template_ref=chart_template_ref)
        cls.env = cls.env(
            context=dict(
                cls.env.context,
                mail_create_nolog=True,
                mail_create_nosubscribe=True,
                mail_notrack=True,
                no_reset_password=True,
                tracking_disable=True,
            )
        )
        cls.AccountObj = cls.env["account.account"]
        cls.InvoiceObj = cls.env["account.move"]
        cls.JournalObj = cls.env["account.journal"]
        cls.MoveObj = cls.env["account.move"]
        cls.TaxObj = cls.env["account.tax"]
        cls.JournalLedgerReportWizard = cls.env["journal.ledger.report.wizard"]
        cls.JournalLedgerReport = cls.env[
            "report.account_financial_report.journal_ledger"
        ]
        cls.company = cls.company_data["company"]
        cls.company.account_sale_tax_id = False
        cls.company.account_purchase_tax_id = False
        today = datetime.today()
        last_year = today - relativedelta(years=1)
        cls.previous_fy_date_start = Date.to_string(last_year.replace(month=1, day=1))
        cls.previous_fy_date_end = Date.to_string(last_year.replace(month=12, day=31))
        cls.fy_date_start = Date.to_string(today.replace(month=1, day=1))
        cls.fy_date_end = Date.to_string(today.replace(month=12, day=31))
        cls.receivable_account = cls.company_data["default_account_receivable"]
        cls.income_account = cls.company_data["default_account_revenue"]
        cls.expense_account = cls.company_data["default_account_expense"]
        cls.payable_account = cls.company_data["default_account_payable"]
        cls.journal_sale = cls.company_data["default_journal_sale"]
        cls.journal_purchase = cls.company_data["default_journal_purchase"]
        cls.tax_15_s = cls.company_data["default_tax_sale"]
        cls.tax_15_s.sequence = 30
        cls.tax_15_s.amount = 15.0
        cls.tax_15_s.amount_type = "percent"
        cls.tax_15_s.include_base_amount = False
        cls.tax_15_s.type_tax_use = "sale"
        cls.tax_20_s = cls.tax_15_s.copy(
            {
                "sequence": 30,
                "name": "Tax 20.0% (Percentage of Price)",
                "amount": 20.0,
                "amount_type": "percent",
                "include_base_amount": False,
                "type_tax_use": "sale",
            }
        )
        cls.tax_15_p = cls.company_data["default_tax_purchase"]
        cls.tax_15_p.sequence = 30
        cls.tax_15_p.amount = 15.0
        cls.tax_15_p.amount_type = "percent"
        cls.tax_15_p.include_base_amount = False
        cls.tax_15_p.type_tax_use = "purchase"
        cls.tax_20_p = cls.tax_15_p.copy(
            {
                "sequence": 30,
                "name": "Tax 20.0% (Percentage of Price)",
                "amount": 20.0,
                "amount_type": "percent",
                "include_base_amount": False,
                "type_tax_use": "purchase",
            }
        )
        cls.partner_2 = cls.env.ref("base.res_partner_2")

    def _add_move(
        self,
        date,
        journal,
        receivable_debit,
        receivable_credit,
        income_debit,
        income_credit,
    ):
        move_name = "move name"
        move_vals = {
            "journal_id": journal.id,
            "date": date,
            "line_ids": [
                (
                    0,
                    0,
                    {
                        "name": move_name,
                        "debit": receivable_debit,
                        "credit": receivable_credit,
                        "account_id": self.receivable_account.id,
                    },
                ),
                (
                    0,
                    0,
                    {
                        "name": move_name,
                        "debit": income_debit,
                        "credit": income_credit,
                        "account_id": self.income_account.id,
                    },
                ),
            ],
        }
        return self.MoveObj.create(move_vals)

    def check_report_journal_debit_credit(
        self, res_data, expected_debit, expected_credit
    ):
        self.assertEqual(
            expected_debit, sum(rec["debit"] for rec in res_data["Journal_Ledgers"])
        )

        self.assertEqual(
            expected_credit, sum(rec["credit"] for rec in res_data["Journal_Ledgers"])
        )

    def check_report_journal_debit_credit_taxes(
        self,
        res_data,
        expected_base_debit,
        expected_base_credit,
        expected_tax_debit,
        expected_tax_credit,
    ):
        for rec in res_data["Journal_Ledgers"]:
            self.assertEqual(
                expected_base_debit,
                sum(tax_line["base_debit"] for tax_line in rec["tax_lines"]),
            )
            self.assertEqual(
                expected_base_credit,
                sum(tax_line["base_credit"] for tax_line in rec["tax_lines"]),
            )
            self.assertEqual(
                expected_tax_debit,
                sum(tax_line["tax_debit"] for tax_line in rec["tax_lines"]),
            )
            self.assertEqual(
                expected_tax_credit,
                sum(tax_line["tax_credit"] for tax_line in rec["tax_lines"]),
            )

    def test_01_test_total(self):
        today_date = Date.today()
        last_year_date = Date.to_string(datetime.today() - relativedelta(years=1))

        move1 = self._add_move(today_date, self.journal_sale, 0, 100, 100, 0)
        move2 = self._add_move(last_year_date, self.journal_sale, 0, 100, 100, 0)

        wiz = self.JournalLedgerReportWizard.create(
            {
                "date_from": self.fy_date_start,
                "date_to": self.fy_date_end,
                "company_id": self.company.id,
                "journal_ids": [(6, 0, self.journal_sale.ids)],
                "move_target": "all",
            }
        )
        data = wiz._prepare_report_data()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 100, 100)

        move3 = self._add_move(today_date, self.journal_sale, 0, 100, 100, 0)

        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 200, 200)
        wiz.move_target = "posted"
        data = wiz._prepare_report_data()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 0, 0)

        move1.action_post()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 100, 100)

        move2.action_post()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 100, 100)

        move3.action_post()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 200, 200)

        wiz.date_from = self.previous_fy_date_start
        data = wiz._prepare_report_data()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 300, 300)

    def test_02_test_taxes_out_invoice(self):
        move_form = Form(
            self.env["account.move"].with_context(default_move_type="out_invoice")
        )
        move_form.partner_id = self.partner_2
        move_form.journal_id = self.journal_sale
        with move_form.invoice_line_ids.new() as line_form:
            line_form.name = "test"
            line_form.quantity = 1.0
            line_form.price_unit = 100
            line_form.account_id = self.income_account
            line_form.tax_ids.add(self.tax_15_s)
        with move_form.invoice_line_ids.new() as line_form:
            line_form.name = "test"
            line_form.quantity = 1.0
            line_form.price_unit = 100
            line_form.account_id = self.income_account
            line_form.tax_ids.add(self.tax_15_s)
            line_form.tax_ids.add(self.tax_20_s)
        invoice = move_form.save()
        invoice.action_post()

        wiz = self.JournalLedgerReportWizard.create(
            {
                "date_from": self.fy_date_start,
                "date_to": self.fy_date_end,
                "company_id": self.company.id,
                "journal_ids": [(6, 0, self.journal_sale.ids)],
                "move_target": "all",
            }
        )
        data = wiz._prepare_report_data()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)
        self.check_report_journal_debit_credit(res_data, 250, 250)
        self.check_report_journal_debit_credit_taxes(res_data, 0, 300, 0, 50)

    def test_03_test_taxes_in_invoice(self):
        move_form = Form(
            self.env["account.move"].with_context(default_move_type="in_invoice")
        )
        move_form.partner_id = self.partner_2
        move_form.journal_id = self.journal_purchase
        move_form.invoice_date = Date.today()
        with move_form.invoice_line_ids.new() as line_form:
            line_form.name = "test"
            line_form.quantity = 1.0
            line_form.price_unit = 100
            line_form.account_id = self.expense_account
            line_form.tax_ids.add(self.tax_15_p)
        with move_form.invoice_line_ids.new() as line_form:
            line_form.name = "test"
            line_form.quantity = 1.0
            line_form.price_unit = 100
            line_form.account_id = self.expense_account
            line_form.tax_ids.add(self.tax_15_p)
            line_form.tax_ids.add(self.tax_20_p)
        move_form.invoice_date = move_form.date
        invoice = move_form.save()
        invoice.action_post()

        wiz = self.JournalLedgerReportWizard.create(
            {
                "date_from": self.fy_date_start,
                "date_to": self.fy_date_end,
                "company_id": self.company.id,
                "journal_ids": [(6, 0, self.journal_purchase.ids)],
                "move_target": "all",
            }
        )
        data = wiz._prepare_report_data()
        res_data = self.JournalLedgerReport._get_report_values(wiz, data)

        self.check_report_journal_debit_credit(res_data, 250, 250)
        self.check_report_journal_debit_credit_taxes(res_data, 300, 0, 50, 0)

    # --- Batched move-line processing (memory optimisation) ---------------
    #
    # These tests pin the equivalence of the batched _get_move_lines: its
    # output must not depend on the batch size. The batch size is forced small
    # by patching _report_line_chunk_size (the config-driven value is clamped
    # to MIN_REPORT_CHUNK, so it cannot be pushed below the floor on purpose).

    def _journal_ledger_wizard(self, journals=None):
        journals = journals or self.journal_sale
        return self.JournalLedgerReportWizard.create(
            {
                "date_from": self.fy_date_start,
                "date_to": self.fy_date_end,
                "company_id": self.company.id,
                "journal_ids": [(6, 0, journals.ids)],
                "move_target": "all",
            }
        )

    def test_04_report_chunk_size_floor(self):
        """A misconfigured chunk size is clamped to the minimum floor."""
        report = self.JournalLedgerReport
        param = self.env["ir.config_parameter"].sudo()
        param.set_param("account_financial_report.report_chunk_size", "1")
        self.assertEqual(report._report_line_chunk_size(), MIN_REPORT_CHUNK)
        param.set_param("account_financial_report.report_chunk_size", "5000")
        self.assertEqual(report._report_line_chunk_size(), 5000)

    def test_05_journal_ledger_chunked_equivalence(self):
        """Batching _get_move_lines keeps int move-id keys and loses no line."""
        move = self._add_move(Date.today(), self.journal_sale, 0, 100, 100, 0)
        move.action_post()
        report = self.JournalLedgerReport
        wizard = self._journal_ledger_wizard()
        with patch.object(type(report), "_report_line_chunk_size", return_value=1):
            (
                move_line_ids,
                move_lines,
                _acc,
                _prt,
                _cur,
                _tax_line,
                _tax,
            ) = report._get_move_lines(move.ids, wizard, self.journal_sale.ids)
        for key in move_lines:
            self.assertIsInstance(key, int)
        self.assertIn(move.id, move_lines)
        report_lines = move_lines[move.id]
        self.assertEqual(len(report_lines), len(move.line_ids))
        src = {ml.id: (ml.debit, ml.credit) for ml in move.line_ids}
        for rl in report_lines:
            self.assertIn(rl["move_line_id"], src)
            self.assertEqual((rl["debit"], rl["credit"]), src[rl["move_line_id"]])
        self.assertEqual(set(move_line_ids), set(move.line_ids.ids))

    def test_06_journal_ledger_multi_move_auto_sequence(self):
        """With several moves split across batches, auto_sequence is decremented
        once per move (not per line, not per batch)."""
        move1 = self._add_move(Date.today(), self.journal_sale, 0, 100, 100, 0)
        move2 = self._add_move(Date.today(), self.journal_sale, 0, 50, 50, 0)
        (move1 + move2).action_post()
        report = self.JournalLedgerReport
        wizard = self._journal_ledger_wizard()
        with patch.object(type(report), "_report_line_chunk_size", return_value=1):
            _ids, move_lines, _a, _p, _c, _t, _tx = report._get_move_lines(
                [move1.id, move2.id], wizard, self.journal_sale.ids
            )
        self.assertEqual(set(move_lines.keys()), {move1.id, move2.id})
        seqs = {}
        for mid, lines in move_lines.items():
            seq_set = {ln["auto_sequence"] for ln in lines}
            self.assertEqual(len(seq_set), 1, "all lines of a move share auto_sequence")
            seqs[mid] = seq_set.pop()
        self.assertEqual(
            len(set(seqs.values())), 2, "different moves -> different auto_sequence"
        )

    def test_07_journal_ledger_chunk_invariance_with_taxes(self):
        """_get_move_lines output is identical regardless of batch size,
        including the tax circuit (tax_ids / exigibility)."""
        move_form = Form(
            self.env["account.move"].with_context(default_move_type="out_invoice")
        )
        move_form.partner_id = self.partner_2
        move_form.journal_id = self.journal_sale
        with move_form.invoice_line_ids.new() as line_form:
            line_form.name = "test"
            line_form.quantity = 2.0
            line_form.price_unit = 150.0
            line_form.account_id = self.income_account
            line_form.tax_ids.add(self.tax_15_s)
        invoice = move_form.save()
        invoice.action_post()
        report = self.JournalLedgerReport
        wizard = self._journal_ledger_wizard()

        with patch.object(type(report), "_report_line_chunk_size", return_value=1):
            r_chunk = report._get_move_lines(invoice.ids, wizard, self.journal_sale.ids)
        with patch.object(type(report), "_report_line_chunk_size", return_value=5000):
            r_single = report._get_move_lines(
                invoice.ids, wizard, self.journal_sale.ids
            )

        # The order between lines tied in the report `order` is not guaranteed
        # by Postgres across two independent searches (inherent to the original
        # code, not to the batching); compare per move sorting by move_line_id.
        def _by_move(move_lines):
            return {
                mid: sorted(lines, key=lambda d: d["move_line_id"])
                for mid, lines in move_lines.items()
            }

        # Whole return tuple must be batch-invariant, not just Move_Lines:
        # move_line_ids (0, order-insensitive), Move_Lines (1), account (2),
        # partner (3), currency (4), tax_line (5) and taxes (6) data.
        self.assertEqual(set(r_chunk[0]), set(r_single[0]))
        self.assertEqual(_by_move(r_chunk[1]), _by_move(r_single[1]))
        self.assertEqual(r_chunk[2], r_single[2])
        self.assertEqual(r_chunk[3], r_single[3])
        self.assertEqual(r_chunk[4], r_single[4])
        self.assertEqual(r_chunk[5], r_single[5])
        self.assertEqual(r_chunk[6], r_single[6])
        self.assertTrue(
            r_chunk[6], "the fixture must carry taxes to exercise the circuit"
        )
