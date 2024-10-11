# © 2016 Julien Coux (Camptocamp)
# Copyright 2020 ForgeFlow S.L. (https://www.forgeflow.com)
# Copyright 2022 Tecnativa - Víctor Martínez
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

import asyncio
import calendar
import datetime
import gc
import operator
from collections import namedtuple

from odoo import _, api, models, sql_db
from odoo.tools import float_is_zero


class GeneralLedgerReport(models.AbstractModel):
    _name = "report.account_financial_report.general_ledger"
    _description = "General Ledger Report"
    _inherit = "report.account_financial_report.abstract_report"

    def _get_analytic_data(self, account_ids):
        analytic_accounts = self.env["account.analytic.account"].browse(account_ids)
        analytic_data = {}
        for account in analytic_accounts:
            analytic_data.update({account.id: {"name": account.name}})
        return analytic_data

    def _get_taxes_data(self, taxes_ids):
        taxes = self.env["account.tax"].browse(taxes_ids)
        taxes_data = {}
        for tax in taxes:
            taxes_data.update(
                {
                    tax.id: {
                        "id": tax.id,
                        "amount": tax.amount,
                        "amount_type": tax.amount_type,
                        "display_name": tax.display_name,
                    }
                }
            )
            if tax.amount_type == "percent" or tax.amount_type == "division":
                taxes_data[tax.id]["string"] = "%"
            else:
                taxes_data[tax.id]["string"] = ""
            taxes_data[tax.id]["tax_name"] = (
                tax.display_name
                + " ("
                + str(tax.amount)
                + taxes_data[tax.id]["string"]
                + ")"
            )
        return taxes_data

    def _get_account_type_domain(self, grouped_by):
        """To avoid set all possible types, set in or not in as operator of the types
        we are interested in. In v15 we used the internal_type field (type of
        account.account.type)."""
        at_op = "in" if grouped_by != "taxes" else "not in"
        return [
            ("account_type", at_op, ["asset_receivable", "liability_payable"]),
        ]

    def _get_acc_prt_accounts_ids(self, company_id, grouped_by):
        accounts_domain = [
            ("company_id", "=", company_id),
        ] + self._get_account_type_domain(grouped_by)
        acc_prt_accounts = self.env["account.account"].search(accounts_domain)
        return acc_prt_accounts.ids

    def _get_initial_balances_bs_ml_domain(
        self, account_ids, company_id, date_from, base_domain, grouped_by, acc_prt=False
    ):
        accounts_domain = [
            ("company_id", "=", company_id),
            ("include_initial_balance", "=", True),
        ]
        if account_ids:
            accounts_domain += [("id", "in", account_ids)]
        domain = []
        domain += base_domain
        domain += [("date", "<", date_from)]
        accounts = self.env["account.account"].search(accounts_domain)
        domain += [("account_id", "in", accounts.ids)]
        if acc_prt:
            domain += self._get_account_type_domain(grouped_by)
        return domain

    def _get_initial_balances_pl_ml_domain(
        self, account_ids, company_id, date_from, fy_start_date, base_domain
    ):
        accounts_domain = [
            ("company_id", "=", company_id),
            ("include_initial_balance", "=", False),
        ]
        if account_ids:
            accounts_domain += [("id", "in", account_ids)]
        domain = []
        domain += base_domain
        domain += [("date", "<", date_from), ("date", ">=", fy_start_date)]
        accounts = self.env["account.account"].search(accounts_domain)
        domain += [("account_id", "in", accounts.ids)]
        return domain

    def _get_accounts_initial_balance(self, initial_domain_bs, initial_domain_pl):
        gl_initial_acc_bs = self.env["account.move.line"].read_group(
            domain=initial_domain_bs,
            fields=["account_id", "debit", "credit", "balance", "amount_currency:sum"],
            groupby=["account_id"],
        )
        gl_initial_acc_pl = self.env["account.move.line"].read_group(
            domain=initial_domain_pl,
            fields=["account_id", "debit", "credit", "balance", "amount_currency:sum"],
            groupby=["account_id"],
        )
        gl_initial_acc = gl_initial_acc_bs + gl_initial_acc_pl
        return gl_initial_acc

    def _get_initial_balance_fy_pl_ml_domain(
        self, account_ids, company_id, fy_start_date, base_domain
    ):
        accounts_domain = [
            ("company_id", "=", company_id),
            ("include_initial_balance", "=", False),
        ]
        if account_ids:
            accounts_domain += [("id", "in", account_ids)]
        domain = []
        domain += base_domain
        domain += [("date", "<", fy_start_date)]
        accounts = self.env["account.account"].search(accounts_domain)
        domain += [("account_id", "in", accounts.ids)]
        return domain

    def _get_pl_initial_balance(
        self, account_ids, company_id, fy_start_date, foreign_currency, base_domain
    ):
        domain = self._get_initial_balance_fy_pl_ml_domain(
            account_ids, company_id, fy_start_date, base_domain
        )
        initial_balances = self.env["account.move.line"].read_group(
            domain=domain,
            fields=["account_id", "debit", "credit", "balance", "amount_currency:sum"],
            groupby=["account_id"],
        )
        pl_initial_balance = {
            "debit": 0.0,
            "credit": 0.0,
            "balance": 0.0,
            "bal_curr": 0.0,
        }
        for initial_balance in initial_balances:
            pl_initial_balance["debit"] += initial_balance["debit"]
            pl_initial_balance["credit"] += initial_balance["credit"]
            pl_initial_balance["balance"] += initial_balance["balance"]
            pl_initial_balance["bal_curr"] += initial_balance["amount_currency"]
        return pl_initial_balance

    def _get_gl_initial_acc(
        self, account_ids, company_id, date_from, fy_start_date, base_domain, grouped_by
    ):
        initial_domain_bs = self._get_initial_balances_bs_ml_domain(
            account_ids, company_id, date_from, base_domain, grouped_by
        )
        initial_domain_pl = self._get_initial_balances_pl_ml_domain(
            account_ids, company_id, date_from, fy_start_date, base_domain
        )
        return self._get_accounts_initial_balance(initial_domain_bs, initial_domain_pl)

    def _prepare_gen_ld_data_item(self, gl):
        res = {}
        for key_bal in ["init_bal", "fin_bal"]:
            res[key_bal] = {}
            for key_field in ["credit", "debit", "balance", "bal_curr"]:
                field_name = key_field if key_field != "bal_curr" else "amount_currency"
                res[key_bal][key_field] = gl[field_name]
        return res

    def _prepare_gen_ld_data(self, gl_initial_acc, domain, grouped_by):
        data = {}
        for gl in gl_initial_acc:
            acc_id = gl["account_id"][0]
            data[acc_id] = self._prepare_gen_ld_data_item(gl)
            data[acc_id]["id"] = acc_id
            if grouped_by:
                data[acc_id][grouped_by] = False
        method = "_prepare_gen_ld_data_group_%s" % grouped_by
        if not hasattr(self, method):
            return data
        return getattr(self, method)(data, domain, grouped_by)

    def _prepare_gen_ld_data_group_partners(self, data, domain, grouped_by):
        gl_initial_acc_prt = self.env["account.move.line"].read_group(
            domain=domain,
            fields=[
                "account_id",
                "partner_id",
                "debit",
                "credit",
                "balance",
                "amount_currency:sum",
            ],
            groupby=["account_id", "partner_id"],
            lazy=False,
        )
        if gl_initial_acc_prt:
            for gl in gl_initial_acc_prt:
                if not gl["partner_id"]:
                    prt_id = 0
                    prt_name = _("Missing Partner")
                else:
                    prt_id = gl["partner_id"][0]
                    prt_name = gl["partner_id"][1]
                    prt_name = prt_name._value
                acc_id = gl["account_id"][0]
                data[acc_id][prt_id] = self._prepare_gen_ld_data_item(gl)
                data[acc_id][prt_id]["id"] = prt_id
                data[acc_id][prt_id]["name"] = prt_name
                data[acc_id][grouped_by] = True
        return data

    def _prepare_gen_ld_data_group_taxes(self, data, domain, grouped_by):
        gl_initial_acc_prt = self.env["account.move.line"].read_group(
            domain=domain,
            fields=[
                "account_id",
                "debit",
                "credit",
                "balance",
                "amount_currency:sum",
                "tax_line_id",
            ],
            groupby=["account_id"],
            lazy=False,
        )
        if gl_initial_acc_prt:
            for gl in gl_initial_acc_prt:
                if "tax_line_id" in gl and gl["tax_line_id"]:
                    tax_id = gl["tax_line_id"][0]
                    tax_name = gl["tax_line_id"][1]
                    tax_name = tax_name._value
                else:
                    tax_id = 0
                    tax_name = "Missing Tax"
                acc_id = gl["account_id"][0]
                data[acc_id][tax_id] = self._prepare_gen_ld_data_item(gl)
                data[acc_id][tax_id]["id"] = tax_id
                data[acc_id][tax_id]["name"] = tax_name
                data[acc_id][grouped_by] = True
        return data

    def _get_initial_balance_data(
        self,
        account_ids,
        partner_ids,
        company_id,
        date_from,
        foreign_currency,
        only_posted_moves,
        unaffected_earnings_account,
        fy_start_date,
        cost_center_ids,
        extra_domain,
        grouped_by,
    ):
        # If explicit list of accounts is provided,
        # don't include unaffected earnings account
        if account_ids:
            unaffected_earnings_account = False
        base_domain = []
        if company_id:
            base_domain += [("company_id", "=", company_id)]
        if partner_ids:
            base_domain += [("partner_id", "in", partner_ids)]
        if only_posted_moves:
            base_domain += [("move_id.state", "=", "posted")]
        else:
            base_domain += [("move_id.state", "in", ["posted", "draft"])]
        if cost_center_ids:
            base_domain += [("analytic_account_ids", "in", cost_center_ids)]
        if extra_domain:
            base_domain += extra_domain
        gl_initial_acc = self._get_gl_initial_acc(
            account_ids, company_id, date_from, fy_start_date, base_domain, grouped_by
        )
        domain = self._get_initial_balances_bs_ml_domain(
            account_ids, company_id, date_from, base_domain, grouped_by, acc_prt=True
        )
        data = self._prepare_gen_ld_data(gl_initial_acc, domain, grouped_by)
        accounts_ids = list(data.keys())
        unaffected_id = unaffected_earnings_account
        if unaffected_id:
            if unaffected_id not in accounts_ids:
                accounts_ids.append(unaffected_id)
                data[unaffected_id] = self._initialize_data(foreign_currency)
                data[unaffected_id]["id"] = unaffected_id
                data[unaffected_id]["mame"] = ""
                data[unaffected_id][grouped_by] = False
            pl_initial_balance = self._get_pl_initial_balance(
                account_ids, company_id, fy_start_date, foreign_currency, base_domain
            )
            for key_bal in ["init_bal", "fin_bal"]:
                fields_balance = ["credit", "debit", "balance"]
                if foreign_currency:
                    fields_balance.append("bal_curr")
                for field_name in fields_balance:
                    data[unaffected_id][key_bal][field_name] += pl_initial_balance[
                        field_name
                    ]
        return data

    @api.model
    def _get_move_line_data(self, move_line):
        move_line_data = {
            "id": move_line.id,
            "date": move_line.date,
            "entry": move_line.move_name,
            "entry_id": move_line.move_id[0],
            "journal_id": move_line.journal_id[0],
            "account_id": move_line.account_id[0],
            "partner_id": move_line.partner_id[0] if move_line.partner_id else False,
            "partner_name": move_line.partner_id[1] if move_line.partner_id else "",
            "ref": move_line.ref or "",
            "name": move_line.name or "",
            "tax_ids": move_line.tax_ids,
            "tax_line_id": move_line.tax_line_id,
            "debit": move_line.debit,
            "credit": move_line.credit,
            "balance": move_line.balance,
            "bal_curr": move_line.amount_currency,
            "rec_id": move_line.full_reconcile_id[0]
            if move_line.full_reconcile_id
            else False,
            "rec_name": move_line.full_reconcile_id[1]
            if move_line.full_reconcile_id
            else "",
            "currency_id": move_line.currency_id,
            "analytic_distribution": move_line.analytic_distribution or {},
        }
        ref_label = (
            move_line_data["name"]
            if move_line_data["ref"] in (move_line_data["name"], "")
            else f"{move_line_data['ref']} - {move_line_data['name']}"
        )
        move_line_data.update({"ref_label": ref_label})
        return move_line_data

    @api.model
    def _get_period_domain(
        self,
        account_ids,
        partner_ids,
        company_id,
        only_posted_moves,
        date_to,
        date_from,
        cost_center_ids,
    ):
        domain = [
            ("display_type", "not in", ["line_note", "line_section"]),
            ("date", ">=", date_from),
            ("date", "<=", date_to),
        ]
        if account_ids:
            domain += [("account_id", "in", account_ids)]
        if company_id:
            domain += [("company_id", "=", company_id)]
        if partner_ids:
            domain += [("partner_id", "in", partner_ids)]
        if only_posted_moves:
            domain += [("move_id.state", "=", "posted")]
        else:
            domain += [("move_id.state", "in", ["posted", "draft"])]

        if cost_center_ids:
            domain += [("analytic_account_ids", "in", cost_center_ids)]
        return domain

    def _initialize_data(self, foreign_currency):
        res = {}
        for key_bal in ["init_bal", "fin_bal"]:
            res[key_bal] = {}
            for key_field in ["balance", "credit", "debit"]:
                res[key_bal][key_field] = 0.0
            if foreign_currency:
                res[key_bal]["bal_curr"] = 0.0
        return res

    def _get_reconciled_after_date_to_ids(self, full_reconcile_ids, date_to):
        full_reconcile_ids = list(full_reconcile_ids)
        domain = [
            ("max_date", ">", date_to),
            ("full_reconcile_id", "in", full_reconcile_ids),
        ]
        fields = ["full_reconcile_id"]
        reconciled_after_date_to = self.env["account.partial.reconcile"].search_read(
            domain=domain, fields=fields
        )
        rec_after_date_to_ids = list(
            map(operator.itemgetter("full_reconcile_id"), reconciled_after_date_to)
        )
        rec_after_date_to_ids = [i[0] for i in rec_after_date_to_ids]
        return rec_after_date_to_ids

    def _prepare_ml_items(self, move_line, grouped_by):
        res = []
        if grouped_by == "partners":
            item_id = move_line.partner_id[0] if move_line.partner_id else 0
            item_name = (
                move_line.partner_id[1]
                if move_line.partner_id
                else _("Missing Partner")
            )
            res.append({"id": item_id, "name": item_name})
        elif grouped_by == "taxes":
            if move_line.tax_line_id:
                res.append(
                    {"id": move_line.tax_line_id[0], "name": move_line.tax_line_id[1]}
                )
            elif move_line.tax_ids:
                res.extend(
                    {"id": tax_item.id, "name": tax_item.name}
                    for tax_id in move_line.tax_ids
                    for tax_item in self.env["account.tax"].browse(tax_id)
                )
            else:
                res.append({"id": 0, "name": "Missing Tax"})
        else:
            res.append({"id": 0, "name": ""})
        return res

    def process_ml_data(
        self,
        move_lines,
        journal_ids,
        taxes_ids,
        analytic_ids,
        full_reconcile_ids,
        full_reconcile_data,
        gen_ld_data,
        foreign_currency,
        grouped_by,
        acc_prt_account_ids,
    ):
        def initialize_if_needed(data, key, name=None):
            if key not in data:
                data[key] = self._initialize_data(foreign_currency)
                data[key]["id"] = key
                if name:
                    data[key]["name"] = name
                if grouped_by:
                    data[key][grouped_by] = False
            return data[key]

        for move_line in move_lines:
            journal_ids.add(move_line.journal_id[0])

            for tax_id in move_line.tax_ids:
                taxes_ids.add(tax_id)

            for analytic_account in move_line.analytic_distribution or {}:
                analytic_ids.add(int(analytic_account))

            if move_line.full_reconcile_id:
                rec_id = move_line.full_reconcile_id[0]
                if rec_id not in full_reconcile_ids:
                    full_reconcile_data[rec_id] = {
                        "id": rec_id,
                        "name": move_line.full_reconcile_id[1],
                    }
                    full_reconcile_ids.add(rec_id)

            acc_id = move_line.account_id[0]
            ml_id = move_line.id
            acc_data = initialize_if_needed(
                gen_ld_data, acc_id, move_line.account_id[1]
            )

            if acc_id in acc_prt_account_ids:
                item_ids = self._prepare_ml_items(move_line, grouped_by)
                for item in item_ids:
                    item_id = item["id"]
                    item_data = initialize_if_needed(acc_data, item_id, item["name"])
                    item_data[ml_id] = self._get_move_line_data(move_line)
                    item_data["fin_bal"]["credit"] += move_line.credit
                    item_data["fin_bal"]["debit"] += move_line.debit
                    item_data["fin_bal"]["balance"] += move_line.balance
                    if foreign_currency:
                        item_data["fin_bal"]["bal_curr"] += move_line.amount_currency
                    if grouped_by:
                        acc_data[grouped_by] = True
            else:
                acc_data[ml_id] = self._get_move_line_data(move_line)

            acc_data["fin_bal"]["credit"] += move_line.credit
            acc_data["fin_bal"]["debit"] += move_line.debit
            acc_data["fin_bal"]["balance"] += move_line.balance
            if foreign_currency:
                acc_data["fin_bal"]["bal_curr"] += move_line.amount_currency

    def _get_period_ml_data(
        self,
        account_ids,
        partner_ids,
        company_id,
        foreign_currency,
        only_posted_moves,
        date_from,
        date_to,
        gen_ld_data,
        cost_center_ids,
        extra_domain,
        grouped_by,
    ):
        domain = self._get_period_domain(
            account_ids,
            partner_ids,
            company_id,
            only_posted_moves,
            date_to,
            date_from,
            cost_center_ids,
        )
        if extra_domain:
            domain += extra_domain

        ml_fields = self._get_ml_fields()
        journal_ids = set()
        full_reconcile_ids = set()
        taxes_ids = set()
        analytic_ids = set()
        full_reconcile_data = {}
        acc_prt_account_ids = self._get_acc_prt_accounts_ids(company_id, grouped_by)
        batch_size = 50000
        offset = 0
        MoveLine = namedtuple("MoveLine", ml_fields)
        test_enable = self.env.context.get("test_enable", False)

        async def fetch_move_lines(offset):
            new_cr = (
                sql_db.db_connect(self.env.cr.dbname).cursor()
                if not test_enable
                else None
            )
            new_env = (
                api.Environment(new_cr, self.env.uid, self.env.context.copy())
                if not test_enable
                else self.env
            )
            move_lines = (
                new_env["account.move.line"]
                .with_context(prefetch_fields=False)
                .search(
                    domain=domain,
                    order="date,move_name",
                    limit=batch_size,
                    offset=offset,
                )
            )
            move_lines_data = move_lines.with_context(prefetch_fields=False).read(
                ml_fields
            )
            move_lines = [MoveLine(**line) for line in move_lines_data]
            return move_lines, new_cr, new_env

        async def process_batches():
            nonlocal offset
            while True:
                move_lines, new_cr, new_env = await fetch_move_lines(offset)
                if not move_lines:
                    if new_cr:
                        new_cr.close()
                    break

                self.with_env(new_env).process_ml_data(
                    move_lines,
                    journal_ids,
                    taxes_ids,
                    analytic_ids,
                    full_reconcile_ids,
                    full_reconcile_data,
                    gen_ld_data,
                    foreign_currency,
                    grouped_by,
                    acc_prt_account_ids,
                )

                if new_cr:
                    new_cr.close()
                offset += batch_size
                gc.collect()

        asyncio.run(process_batches())

        journals_data = self._get_journals_data(list(journal_ids))
        accounts_data = self._get_accounts_data(gen_ld_data.keys())
        taxes_data = self._get_taxes_data(list(taxes_ids))
        analytic_data = self._get_analytic_data(list(analytic_ids))
        rec_after_date_to_ids = self._get_reconciled_after_date_to_ids(
            full_reconcile_data.keys(), date_to
        )

        return (
            gen_ld_data,
            accounts_data,
            journals_data,
            full_reconcile_data,
            taxes_data,
            analytic_data,
            rec_after_date_to_ids,
        )

    @api.model
    def _recalculate_cumul_balance(
        self, move_lines, last_cumul_balance, rec_after_date_to_ids
    ):
        for move_line in move_lines:
            move_line["balance"] += last_cumul_balance
            last_cumul_balance = move_line["balance"]
            if move_line["rec_id"] in rec_after_date_to_ids:
                move_line["rec_name"] = "(" + _("future") + ") " + move_line["rec_name"]
        return move_lines

    def _create_account(self, account, acc_id, gen_led_data, rec_after_date_to_ids):
        move_lines = []
        for ml_id in gen_led_data[acc_id].keys():
            if not isinstance(ml_id, int):
                account.update({ml_id: gen_led_data[acc_id][ml_id]})
            else:
                move_lines += [gen_led_data[acc_id][ml_id]]
        move_lines = sorted(move_lines, key=lambda k: (k["date"]))
        move_lines = self._recalculate_cumul_balance(
            move_lines,
            gen_led_data[acc_id]["init_bal"]["balance"],
            rec_after_date_to_ids,
        )
        account.update({"move_lines": move_lines})
        return account

    def _create_account_not_show_item(
        self, account, acc_id, gen_led_data, rec_after_date_to_ids, grouped_by
    ):
        move_lines = []
        for prt_id in gen_led_data[acc_id].keys():
            if not isinstance(prt_id, int):
                account.update({prt_id: gen_led_data[acc_id][prt_id]})
            elif isinstance(gen_led_data[acc_id][prt_id], dict):
                for ml_id in gen_led_data[acc_id][prt_id].keys():
                    if isinstance(ml_id, int):
                        move_lines += [gen_led_data[acc_id][prt_id][ml_id]]
        move_lines = sorted(move_lines, key=lambda k: (k["date"]))
        move_lines = self._recalculate_cumul_balance(
            move_lines,
            gen_led_data[acc_id]["init_bal"]["balance"],
            rec_after_date_to_ids,
        )
        account.update({"move_lines": move_lines, grouped_by: False})
        return account

    def _get_list_grouped_item(
        self, data, account, rec_after_date_to_ids, hide_account_at_0, rounding
    ):
        list_grouped = []
        for data_id in data.keys():
            group_item = {}
            move_lines = []
            if not isinstance(data_id, int):
                account.update({data_id: data[data_id]})
            else:
                for ml_id in data[data_id].keys():
                    if not isinstance(ml_id, int):
                        group_item.update({ml_id: data[data_id][ml_id]})
                    else:
                        move_lines += [data[data_id][ml_id]]
                move_lines = sorted(move_lines, key=lambda k: (k["date"]))
                move_lines = self._recalculate_cumul_balance(
                    move_lines,
                    data[data_id]["init_bal"]["balance"],
                    rec_after_date_to_ids,
                )
                group_item.update({"move_lines": move_lines})
                if (
                    hide_account_at_0
                    and float_is_zero(
                        data[data_id]["init_bal"]["balance"],
                        precision_rounding=rounding,
                    )
                    and group_item["move_lines"] == []
                ):
                    continue
                list_grouped += [group_item]
        return account, list_grouped

    def _create_general_ledger(
        self,
        gen_led_data,
        accounts_data,
        grouped_by,
        rec_after_date_to_ids,
        hide_account_at_0,
    ):
        general_ledger = []
        rounding = self.env.company.currency_id.rounding

        def should_hide_account(account_data, balance_key, rounding):
            return (
                hide_account_at_0
                and float_is_zero(
                    account_data[balance_key]["balance"], precision_rounding=rounding
                )
                and not account_data.get("move_lines", [])
                and not account_data.get("list_grouped", [])
            )

        for acc_id, acc_data in gen_led_data.items():
            account = {
                "code": accounts_data[acc_id]["code"],
                "name": accounts_data[acc_id]["name"],
                "type": "account",
                "currency_id": accounts_data[acc_id]["currency_id"],
                "centralized": accounts_data[acc_id]["centralized"],
                "grouped_by": grouped_by,
            }

            if grouped_by and not acc_data[grouped_by]:
                account = self._create_account(
                    account, acc_id, gen_led_data, rec_after_date_to_ids
                )
                if should_hide_account(acc_data, "init_bal", rounding):
                    continue
            else:
                if grouped_by:
                    account, list_grouped = self._get_list_grouped_item(
                        acc_data,
                        account,
                        rec_after_date_to_ids,
                        hide_account_at_0,
                        rounding,
                    )
                    account["list_grouped"] = list_grouped
                    if should_hide_account(acc_data, "init_bal", rounding):
                        continue
                else:
                    account = self._create_account_not_show_item(
                        account, acc_id, gen_led_data, rec_after_date_to_ids, grouped_by
                    )
                    if should_hide_account(acc_data, "init_bal", rounding):
                        continue

            general_ledger.append(account)

        return general_ledger

    @api.model
    def _calculate_centralization(self, centralized_ml, move_line, date_to):
        jnl_id = move_line["journal_id"]
        month = move_line["date"].month
        if jnl_id not in centralized_ml.keys():
            centralized_ml[jnl_id] = {}
        if month not in centralized_ml[jnl_id].keys():
            centralized_ml[jnl_id][month] = {}
            last_day_month = calendar.monthrange(move_line["date"].year, month)
            date = datetime.date(move_line["date"].year, month, last_day_month[1])
            if date > date_to:
                date = date_to
            centralized_ml[jnl_id][month].update(
                {
                    "journal_id": jnl_id,
                    "ref_label": "Centralized entries",
                    "date": date,
                    "debit": 0.0,
                    "credit": 0.0,
                    "balance": 0.0,
                    "bal_curr": 0.0,
                    "partner_id": False,
                    "rec_id": 0,
                    "entry_id": False,
                    "tax_ids": [],
                    "tax_line_id": False,
                    "full_reconcile_id": False,
                    "id": False,
                    "currency_id": False,
                    "analytic_distribution": {},
                }
            )
        centralized_ml[jnl_id][month]["debit"] += move_line["debit"]
        centralized_ml[jnl_id][month]["credit"] += move_line["credit"]
        centralized_ml[jnl_id][month]["balance"] += (
            move_line["debit"] - move_line["credit"]
        )
        centralized_ml[jnl_id][month]["bal_curr"] += move_line["bal_curr"]
        return centralized_ml

    @api.model
    def _get_centralized_ml(self, account, date_to, grouped_by):
        centralized_ml = {}
        if isinstance(date_to, str):
            date_to = datetime.datetime.strptime(date_to, "%Y-%m-%d").date()
        if grouped_by and account[grouped_by]:
            for item in account["list_grouped"]:
                for move_line in item["move_lines"]:
                    centralized_ml = self._calculate_centralization(
                        centralized_ml,
                        move_line,
                        date_to,
                    )
        else:
            for move_line in account["move_lines"]:
                centralized_ml = self._calculate_centralization(
                    centralized_ml,
                    move_line,
                    date_to,
                )
        list_centralized_ml = []
        for jnl_id in centralized_ml.keys():
            list_centralized_ml += list(centralized_ml[jnl_id].values())
        return list_centralized_ml

    def _get_report_values(self, docids, data):
        wizard_id = data["wizard_id"]
        company = self.env["res.company"].browse(data["company_id"])
        company_id = data["company_id"]
        date_to = data["date_to"]
        date_from = data["date_from"]
        partner_ids = data["partner_ids"]
        account_ids = data["account_ids"]
        cost_center_ids = data["cost_center_ids"]
        grouped_by = data["grouped_by"]
        hide_account_at_0 = data["hide_account_at_0"]
        foreign_currency = data["foreign_currency"]
        only_posted_moves = data["only_posted_moves"]
        unaffected_earnings_account = data["unaffected_earnings_account"]
        fy_start_date = data["fy_start_date"]
        extra_domain = data["domain"]
        gen_ld_data = self._get_initial_balance_data(
            account_ids,
            partner_ids,
            company_id,
            date_from,
            foreign_currency,
            only_posted_moves,
            unaffected_earnings_account,
            fy_start_date,
            cost_center_ids,
            extra_domain,
            grouped_by,
        )
        centralize = data["centralize"]
        (
            gen_ld_data,
            accounts_data,
            journals_data,
            full_reconcile_data,
            taxes_data,
            analytic_data,
            rec_after_date_to_ids,
        ) = self._get_period_ml_data(
            account_ids,
            partner_ids,
            company_id,
            foreign_currency,
            only_posted_moves,
            date_from,
            date_to,
            gen_ld_data,
            cost_center_ids,
            extra_domain,
            grouped_by,
        )
        general_ledger = self._create_general_ledger(
            gen_ld_data,
            accounts_data,
            grouped_by,
            rec_after_date_to_ids,
            hide_account_at_0,
        )
        if centralize:
            for account in general_ledger:
                if account["centralized"]:
                    centralized_ml = self._get_centralized_ml(
                        account, date_to, grouped_by
                    )
                    account["move_lines"] = centralized_ml
                    account["move_lines"] = self._recalculate_cumul_balance(
                        account["move_lines"],
                        gen_ld_data[account["id"]]["init_bal"]["balance"],
                        rec_after_date_to_ids,
                    )
                    if grouped_by and account[grouped_by]:
                        account[grouped_by] = False
                        del account["list_grouped"]
        general_ledger = sorted(general_ledger, key=lambda k: k["code"])
        return {
            "doc_ids": [wizard_id],
            "doc_model": "general.ledger.report.wizard",
            "docs": self.env["general.ledger.report.wizard"].browse(wizard_id),
            "foreign_currency": data["foreign_currency"],
            "company_name": company.display_name,
            "company_currency": company.currency_id,
            "currency_name": company.currency_id.name,
            "date_from": data["date_from"],
            "date_to": data["date_to"],
            "only_posted_moves": data["only_posted_moves"],
            "hide_account_at_0": data["hide_account_at_0"],
            "show_cost_center": data["show_cost_center"],
            "general_ledger": general_ledger,
            "accounts_data": accounts_data,
            "journals_data": journals_data,
            "full_reconcile_data": full_reconcile_data,
            "taxes_data": taxes_data,
            "centralize": centralize,
            "analytic_data": analytic_data,
            "filter_partner_ids": True if partner_ids else False,
            "currency_model": self.env["res.currency"],
        }

    def _get_ml_fields(self):
        return self.COMMON_ML_FIELDS + [
            "analytic_distribution",
            "full_reconcile_id",
            "tax_line_id",
            "currency_id",
            "credit",
            "debit",
            "amount_currency",
            "balance",
            "tax_ids",
            "move_name",
        ]
