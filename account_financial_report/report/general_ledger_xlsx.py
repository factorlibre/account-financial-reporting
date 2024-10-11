# Author: Damien Crier
# Author: Julien Coux
# Copyright 2016 Camptocamp SA
# Copyright 2021 Tecnativa - João Marques
# Copyright 2022 Tecnativa - Víctor Martínez
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

from odoo import _, api, models, sql_db


class GeneralLedgerXslx(models.AbstractModel):
    _name = "report.a_f_r.report_general_ledger_xlsx"
    _description = "General Ledger XLSL Report"
    _inherit = "report.account_financial_report.abstract_report_xlsx"

    def _get_report_name(self, report, data=False):
        company_id = data.get("company_id", False)
        report_name = _("General Ledger")
        if company_id:
            company = self.env["res.company"].browse(company_id)
            suffix = " - {} - {}".format(company.name, company.currency_id.name)
            report_name = report_name + suffix
        return report_name

    def _get_report_columns(self, report):
        res = [
            {"header": _("Date"), "field": "date", "width": 11},
            {"header": _("Entry"), "field": "entry", "width": 18},
            {"header": _("Journal"), "field": "journal", "width": 8},
            {"header": _("Account"), "field": "account", "width": 9},
            {"header": _("Taxes"), "field": "taxes_description", "width": 15},
            {"header": _("Partner"), "field": "partner_name", "width": 25},
            {"header": _("Ref - Label"), "field": "ref_label", "width": 40},
        ]
        if report.show_cost_center:
            res += [
                {
                    "header": _("Analytic Distribution"),
                    "field": "analytic_distribution",
                    "width": 20,
                },
            ]
        res += [
            {"header": _("Rec."), "field": "rec_name", "width": 15},
            {
                "header": _("Debit"),
                "field": "debit",
                "field_initial_balance": "initial_debit",
                "field_final_balance": "final_debit",
                "type": "amount",
                "width": 14,
            },
            {
                "header": _("Credit"),
                "field": "credit",
                "field_initial_balance": "initial_credit",
                "field_final_balance": "final_credit",
                "type": "amount",
                "width": 14,
            },
            {
                "header": _("Cumul. Bal."),
                "field": "balance",
                "field_initial_balance": "initial_balance",
                "field_final_balance": "final_balance",
                "type": "amount",
                "width": 14,
            },
        ]
        if report.foreign_currency:
            res += [
                {
                    "header": _("Amount cur."),
                    "field": "bal_curr",
                    "field_initial_balance": "initial_bal_curr",
                    "field_final_balance": "final_bal_curr",
                    "type": "amount_currency",
                    "width": 10,
                },
                {
                    "header": _("Cumul cur."),
                    "field": "total_bal_curr",
                    "field_initial_balance": "initial_bal_curr",
                    "field_final_balance": "final_bal_curr",
                    "type": "amount_currency",
                    "width": 10,
                },
            ]
        res_as_dict = {}
        for i, column in enumerate(res):
            res_as_dict[i] = column
        return res_as_dict

    def _get_report_filters(self, report):
        return [
            [
                _("Date range filter"),
                _("From: %(date_from)s To: %(date_to)s")
                % ({"date_from": report.date_from, "date_to": report.date_to}),
            ],
            [
                _("Target moves filter"),
                _("All posted entries")
                if report.target_move == "posted"
                else _("All entries"),
            ],
            [
                _("Account balance at 0 filter"),
                _("Hide") if report.hide_account_at_0 else _("Show"),
            ],
            [_("Centralize filter"), _("Yes") if report.centralize else _("No")],
            [
                _("Show foreign currency"),
                _("Yes") if report.foreign_currency else _("No"),
            ],
        ]

    def _get_col_count_filter_name(self):
        return 2

    def _get_col_count_filter_value(self):
        return 2

    def _get_col_pos_initial_balance_label(self):
        return 5

    def _get_col_count_final_balance_name(self):
        return 5

    def _get_col_pos_final_balance_label(self):
        return 5

    # flake8: noqa: C901
    def _generate_report_content(self, workbook, report, data, report_data):
        res_data = self.env[
            "report.account_financial_report.general_ledger"
        ]._get_report_values(report, data)
        general_ledger = res_data["general_ledger"]
        accounts_data = res_data["accounts_data"]
        journals_data = res_data["journals_data"]
        taxes_data = res_data["taxes_data"]
        analytic_data = res_data["analytic_data"]
        filter_partner_ids = res_data["filter_partner_ids"]
        foreign_currency = res_data["foreign_currency"]
        test_enable = self.env.context.get("test_enable", False)
        # For each account
        for account in general_ledger:
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
            # Write account title
            total_bal_curr = account["init_bal"].get("bal_curr", 0)
            self.write_array_title(
                account["code"] + " - " + accounts_data[account["id"]]["name"],
                report_data,
            )
            self.with_env(new_env)._process_account_lines(
                account,
                report_data,
                accounts_data,
                journals_data,
                taxes_data,
                analytic_data,
                filter_partner_ids,
                foreign_currency,
                total_bal_curr,
            )
            # 2 lines break
            report_data["row_pos"] += 2
            if not test_enable:
                new_cr.close()

    def write_initial_balance_from_dict(self, my_object, report_data):
        """Specific function to write initial balance for General Ledger"""
        label = False
        if "account" not in my_object["type"] and "grouped_by" in my_object:
            if my_object["grouped_by"] == "partners":
                label = _("Partner Initial balance")
            elif my_object["grouped_by"] == "taxes":
                label = _("Tax Initial balance")
        label = label if label else _("Initial balance")
        return super().write_initial_balance_from_dict(my_object, label, report_data)

    def write_ending_balance_from_dict(self, my_object, report_data):
        """Specific function to write ending balance for General Ledger"""
        label = name = False
        if "account" in my_object["type"]:
            name = my_object["code"] + " - " + my_object["name"]
        elif "grouped_by" in my_object:
            name = my_object["name"]
            if my_object["grouped_by"] == "partners":
                label = _("Partner ending balance")
            elif my_object["grouped_by"] == "taxes":
                label = _("Tax ending balance")
        label = label if label else _("Ending balance")
        return super().write_ending_balance_from_dict(
            my_object, name, label, report_data
        )

    def _process_account_lines(
        self,
        account,
        report_data,
        accounts_data,
        journals_data,
        taxes_data,
        analytic_data,
        filter_partner_ids,
        foreign_currency,
        total_bal_curr,
    ):
        process_method = (
            self._process_single_account
            if "list_grouped" not in account
            else self._process_grouped_items
        )
        process_method(
            account,
            report_data,
            accounts_data,
            journals_data,
            taxes_data,
            analytic_data,
            foreign_currency,
            total_bal_curr,
            filter_partner_ids=filter_partner_ids
            if "list_grouped" in account
            else False,
        )

    def _process_single_account(
        self,
        account,
        report_data,
        accounts_data,
        journals_data,
        taxes_data,
        analytic_data,
        foreign_currency,
        total_bal_curr,
        filter_partner_ids=False,
    ):
        self.write_array_header(report_data)
        account.update(
            {
                "initial_debit": account["init_bal"]["debit"],
                "initial_credit": account["init_bal"]["credit"],
                "initial_balance": account["init_bal"]["balance"],
            }
        )
        if foreign_currency:
            account["initial_bal_curr"] = account["init_bal"]["bal_curr"]
        self.write_initial_balance_from_dict(account, report_data)
        batch_size = 25000
        for i in range(0, len(account["move_lines"]), batch_size):
            batch_lines = account["move_lines"][i : i + batch_size]
            total_bal_curr = self._process_lines(
                batch_lines,
                account,
                journals_data,
                taxes_data,
                analytic_data,
                foreign_currency,
                total_bal_curr,
                report_data,
            )
        account.update(
            {
                "final_debit": account["fin_bal"]["debit"],
                "final_credit": account["fin_bal"]["credit"],
                "final_balance": account["fin_bal"]["balance"],
            }
        )
        if foreign_currency:
            account["final_bal_curr"] = account["fin_bal"]["bal_curr"]
        self.write_ending_balance_from_dict(account, report_data)

    def _process_grouped_items(
        self,
        account,
        report_data,
        accounts_data,
        journals_data,
        taxes_data,
        analytic_data,
        foreign_currency,
        total_bal_curr,
        filter_partner_ids,
    ):
        for group_item in account["list_grouped"]:
            self.write_array_title(group_item["name"], report_data)
            self.write_array_header(report_data)
            currency_id = accounts_data[account["id"]]["currency_id"]
            currency_name = accounts_data[account["id"]]["currency_name"]
            group_item.update(
                {
                    "initial_debit": group_item["init_bal"]["debit"],
                    "initial_credit": group_item["init_bal"]["credit"],
                    "initial_balance": group_item["init_bal"]["balance"],
                    "type": "partner",
                    "grouped_by": account.get("grouped_by", ""),
                    "currency_id": currency_id,
                    "currency_name": currency_name,
                }
            )
            if foreign_currency:
                group_item["initial_bal_curr"] = group_item["init_bal"]["bal_curr"]
            self.write_initial_balance_from_dict(group_item, report_data)
            batch_size = 25000  # Define the batch size
            for i in range(0, len(group_item["move_lines"]), batch_size):
                batch_lines = group_item["move_lines"][i : i + batch_size]
                total_bal_curr = self._process_lines(
                    batch_lines,
                    account,
                    journals_data,
                    taxes_data,
                    analytic_data,
                    foreign_currency,
                    total_bal_curr,
                    report_data,
                )
            group_item.update(
                {
                    "final_debit": group_item["fin_bal"]["debit"],
                    "final_credit": group_item["fin_bal"]["credit"],
                    "final_balance": group_item["fin_bal"]["balance"],
                }
            )
            if foreign_currency and group_item["currency_id"]:
                group_item["final_bal_curr"] = group_item["fin_bal"]["bal_curr"]
            self.write_ending_balance_from_dict(group_item, report_data)
            report_data["row_pos"] += 1
        if not filter_partner_ids:
            account.update(
                {
                    "final_debit": account["fin_bal"]["debit"],
                    "final_credit": account["fin_bal"]["credit"],
                    "final_balance": account["fin_bal"]["balance"],
                }
            )
            if foreign_currency and account["currency_id"]:
                account["final_bal_curr"] = account["fin_bal"]["bal_curr"]
            self.write_ending_balance_from_dict(account, report_data)

    def _process_lines(
        self,
        lines,
        account,
        journals_data,
        taxes_data,
        analytic_data,
        foreign_currency,
        total_bal_curr,
        report_data,
    ):
        for line in lines:
            line, total_bal_curr = self._update_line_with_additional_info(
                line,
                account,
                journals_data,
                taxes_data,
                analytic_data,
                foreign_currency,
                total_bal_curr,
            )
            self.write_line_from_dict(line, report_data)
        return total_bal_curr

    def _update_line_with_additional_info(
        self,
        line,
        account,
        journals_data,
        taxes_data,
        analytic_data,
        foreign_currency,
        total_bal_curr,
    ):
        line["account"] = account["code"]
        line["journal"] = journals_data[line["journal_id"]]["code"]
        if line["currency_id"]:
            line["currency_name"] = line["currency_id"][1]
            line["currency_id"] = line["currency_id"][0]
        if line["ref_label"] != "Centralized entries":
            line["taxes_description"] = " ".join(
                taxes_data[tax_id]["tax_name"] for tax_id in line["tax_ids"]
            )
            if line["tax_line_id"]:
                line["taxes_description"] += line["tax_line_id"][1]
            line["analytic_distribution"] = " ".join(
                f"{analytic_data[int(account_id)]['name']} {value}%"
                if value < 100
                else f"{analytic_data[int(account_id)]['name']}"
                for account_id, value in line["analytic_distribution"].items()
            )
        if foreign_currency:
            total_bal_curr += line["bal_curr"]
            line["total_bal_curr"] = total_bal_curr
        return line, total_bal_curr
