import frappe
from erpnext.hr.doctype.employee_transfer.employee_transfer import EmployeeTransfer
from payroll_lavado.employee_change_log import create_employee_change_log


def employee_transfer_create_employee_change_log(doc: EmployeeTransfer, method):
    employee_id = frappe.db.get_value("Employee", doc.employee)
    create_employee_change_log(employee_id=employee_id, source_doctype=doc.doctype, source_id=doc.name)
