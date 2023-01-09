import frappe
from erpnext.hr.doctype.employee.employee import Employee
from payroll_lavado.employee_change_log import create_employee_change_log


def employee_create_employee_change_log(doc: Employee, method):
    create_employee_change_log(employee_id=doc.name, source_doctype=doc.doctype, source_id=doc.name)
