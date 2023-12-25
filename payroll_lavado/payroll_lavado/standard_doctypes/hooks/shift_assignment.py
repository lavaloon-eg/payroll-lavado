import frappe
from hrms.hr.doctype.shift_assignment.shift_assignment import ShiftAssignment
from payroll_lavado.employee_change_log import create_employee_change_log


def shift_assignment_create_employee_change_log(doc: ShiftAssignment, method):
    employee_id = frappe.db.get_value("Employee", doc.employee)
    create_employee_change_log(employee_id=employee_id, source_doctype=doc.doctype, source_id=doc.name)
