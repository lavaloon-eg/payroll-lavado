import frappe
from erpnext.payroll.doctype.salary_structure_assignment.salary_structure_assignment import SalaryStructureAssignment
from payroll_lavado.employee_change_log import create_employee_change_log


def salary_structure_assignment_create_employee_change_log(doc: SalaryStructureAssignment, method):
    employee_id = frappe.db.get_value("Employee", doc.employee)
    create_employee_change_log(employee_id=employee_id, source_doctype=doc.doctype, source_id=doc.name)
