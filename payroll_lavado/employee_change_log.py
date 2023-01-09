import frappe
import datetime


def create_employee_change_log(employee_id: str, source_doctype: str, source_id: str):
    employee_doc = frappe.get_doc("Employee", employee_id)

    if source_doctype.lower() == "employee":
        employee_old_doc = employee_doc.get_doc_before_save()
        if employee_old_doc.designation.lower() == employee_doc.designation.lower() and \
                employee_old_doc.company == employee_doc.company:
            return

    salary_structure_doc = None
    if source_doctype.lower() == "salary structure assignment":
        salary_structure_assignment = frappe.get_doc("Salary Structure Assignment", source_id)
        salary_structure_doc = frappe.get_doc("Salary Structure", salary_structure_assignment.salary_structure)
    else:
        salary_structure_assignments = frappe.get_all("Salary Structure Assignment",
                                                      filters={'employee': employee_id, 'company': employee_doc.company}
                                                      , fields=['*'], order_by="from_date desc")
        if salary_structure_assignments:
            salary_structure_doc = frappe.get_doc("Salary Structure", salary_structure_assignments[0].salary_structure)
        else:
            return

    shift_type_doc = None
    if source_doctype.lower() == "shift assignment":
        shift_assignment = frappe.get_doc("Shift Assignment", source_id)
        shift_type_doc = frappe.get_doc("Shift Type", shift_assignment.shift_type)
    else:
        shift_assignments = frappe.get_all("Shift Assignment",
                                           filters={'employee': employee_id, 'company': employee_doc.company}
                                           , fields=['*'], order_by="start_date desc")
        if shift_assignments:
            shift_type_doc = frappe.get_doc("Shift Type", shift_assignments[0].shift_type)
        else:
            return

    # TODO: consider Employee Transfer doctype records
    # TODO: to avoid duplicates, check if the combination already exists

    employee_change_log_record = frappe.new_doc('Lava Employee Payroll Changelog')
    employee_change_log_record.employee = employee_id
    employee_change_log_record.company = employee_doc.company
    employee_change_log_record.designation = employee_doc.designation
    employee_change_log_record.change_date = datetime.date.today()

    employee_change_log_record.salary_structure = salary_structure_doc.name
    employee_change_log_record.hour_rate = salary_structure_doc.hour_rate or 0

    employee_change_log_record.shift_type = shift_type_doc.name
    # TODO: To be check with Mr.Khaled attendance plan is not exist in Lava Employee Payroll Changelog or Shift type
    # employee_change_log_record.attendance_plan = shift_type_doc.attendance_plan

    employee_change_log_record.insert(ignore_permissions=True)
