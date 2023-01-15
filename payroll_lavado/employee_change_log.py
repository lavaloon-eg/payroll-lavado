import frappe
import datetime


def create_employee_change_log(employee_id: str, source_doctype: str, source_id: str):
    employee_doc = frappe.get_doc("Employee", employee_id)
    effective_change_date = None

    if source_doctype.lower() == "employee":
        employee_old_doc = employee_doc.get_doc_before_save()
        effective_change_date = datetime.date.today()
        if employee_old_doc:
            if employee_old_doc.designation.lower() == employee_doc.designation.lower() and \
                    employee_old_doc.company == employee_doc.company:
                return

    salary_structure_doc = None
    if source_doctype.lower() == "salary structure assignment":
        salary_structure_assignment = frappe.get_doc("Salary Structure Assignment", source_id)
        salary_structure_doc = frappe.get_doc("Salary Structure", salary_structure_assignment.salary_structure)
        # Salary structure has no attribute name from date so i changed it to salary structure assignment
        effective_change_date = salary_structure_assignment.from_date
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
        effective_change_date = shift_type_doc.start_date
    else:
        shift_assignments = frappe.get_all("Shift Assignment",
                                           filters={'employee': employee_id, 'company': employee_doc.company}
                                           , fields=['*'], order_by="start_date desc")
        if shift_assignments:
            shift_type_doc = frappe.get_doc("Shift Type", shift_assignments[0].shift_type)
        else:
            return

    existing_change_log_doc = frappe.get_last_doc("Lava Employee Payroll Changelog",
                                                  filters={"company": employee_doc.company, "employee": employee_id},
                                                  order_by="modified desc")
    if existing_change_log_doc:
        if existing_change_log_doc.designation == employee_doc.designation and \
                existing_change_log_doc.branch == employee_doc.branch and \
                existing_change_log_doc.salary_structure_assignment == salary_structure_doc.name and \
                existing_change_log_doc.shift_type == shift_type_doc.name:
            return  # no need to save a new record
        else:
            # delete the existing record to get ready for the new record on the same date
            if existing_change_log_doc.change_date == effective_change_date:
                frappe.delete_doc("Lava Employee Payroll Changelog", name=existing_change_log_doc.name)

    employee_change_log_record = frappe.new_doc('Lava Employee Payroll Changelog')
    employee_change_log_record.employee = employee_id
    employee_change_log_record.company = employee_doc.company
    employee_change_log_record.designation = employee_doc.designation
    employee_change_log_record.branch = employee_doc.branch

    employee_change_log_record.change_date = effective_change_date

    employee_change_log_record.salary_structure = salary_structure_doc.name
    employee_change_log_record.hourly_rate = salary_structure_doc.hour_rate or 0

    employee_change_log_record.shift_type = shift_type_doc.name

    employee_change_log_record.insert(ignore_permissions=True)
