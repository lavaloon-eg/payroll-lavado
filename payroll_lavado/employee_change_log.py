import frappe


def create_employee_change_log(employee_id: str, source_doctype=None):
    """
    invokes on doctypes update and insert event
    :param employee_id:
    :param source_doctype: (Employee / Salary Structure Assignment / Shift Assignment / Employee Transfer)
    """
    pass
