from datetime import datetime
from typing import List
import frappe
from frappe.utils import cint, now
from frappe.utils.logger import get_logger


# time sheet  , timesheet record and additional salary and penalty_record should be linked with batch no
# We can addd a button in shift type screen to run the batch
# FIXME: ask about the difference between "Last Sync of Checkin" and "Process Attendance After" in the shift type
# add screen payroll lavado screen
# - user should select the company
# - select start date
# - select end date

class PayrollLavaDo:
    penalty_policy_groups = None
    penalty_policies = None

    @staticmethod
    def get_penalty_policy_groups():
        pass
        # TODO: clear and then load the list

    @staticmethod
    def get_penalty_policies():
        pass
        # TODO: clear and then load the list and it's sub list of the designations

    @staticmethod
    def create_resume_batch(company: str, start_date: datetime, end_date: datetime):
        # this is the main entry point of the entire batch, and it can be called from a UI screen on desk,
        # or from a background scheduled job
        batch_id = ""
        last_processed_employee_id = ""
        PayrollLavaDo.penalty_policy_groups = PayrollLavaDo.get_penalty_policy_groups()
        PayrollLavaDo.penalty_policies = PayrollLavaDo.get_penalty_policies()

        PayrollLavaDo.add_action_log(action="Start validating shift types.")
        shift_types = frappe.get_all("Shift Type", ['name', 'enable_auto_attendance', 'process_attendance_after',
                                                    'last_sync_of_checkin'])
        try:
            PayrollLavaDo.validate_shift_types(shift_types)
        except:
            PayrollLavaDo.add_action_log(
                action="Due to the batches validation issue, exit the Batch Process for Company: {}, start date: {}, "
                       "end date: {}".format(
                    company,
                    start_date,
                    end_date))
            return

        PayrollLavaDo.create_employees_first_changelog_records(company)
        PayrollLavaDo.add_action_log(
            action="Created the first employees changelog records if missing, Batch Process for Company: {}, "
                   "start date: {}, "
                   "end date: {}".format(
                company,
                start_date,
                end_date))

        PayrollLavaDo.add_action_log(
            action="Decide to resume or create a new Batch Process for Company: {}, start date: {}, end date: {}".format(
                company,
                start_date,
                end_date))
        progress_batches = frappe.get_all("Lava Payroll LavaDo", {"status": "In Progress", "company": company},
                                          ['start_date', 'end_date', 'last_processed_employee'])
        if len(progress_batches) > 1:
            exp_msg = frappe._("Company {} has more than a batch in progress, Please check".format(company))
            get_logger(exp_msg)
            frappe.throw(exp_msg)

        elif len(progress_batches) == 1:
            if progress_batches[0].start_date != start_date or progress_batches[0].end_date != end_date:
                exp_msg = frappe._(
                    "Company {} has  batch {} in progress but with different date range than requested, Please check".format(
                        company, progress_batches[0].name))
                get_logger(exp_msg)
                frappe.throw(exp_msg)
        else:
            batch_id = progress_batches[0].name
            last_processed_employee_id = progress_batches[0].last_processed_employee
            PayrollLavaDo.add_action_log(action="Resume batch {} for company: {}".format(batch_id, company))
            PayrollLavaDo.delete_last_processed_employee_batch_records(last_processed_employee_id)
            PayrollLavaDo.add_action_log(
                action="Batch: {} for Company: {} removed the records of employee {}".format(batch_id, company,
                                                                                             last_processed_employee_id))

        if batch_id == "":
            new_batch = frappe.new_doc("Lava Payroll LavaDo")
            new_batch.company = company
            new_batch.start_date = frappe.utils.now()
            new_batch.status = "In Progress"
            new_batch.save(ignore_permissions=True)
            batch_id = new_batch.name
            PayrollLavaDo.add_action_log(
                action="Batch: {} for Company: {} created".format(batch_id, company))
        PayrollLavaDo.add_action_log(
            action="Batch: {} starting auto attendance".format(batch_id))
        PayrollLavaDo.run_standard_auto_attendance(shift_types)

        PayrollLavaDo.process_employees(batch_id, company, start_date, end_date, last_processed_employee_id)
        PayrollLavaDo.add_action_log(
            action="Batch: {} completed and will update the status".format(batch_id))
        PayrollLavaDo.update_batch_status(batch_id)

    @staticmethod
    def add_action_log(action: str, action_type: str = ""):
        # TODO: record records to trace actions
        print(now(), action, action_type)

    @staticmethod
    def delete_last_processed_employee_batch_records(employee, batch_id):
        related_doctypes = ["TimeSheet", "Penalty Policy Record", "Additional Salary"]
        filters = {"employee": employee, "lava_payroll_batch": batch_id}

        # TODO:separate the logic

    @staticmethod
    def validate_shift_types(shift_types):
        invalid_shift_types = []
        for shift_type in shift_types:
            if (not shift_type.enable_auto_attendance
                    or not shift_type.process_attendance_after
                    or not shift_type.last_sync_of_checkin
            ):
                invalid_shift_types.append(shift_type.name)

        if invalid_shift_types:
            exp_msg = "Shift types {} are missing data".format(shift_types)
            frappe.throw(frappe._(exp_msg))
            get_logger(exp_msg)

    @staticmethod
    def process_employees(batch_id: str, company: str, start_date: datetime, end_date: datetime,
                          last_processed_employee_id: str = None):
        employees = PayrollLavaDo.get_company_employees(company, last_processed_employee_id)
        for employee in employees:
            PayrollLavaDo.update_batch(batch_id, employee)
            PayrollLavaDo.add_action_log(
                action="Start process employee: {} for company: {} into batch : {}".format(employee.name, company,
                                                                                           batch_id))

            attendance_list = PayrollLavaDo.get_attendance_list(employee, start_date, end_date)
            employee_changelog_records = PayrollLavaDo.get_employee_changelog_records(max_date=end_date,
                                                                                      employee=employee)
            employee_timesheet = PayrollLavaDo.create_employee_timesheet(employee=employee, batch_id=batch_id)

            for attendance in attendance_list:
                # TODO: Should we check the salary structure as on that date (upon the employee changelog)
                employee_changelog_record = PayrollLavaDo.get_employee_changelog_record(
                    attendance_date=attendance.attendance_date,
                    employee_change_log_records=employee_changelog_records)
                # TODO: handle the exceptions
                PayrollLavaDo.calc_attendance_working_hours_breakdowns(attendance, employee_changelog_record)
                PayrollLavaDo.add_timesheet_record(employee_timesheet, attendance.attendance_date, activity_type=None,
                                                   duration_in_hours=attendance.working_hours)
                # TODO:Check if we need to override this field (attendance.working_hours) calculation
                employee_applied_policies = PayrollLavaDo.get_employee_applied_policies(employee_changelog_record['designation'])
                PayrollLavaDo.add_penalties(employee_changelog_record, batch_id, attendance, employee_applied_policies)

                PayrollLavaDo.add_action_log(
                    action="End process employee: {} for company: {} into batch : {}".format(employee.name, company,
                                                                                             batch_id))

    @staticmethod
    def add_penalties(employee_changelog_record, batch_id, attendance, applied_policies):
        pass  # TODO
        # for penalty_record in the date for employee :
        #     PayrollLavaDo.add_additional_salary(penalty_record) if needed
        # for penalty_policy related to employee(designation in penalty policy equals designation in the employee_record):

        for policy in applied_policies:
            pass
        #     adding logic of applying penalty policy
        #     PayrollLavaDo.add_penalty_record(  employee)

    @staticmethod
    def get_employee_applied_policies(employee_designation):
        applied_policies = []
        for policy in PayrollLavaDo.penalty_policies:
            if policy['designation'] == employee_designation:
                applied_policies.append(policy)
        return applied_policies

    @staticmethod
    def update_batch(batch_id, employee):
        frappe.set_value("Lava Payroll LavaDo", batch_id, "last_processed_employee", employee)

    @staticmethod
    def get_attendance_list(employee: str, start_date: datetime, end_date: datetime):
        return frappe.get_all("Attendance",
                              {'employee': employee, 'attendance_date': ['between', [start_date, end_date]]},
                              ['*'])  # TODO: specify fields

    @staticmethod
    def create_employees_first_changelog_records(company: str):
        pass
        # TODO: get all employees that don't have changelog records, and then create changelog record for each one as
        #  per the current data. We can change the employee transfer doctype to enhance the result accuracy

    @staticmethod
    def get_employee_changelog_records(max_date: datetime, employee: str):
        employee_changelogs = frappe.get_all("Lava Employee Payroll Changelog",
                                             filters={'employee': employee, 'change_date': ['<=', max_date]},
                                             order_by="creation desc", fields=['*'])
        return employee_changelogs

    @staticmethod
    def get_employee_changelog_record(attendance_date: datetime, employee_change_log_records):
        for record in employee_change_log_records:
            if record['change_date'] <= attendance_date:
                return record
        return None

    @staticmethod
    def get_company_employees(company, last_processed_employee_id: str = None):
        # TODO: Handle last processed employee id; we may need to use another field like the system creation time if
        #  timestamped
        # TODO: pick the needed fields
        employee_list = frappe.get_all("Employee", {'company': company}, order_by='name')
        return employee_list

    @staticmethod
    def run_standard_auto_attendance(shift_types: List[dict]):
        for shift_type in shift_types:
            # TODO: call the standard attendance process
            pass

    @staticmethod
    def create_employee_timesheet(employee, batch_id):
        timesheet = None
        # TODO:Future enhancement avoid duplications in timesheet creation
        # TODO: create new timesheet record and assign the batch_id
        return timesheet

    @staticmethod
    def calc_attendance_working_hours_breakdowns(attendance):
        pass
        # TODO

    @staticmethod
    def add_timesheet_record(parent_timesheet, activity_type, date, duration_in_hours):
        pass
        # TODO

    @staticmethod
    def add_penalty_record(employee):
        return  # TODO
        # adds penalty record for the employee
        # call PayrollLavaDo.add_additional_salary(penalty_record) if needed

    @staticmethod
    def add_additional_salary(penalty_record):
        pass  # TODO
