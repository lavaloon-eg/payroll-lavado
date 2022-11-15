from datetime import datetime
from typing import List

import frappe
from frappe.utils import cint, now
from frappe.utils.logger import get_logger


# time sheet  , timesheet record and additional salary and penaltyrecord should be llinkedwith batch no
# We can addd a button in shift type screen to run the batch
# FIXME: ask about the difference between "Last Sync of Checkin" and "Process Attendance After" in the shift type
# add screen payroll lavado screen
# -user should select the company
# - select start date
# - select end adte

class PayrollLavaDo:
    @staticmethod
    def create_resume_batch(company: str, start_date: datetime, end_date: datetime):
        PayrollLavaDo.add_action_log(action="Start validate shift types.")
        shift_types = frappe.get_all("Shift Type", ['name', 'enable_auto_attendance', 'process_attendance_after',
                                                    'last_sync_of_checkin'])
        PayrollLavaDo.validate_shift_types(shift_types)
        batch_id = ""
        last_processed_employee_id = ""
        PayrollLavaDo.add_action_log(
            action="Check to Start Batch Process for Company: {}, start date: {}, end date: {}".format(company,
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
    def delete_last_processed_employee_batch_records(employee, batch_no):
        related_doctypes = ["TimeSheet", "Penalty Policy Record", "Additional Salary"]
        filters = {"employee": employee, "lava_payroll_batch": batch_no}

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
            frappe.throw(frappe._("Shift types {} are missing data".format(shift_types)))
            get_logger("Shift types {} have missing data".format(invalid_shift_types))

    @staticmethod
    def process_employees(batch_id: str, company: str, start_date: datetime, end_date: datetime,
                          last_processed_employee_id: str = None):
        employees = PayrollLavaDo.get_company_employees(company, last_processed_employee_id)
        for employee in employees:
            PayrollLavaDo.update_batch(batch_id, employee)
            PayrollLavaDo.add_action_log(
                action="Start process employee: {} for company: {} into batch : {}".format(employee.name, company,
                                                                                           batch_id))

            # TODO: Should we check the salary structure as on that date (upon the employee changelog)
            attendance_list = PayrollLavaDo.get_attendance_list(employee, start_date, end_date)
            employee_timesheet = PayrollLavaDo.create_resume_employee_timesheet(employee, attendance_list, start_date,
                                                                                end_date)

            for attendance in attendance_list:
                employee_changelog_record = PayrollLavaDo.get_employee_changelog(
                    attendance_date=attendance.attendance_date,
                    employee=employee)
                PayrollLavaDo.calc_attendance_working_hours_breakdowns(attendance, employee_changelog_record)
                PayrollLavaDo.add_timesheet_record(employee_timesheet, attendance.attendance_date, activity_type=None,
                                                   duration_in_hours=attendance.working_hours)  # TODO:Check if we need to override this field calculation
                PayrollLavaDo.add_penalties(employee_changelog_record, batch_id, attendance.attendance_date)

                PayrollLavaDo.add_action_log(
                    action="End process employee: {} for company: {} into batch : {}".format(employee.name, company,
                                                                                             batch_id))

    @staticmethod
    def add_penalties(employee_changelog_record, batch_id, attendance_date):
        pass #TODO
        # for penalty_record in the date for employee :
        #     PayrollLavaDo.add_additional_salary(penalty_record) if needed
        # for penalty_policy related to employee(designation in penalty policy equals designation in the employee_record):
        #     """adding logic of appying penalty policy"""
        #     PayrollLavaDo.add_penalty_record(  employee)

    @staticmethod
    def update_batch(batch_id, employee):
        frappe.set_value("Lava Payroll LavaDo", batch_id, "last_processed_employee", employee)

    @staticmethod
    def get_attendance_list(employee: str, start_date: datetime, end_date: datetime):
        return frappe.get_all("Attendance",
                              {'employee': employee, 'attendance_date': ['between', [start_date, end_date]]},
                              ['*'])  # TODO: specify fields

    @staticmethod
    def get_employee_changelog(attendance_date: datetime, employee: str):
        employee_last_changelog = frappe.get_all("Lava Employee Payroll Changelog",
                                                 filters={'employee': employee, 'change_date': ['<=', attendance_date]},
                                                 order_by="creation desc", fields=['*'], limit=1)
        return employee_last_changelog

    @staticmethod
    def get_company_employees(company, last_processed_employee_id: str = None):
        # TODO: Handle last processed employee id
        employee_list = frappe.get_all("Employee", {'company': company}, order_by='name')
        return employee_list

    @staticmethod
    def run_standard_auto_attendance(shift_types: List[dict]):
        for shift_type in shift_types:
            # TODO: call the standard attendance
            pass

    @staticmethod
    def create_resume_employee_timesheet(employee, attendance_list, start_date, end_date):
        pass
        return {}
        # TODO:Future enhancement avoid duplications in timesheet creation
        # if the timesheet exists identical start_Date and end_date for the employee:
        #     get_timesheet_refrence
        #     update
        #     batch in timesheet
        # else:
        #     create
        # for attendance in attendance_list:
        #     calc_attendance_working_hours_breakdowns(attendance)
        #
        #     PayrollLavaDo.add_timesheet_record(attendance)
    @staticmethod
    def calc_attendance_working_hours_breakdowns(attendance):
        pass
        #TODO
    @staticmethod
    def add_timesheet_record(parent_timesheet, activity_type, date, duration_in_hours):
        pass
        #TODO

    @staticmethod
    def add_penalty_record(employee):
        return #TODO
        # adds penalties records for the employee
        # call PayrollLavaDo.add_additional_salary(penalty_record) if needed

    @staticmethod
    def add_additional_salary(penalty_record):
        pass #TODO


#*************************** OLD ****************************************#
# for employee in get_all employees filtered by company and
# if last_proccessed employee > employee and resuming_abatch and it is the first employee in the new batch:
#     delete_employee_batch_record(employee,
#                                  batch_ref)  # delete penalty records and additional salary timesheets linked to penalty record
# TODO: Should we check the salary structure as on that date (upon the employee changelog)
#
# for attendance in employee.get_attendance_list(between start_Date and end_Date ):
#     employee_record = get
#     employee
#     changelog
#     record(date=most
#     recent < = record
#     near
#     the
#     attendance.date, employee)
#     PayrollLavaDo.create_resume_employee_timesheet(employee, attendance_list, start_date, end_date)
#
#     calc_attendance_working_hours_breakdowns(attendance)
#     PayrollLavaDo.add_timesheet_record(timesheet_record, activity_type=hardcoded, date, duration_in_hours="equation")
#     PayrollLavaDo.add_penalties(employee_record, batch, date)
# PayrollLavaDo.update_batch(employee)
#
# update_batch_status(batch)
#************************** END OF OLD *****************************************#

def create_timesheet_records(attendance_records):
    pass
    # 1 - Group
    # attendance
    # records
    # by
    # employee
    # for record in attendance_records:
        # 2- timesheets = reate time sheet for each employee then append all attendance records as child task in the time sheet.(start time is checkin time , end is checkout) #I think it is redundant
        # PayrollLavaDo.create_penalty_records(timesheets)
        # PayrollLavaDo.create_penalty_records(attendance_records)


def create_penalty_records(attendance_records):
    pass
    # penalty_list = []
    # for record in attendance_records:
    #     penalty = create
    #     penalty_record
    #     penalty_list.append(penalty)
    # PayrollLavaDo.create_deductions(penalty_list)


def create_deductions(penalty_list):
    pass
    # for penalty in penalties:
    #     get
    #     total
    #     deductions
    #     create
    #     deduction
    #     record(salary_additional for the total)
