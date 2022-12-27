import datetime
import json
from datetime import date

import frappe
from frappe.utils import now, time_diff_in_hours, get_datetime, getdate, time_diff_in_seconds, to_timedelta
# noinspection PyProtectedMember
from frappe import _dict as fdict
# noinspection PyProtectedMember
from frappe import _
from lava_custom.lava_custom.doctype.lava_biometric_attendance_record import lava_biometric_attendance_record


# from frappe.utils.logger import get_logger
# TODO: Add hook on employee & salary structure assignment on save event :
#  create 'employee chengelog record' with the new data
# TODO: Check if the employee transfer updates the employee record; then no need to ad hook into employee transfer too


def create_resume_batch(company: str, start_date: date, end_date: date, new_batch_id: str, batch_options):
    try:
        print("start the job")
        PayrollLavaDo.create_resume_batch_process(company=company, start_date=start_date,
                                                  end_date=end_date, new_batch_id=new_batch_id,
                                                  batch_options=batch_options)
        print("end the job")
    except Exception as ex:
        print(str(ex))
        frappe.log_error(message="Create/resume batch; Error message: '{}'".format(str(ex)),
                         title=PayrollLavaDo.batch_process_title)


class PayrollLavaDo:
    penalty_policy_groups = []
    penalty_policies = []
    shift_types = None
    debug_mode = False  # TODO: make it False for production
    payroll_activity_type = None
    batch_process_title = "LavaDo Payroll Process"
    batch_biometric_process_title = "run_biometric_attendance_records_process"

    @staticmethod
    def run_biometric_attendance_records_process(start_date, end_date):
        get_more_records = True
        limit_page_start = 0
        while get_more_records:
            checkin_records = frappe.get_all("Lava Biometric Attendance Record",
                                             {
                                                 'status': ["!=", 'Processed'],
                                                 'timestamp': ["between", [start_date.strftime('%Y-%m-%d'),
                                                                           end_date.strftime('%Y-%m-%d')]]
                                             }, limit_start=limit_page_start, limit_page_length=100)
            limit_page_start += 100
            if len(get_more_records) > 0:
                for checkin_record in checkin_records:
                    try:
                        checkin_record.process()
                    except Exception as ex:
                        frappe.log_error(message="record id: '{}', employee_biometric_id: '{}'. Error: '{}'".format(
                            checkin_record.name, checkin_record.employee_biometric_id, str(ex)),
                            title=PayrollLavaDo.batch_biometric_process_title)
            else:
                get_more_records = False

    @staticmethod
    def check_payroll_activity_type():
        if not frappe.db.exists("Activity Type", "payroll_activity_type"):
            activity_type = frappe.new_doc("Activity Type")
            activity_type.activity_type = "payroll_activity_type"
            activity_type.code = "pat"
            activity_type.insert(ignore_permissions=True)
        PayrollLavaDo.payroll_activity_type = "payroll_activity_type"

    @staticmethod
    def add_batch_to_background_jobs(company: str, start_date: date, end_date: date,
                                     new_batch_id: str, batch_options=None):
        try:
            if PayrollLavaDo.debug_mode:
                PayrollLavaDo.create_resume_batch_process(company=company, start_date=start_date,
                                                          end_date=end_date, new_batch_id=new_batch_id,
                                                          batch_options=batch_options)
            else:
                print("add the background job")
                frappe.enqueue(method='payroll_lavado.payroll_batch.create_resume_batch',
                               queue="long", timeout=36000000,
                               company=company, start_date=start_date,
                               end_date=end_date, new_batch_id=new_batch_id, batch_options=batch_options)
                print("added the  background job")
        except Exception as ex:
            print(str(ex))
            PayrollLavaDo.update_last_running_batch_in_progress(status="Failed")
            frappe.log_error(
                message="add the background job or run direct process; Error message: '{}'".format(str(ex)),
                title=PayrollLavaDo.batch_process_title)

    @staticmethod
    def update_last_running_batch_in_progress(batch_new_status: str = "Completed", batch_id=None):
        batch_doc = None
        try:
            if batch_id:
                batch_doc = frappe.get_doc("Lava Payroll LavaDo Batch", batch_id)
            else:
                batch_doc = frappe.get_doc("Lava Payroll LavaDo Batch", filters={"status": "In Progress"},
                                           order_by='modified desc', limit_page_legnth=1)
            if batch_doc:
                batch_doc.status = batch_new_status
                batch_doc.batch_process_end_time = datetime.datetime.now()
                batch_doc.save()
        except Exception as ex:
            print(str(ex))

    @staticmethod
    def get_penalty_policy_groups():
        if PayrollLavaDo.penalty_policy_groups:
            PayrollLavaDo.penalty_policy_groups.clear()
        PayrollLavaDo.penalty_policy_groups = frappe.get_all("Lava Penalty Group",
                                                             order_by='title', fields=["*"],
                                                             limit_start=0,
                                                             limit_page_length=100)
        return

    @staticmethod
    def get_shift_types():
        if PayrollLavaDo.shift_types:
            PayrollLavaDo.shift_types.clear()
        PayrollLavaDo.shift_types = frappe.get_all("Shift Type", fields=["*"],
                                                   limit_start=0, limit_page_length=100)
        return

    @staticmethod
    def get_policy_group_by_id(policy_group_id, groups=None):
        if not groups:
            groups = PayrollLavaDo.penalty_policy_groups

        for group in groups:
            if group.name == policy_group_id:
                return group
        return None

    @staticmethod
    def get_shift_type_by_id(shift_type_id, shift_types=None):
        if not shift_types:
            shift_types = PayrollLavaDo.shift_types

        for shift_type in shift_types:
            if shift_type.name == shift_type_id:
                return PayrollLavaDo.get_shift_type_doc(shift_type_id)
        return None

    @staticmethod
    def get_shift_type_doc(shift_type_id):
        return frappe.get_doc('Shift Type', shift_type_id)

    @staticmethod
    def get_policy_by_id(policy_id, policies=None):
        if not policies:
            policies = PayrollLavaDo.penalty_policies

        for policy in policies:
            if policy['policy_name'] == policy_id:
                return policy
        return None

    @staticmethod
    def get_policy_by_filters(policies, group_name, subgroup_name, occurrence_number, gap_duration_in_minutes):
        # the policies are being assumed that they sorted by group, subgroup, tolerance_duration desc,
        # occurrence_number desc
        for policy in policies:
            if policy['penalty_group'].lower() == group_name.lower() and \
                    policy['policy_subgroup'].lower() == subgroup_name.lower() and \
                    policy['occurrence_number'] or 0 <= occurrence_number and \
                    policy['tolerance_duration'] < gap_duration_in_minutes:
                return policy
        return None

    @staticmethod
    def get_penalty_policies(company):
        if PayrollLavaDo.penalty_policies:
            PayrollLavaDo.penalty_policies.clear()
        rows = frappe.db.sql("""SELECT 
                                p.name, p.title AS policy_title, p.penalty_group, p.occurrence_number,
                                p.deduction_in_days, p. deduction_amount,
                                p.penalty_subgroup,
                                p.tolerance_duration,
                                d.designation AS designation_name,
                                g.deduction_rule, g.reset_duration
                            FROM 
                                `tabLava Penalty Policy` AS p 
                            INNER JOIN `tabPolicy Designations` AS d
                                ON p.name = d.parent
                            INNER JOIN `tabLava Penalty Group` AS g
                                ON p.penalty_group = g.name
                            WHERE
                                p.enabled= 1 AND p.company = %(company)s
                            ORDER BY p.penalty_group, p.penalty_subgroup, p.tolerance_duration desc,
                             p.occurrence_number desc
                                """, {'company': company}, as_dict=1)
        last_parent_policy = None
        for row in rows:
            if not last_parent_policy or last_parent_policy['policy_name'] != row.name:
                last_parent_policy = {'policy_name': row.name,
                                      'policy_title': row.policy_title,
                                      'penalty_group': row.penalty_group.lower(),
                                      'deduction_rule': row.deduction_rule.lower(),
                                      'reset_duration': row.reset_duration,
                                      'occurrence_number': row.occurence_number,
                                      'policy_subgroup': row.penalty_subgroup.lower(),
                                      'deduction_in_days': row.deduction_in_days,
                                      'deduction_amount': row.deduction_amount,
                                      'tolerance_duration': row.tolerance_duration,
                                      'designations': []}
                PayrollLavaDo.penalty_policies.append(last_parent_policy)
            last_parent_policy['designations'].append({'designation_name': row.designation_name})

    @staticmethod
    def create_resume_batch_process(company: str, start_date: date, end_date: date,
                                    new_batch_id: str, batch_options):
        if batch_options['chk-clear-action-log-records']:
            PayrollLavaDo.add_action_log("clear action log records", "Log")
            frappe.db.truncate("Lava Action Log")
            frappe.db.commit()
            PayrollLavaDo.add_action_log("cleared action log records", "Log")

        frappe.publish_realtime('msgprint', 'Starting create_resume_batch_process...')
        # this is the main entry point of the entire batch, and it can be called from a UI screen on desk,
        # or from a background scheduled job

        if batch_options['chk-clear-error-log-records']:
            PayrollLavaDo.add_action_log("clear old error records", "Log")
            frappe.db.sql(f"""
                            delete from `tabError Log` where method in ('{PayrollLavaDo.batch_biometric_process_title}',
                             '{PayrollLavaDo.batch_process_title}')
                            """)
            frappe.db.commit()
            PayrollLavaDo.add_action_log("cleared old error records", "Log")

        if batch_options['chk-batch-objects']:
            PayrollLavaDo.add_action_log("clear batch object records", "Log")
            frappe.db.truncate("Lava Batch Object")
            frappe.db.commit()
            PayrollLavaDo.add_action_log("cleared batch object records", "Log")

        if batch_options['chk-biometric-process']:
            PayrollLavaDo.add_action_log("Start processing the new and failed biometric attendance records ", "Log")
            PayrollLavaDo.run_biometric_attendance_records_process(start_date=start_date, end_date=end_date)
            PayrollLavaDo.add_action_log("End processing the new and failed biometric attendance records ", "Log")

        PayrollLavaDo.add_action_log("Start batch", "Log")

        batch_id = ""
        last_processed_employee_id = ""
        PayrollLavaDo.get_penalty_policy_groups()
        PayrollLavaDo.get_penalty_policies(company)
        PayrollLavaDo.get_shift_types()
        PayrollLavaDo.check_payroll_activity_type()

        PayrollLavaDo.add_action_log(action="Start validating shift types.")

        try:
            PayrollLavaDo.validate_shift_types(PayrollLavaDo.shift_types)
        except Exception as ex:
            PayrollLavaDo.add_action_log(
                action="Due to the batches validation issue, exit the Batch Process for Company: {}, start date: {}, "
                       "end date: {}".format(company, start_date, end_date))
            frappe.log_error(message="validate_shift_types; Error message: '{}'".format(str(ex)),
                             title=PayrollLavaDo.batch_process_title)

        PayrollLavaDo.create_employees_first_changelog_records(company)
        PayrollLavaDo.add_action_log(
            action="Created the first employees changelog records if missing, Batch Process for Company: {}, "
                   "start date: {}, "
                   "end date: {}".format(company, start_date, end_date))

        PayrollLavaDo.add_action_log(
            action="Decide to resume or create a new Batch Process for Company: {}, start date: {},"
                   "end date: {}".format(company, start_date, end_date))
        progress_batches = frappe.get_all("Lava Payroll LavaDo Batch", {"status": "In Progress", "company": company},
                                          ['name', 'start_date', 'end_date'], limit_page_length=40)
        if len(progress_batches) > 1:
            exp_msg = _("Company {} has more than a batch in progress, Please check".format(company))
            frappe.log_error(message="Error message: '{}'".format(exp_msg), title=PayrollLavaDo.batch_process_title)

            frappe.throw(exp_msg)

        elif len(progress_batches) == 1 and new_batch_id == progress_batches[0].name:
            batch_id = progress_batches[0].name
            if progress_batches[0].start_date != start_date or progress_batches[0].end_date != end_date:
                exp_msg = _(
                    "Company {} has  batch {} in progress but with different date range than requested,"
                    " Please check".format(
                        company, progress_batches[0].name))
                frappe.log_error(message="Error message: '{}'".format(exp_msg), title=PayrollLavaDo.batch_process_title)

                frappe.throw(exp_msg)
            else:
                old_batch = frappe.get_doc("Lava Payroll LavaDo Batch", batch_id)
                old_batch.batch_process_start_time = datetime.datetime.now()
                old_batch.batch_process_end_time = None
                old_batch.status = "In Progress"
                old_batch.save()
                last_processed_employee_id = PayrollLavaDo.get_batch_last_processed_employee_id(batch_id)
                PayrollLavaDo.add_action_log(action="Resume batch {} for company: {}".format(batch_id, company))
                if last_processed_employee_id:
                    PayrollLavaDo.delete_employee_batch_records(last_processed_employee_id, batch_id)
                    PayrollLavaDo.add_action_log(
                        action="Batch: {} for Company: {} removed the records of"
                               " employee {}".format(batch_id, company,
                                                     last_processed_employee_id))

        if batch_id == "":
            new_batch = frappe.new_doc("Lava Payroll LavaDo Batch")
            new_batch.company = company
            new_batch.start_date = start_date
            new_batch.batch_process_start_time = datetime.datetime.now()
            new_batch.batch_process_end_time = None
            new_batch.status = "In Progress"
            new_batch.end_date = end_date
            new_batch.save(ignore_permissions=True)
            batch_id = new_batch.name
            PayrollLavaDo.add_action_log(
                action="Batch: {} for Company: {} created".format(batch_id, company))
        PayrollLavaDo.add_action_log(
            action="Batch: {} starting auto attendance".format(batch_id))
        for shift_type in PayrollLavaDo.shift_types:
            if shift_type.enable_auto_attendance:
                PayrollLavaDo.add_action_log(
                    action="Batch: {} run auto attendance of shift type '{}'".format(batch_id, shift_type.name))
                try:
                    PayrollLavaDo.run_standard_auto_attendance(shift_type)
                except Exception as ex:
                    frappe.log_error(
                        message="Auto attendance of Shift Type: '{}' Error message: '{}'".format(shift_type.name,
                                                                                                 str(ex)),
                        title=PayrollLavaDo.batch_process_title)

        PayrollLavaDo.process_employees(batch_id, company, start_date, end_date, last_processed_employee_id)
        PayrollLavaDo.add_action_log(
            action="Batch: {} completed and will update the status".format(batch_id))
        PayrollLavaDo.update_last_running_batch_in_progress(batch_new_status="Completed", batch_id=batch_id)
        frappe.publish_realtime('msgprint', 'Ending create_resume_batch_process...')

    @staticmethod
    def add_action_log(action: str, action_type: str = "LOG", notes: str = None):
        print(now(), action, action_type)
        new_doc = frappe.new_doc("Lava Action Log")
        new_doc.action = action
        new_doc.action_type = action_type
        new_doc.notes = notes
        new_doc.insert(ignore_permissions=True)
        # new_doc.commit

    @staticmethod
    def get_batch_last_processed_employee_id(batch_id):
        try:
            employee_id = frappe.get_last_doc('Lava Batch Object', {"batch_id": batch_id, "object_type": 'Employee'},
                                              ).object_id
        except frappe.DoesNotExistError:
            employee_id = None

        return employee_id

    @staticmethod
    def delete_employee_batch_records(employee_id: str, batch_id: str):
        batch_related_doctypes = ["Timesheet", "Lava Penalty Record", "Additional Salary"]
        get_more_records = True
        limit_page_start = 0
        while get_more_records:
            employee_batch_records = frappe.get_all("Lava Batch Object",
                                                    {'object_type': ['in', batch_related_doctypes],
                                                     'batch_id': batch_id},
                                                    ['object_type', 'object_id'], limit_start=limit_page_start,
                                                    limit_page_length=100)
            limit_page_start += 100
            if len(employee_batch_records) > 0:
                for record in employee_batch_records:
                    frappe.delete_doc(doctype=record.object_type, name=record.object_id, force=1)
            else:
                get_more_records = False

        frappe.delete_doc(doctype="Employee", name=employee_id, force=1)

    @staticmethod
    def validate_shift_types(shift_types):
        invalid_shift_types = []
        for shift_type in shift_types:
            if (not shift_type.enable_auto_attendance or not shift_type.process_attendance_after
                    or not shift_type.last_sync_of_checkin):
                invalid_shift_types.append(shift_type.name)

        if invalid_shift_types:
            invalid_shift_types_ids = ""
            for invalid_shift_type in invalid_shift_types:
                invalid_shift_types_ids += "," + invalid_shift_type.name
            exp_msg = "Shift types ({}) have missing data".format(invalid_shift_types_ids)
            frappe.log_error(message="Error message: '{}'".format(exp_msg), title=PayrollLavaDo.batch_process_title)

    @staticmethod
    def process_employees(batch_id: str, company: str, start_date: date, end_date: date,
                          last_processed_employee_id: str = None):
        employees = PayrollLavaDo.get_company_employees(company, batch_id, last_processed_employee_id)
        for employee in employees:
            PayrollLavaDo.create_batch_object_record(batch_id=batch_id, object_type="Employee",
                                                     object_id=employee.employee_id, status="In progress", notes="")
            PayrollLavaDo.add_action_log(
                action="Start process employee: {} for company: {} into batch : {}".format(employee.employee_id,
                                                                                           company,
                                                                                           batch_id))

            attendance_list = PayrollLavaDo.get_attendance_list(employee.employee_id, start_date, end_date)
            employee_changelog_records = PayrollLavaDo.get_employee_changelog_records(max_date=end_date,
                                                                                      employee_id=employee.employee_id)
            employee_timesheet = PayrollLavaDo.create_employee_timesheet(employee_id=employee.employee_id,
                                                                         company=company
                                                                         )

            for attendance in attendance_list:
                employee_changelog_record = PayrollLavaDo.get_employee_changelog_record(
                    attendance_date=attendance.attendance_date,
                    employee_change_log_records=employee_changelog_records)
                if not employee_changelog_record:
                    exception_msg = f"Skipping {attendance.name} for the employee {attendance.employee} " \
                                    f"as he hasn't changelog for this date"
                    frappe.log_error(message=exception_msg, title=PayrollLavaDo.batch_process_title)
                    continue
                try:
                    PayrollLavaDo.calc_attendance_working_hours_breakdowns(attendance, employee_changelog_record)
                except Exception as ex:
                    frappe.log_error(message="calc_attendance_working_hours_breakdowns:"
                                             " Employee: {}, attendance ID: {}; Error message: '{}'".format(
                        employee.employee_id, attendance.name, str(ex)),
                        title=PayrollLavaDo.batch_process_title)
                try:
                    PayrollLavaDo.add_timesheet_record(employee_timesheet=employee_timesheet,
                                                       attendance=attendance,
                                                       activity_type=PayrollLavaDo.payroll_activity_type)
                except Exception as ex:
                    frappe.log_error(message="add_timesheet_record:"
                                             " Employee: {}, attendance ID: {};"
                                             " Error message: '{}'".format(employee.employee_id,
                                                                           attendance.name, str(ex)),
                                     title=PayrollLavaDo.batch_process_title)
                employee_applied_policies = PayrollLavaDo.get_employee_applied_policies(
                    employee_changelog_record['designation'] if employee_changelog_record else "")
                if employee_applied_policies:
                    PayrollLavaDo.add_penalties(employee_changelog_record, batch_id, attendance,
                                                employee_applied_policies)
                try:
                    employee_timesheet.save(ignore_permissions=True)
                    employee_timesheet.submit()
                    PayrollLavaDo.create_batch_object_record(batch_id=batch_id, object_type="Timesheet",
                                                             object_id=employee_timesheet.name,
                                                             status="In progress", notes="")
                except frappe.MandatoryError as mandatory_error_ex:
                    if "time_logs" in str(mandatory_error_ex):  # no need to save timesheet without time_logs
                        frappe.log_error(message="process employee: {}, save timesheet; Error message: '{}'".format(
                            employee.employee_id, str(mandatory_error_ex)), title=PayrollLavaDo.batch_process_title)
                except Exception as ex:
                    frappe.log_error(message="process employee: {}, save timesheet; Error message: '{}'".format(
                        employee.employee_id, str(ex)), title=PayrollLavaDo.batch_process_title)

            PayrollLavaDo.add_action_log(
                action="End process employee: {} for company: {} into batch : {}".format(employee.employee_id,
                                                                                         company,
                                                                                         batch_id))

    @staticmethod
    def add_penalties(employee_changelog_record, batch_id, attendance, applied_policies):
        existing_penalty_records = frappe.get_all("Lava Penalty Record",
                                                  filters={"employee": employee_changelog_record.employee,
                                                           "penalty_date": ['=', attendance.attendance_date]},
                                                  order_by='modified', limit_page_length=100)

        for existing_penalty_record in existing_penalty_records:
            policy = PayrollLavaDo.get_policy_by_id(existing_penalty_record.penalty_policy, policies=applied_policies)
            if policy:
                PayrollLavaDo.get_policy_and_add_penalty_record(
                    employee_changelog_record=employee_changelog_record,
                    attendance=attendance,
                    policy_group_obj=PayrollLavaDo.get_policy_group_by_id(policy.penalty_group),
                    policy_subgroup=policy.penalty_subgroup,
                    batch_id=batch_id,
                    applied_policies=applied_policies)

        if attendance.status.lower() == "absent":
            PayrollLavaDo.get_policy_and_add_penalty_record(
                employee_changelog_record=employee_changelog_record,
                attendance=attendance,
                policy_group_obj=PayrollLavaDo.get_policy_group_by_id("Attendance"),
                policy_subgroup="attendance absence",
                batch_id=batch_id,
                applied_policies=applied_policies)
        if attendance.late_entry:
            PayrollLavaDo.get_policy_and_add_penalty_record(
                employee_changelog_record=employee_changelog_record,
                attendance=attendance,
                policy_group_obj=PayrollLavaDo.get_policy_group_by_id("Attendance"),
                policy_subgroup="attendance check-in",
                batch_id=batch_id,
                applied_policies=applied_policies)
        if attendance.early_exit:
            PayrollLavaDo.get_policy_and_add_penalty_record(
                employee_changelog_record=employee_changelog_record,
                attendance=attendance,
                policy_group_obj=PayrollLavaDo.get_policy_group_by_id("Attendance"),
                policy_subgroup="attendance check-out",
                batch_id=batch_id,
                applied_policies=applied_policies)

    @staticmethod
    def get_policy_and_add_penalty_record(employee_changelog_record, attendance,
                                          policy_group_obj, policy_subgroup, batch_id, applied_policies):
        if not policy_group_obj:
            return None

        gap_duration_in_minutes = 0
        if policy_subgroup.lower() == "attendance check-in":
            gap_duration_in_minutes = attendance.lava_entry_duration_difference
        elif policy_subgroup.lower() == "attendance check-out":
            gap_duration_in_minutes = attendance.lava_exit_duration_difference
        policy_occurrence_number = PayrollLavaDo.get_penalty_records_number_within_duration(
            employee=employee_changelog_record.employee,
            check_date=attendance.attendance_date,
            duration_in_days=policy_group_obj.reset_duration,
            policy_subgroup=policy_subgroup) + 1
        occurred_policy = PayrollLavaDo.get_policy_by_filters(
            policies=applied_policies,
            group_name=policy_group_obj.name,
            subgroup_name=policy_subgroup,
            occurrence_number=policy_occurrence_number,
            gap_duration_in_minutes=gap_duration_in_minutes)
        if occurred_policy:
            PayrollLavaDo.add_penalty_record(employee_changelog_record=employee_changelog_record,
                                             batch_id=batch_id,
                                             attendance=attendance,
                                             policy=occurred_policy,
                                             policy_occurrence_number=policy_occurrence_number,
                                             existing_penalty_record=None)

    @staticmethod
    def get_penalty_records_number_within_duration(employee, check_date, duration_in_days, policy_subgroup):
        # TODO: consider the correction records
        from_date = check_date - datetime.timedelta(days=duration_in_days)
        penalty_records_number_within_duration = frappe.get_all("Lava Penalty Record",
                                                                filters={
                                                                    "employee": employee,
                                                                    "penalty_date": ['>=', from_date],
                                                                    "policy_subgroup": policy_subgroup
                                                                },
                                                                order_by='penalty_date', limit_page_length=100)
        return len(penalty_records_number_within_duration)

    @staticmethod
    def get_employee_applied_policies(employee_designation):
        applied_policies = []
        for policy in PayrollLavaDo.penalty_policies:
            for designation in policy['designations']:
                if designation['designation_name'] == employee_designation:
                    applied_policies.append(policy)
                    break
        return applied_policies

    @staticmethod
    def get_attendance_list(employee: str, start_date: date, end_date: date):
        return frappe.get_all("Attendance", filters={'employee': employee,
                                                     'attendance_date': ['between', (start_date, end_date)]},
                              fields=['*'], order_by="attendance_date asc", limit_page_length=365)

    @staticmethod
    def create_employees_first_changelog_records(company: str):
        # TODO: future enhancement: We can use the employee transfer doctype to enhance the result accuracy

        rows = frappe.db.sql("""
        SELECT 
          e.name AS employee_id,e.designation, e.company As employee_company,
          ssa.name AS salary_structure_assignment,
          ssa.from_date AS salary_structure_assignment_from_date,
          e.modified AS last_modified_date,
          sha.shift_type AS shift_assignment_shift_type,
          ss.hour_rate AS salary_structure_hour_rate
        FROM 
          `tabEmployee` AS e LEFT JOIN `tabSalary Structure Assignment` ssa
          ON e.name = ssa.employee and ssa.company = %(company)s
        LEFT JOIN `tabShift Assignment` AS sha
            ON sha.status = 'Active' 
            AND sha.employee = e.name 
            AND sha.company = e.company
        LEFT JOIN `tabSalary Structure` AS ss
            ON ssa.salary_structure = ss.name AND ss.company = %(company)s
        WHERE
          e.company = %(company)s
          AND e.name NOT IN
          (
            SELECT employee
            from `tabLava Employee Payroll Changelog`
            WHERE company = %(company)s
          )""", {'company': company}, as_dict=1)
        employees_with_missing_data = []
        for row in rows:
            if row.designation is None or row.salary_structure_assignment is None or not row.shift_assignment_shift_type:
                employees_with_missing_data.append(row.employee_id)
            else:
                employee_change_log_record = frappe.new_doc('Lava Employee Payroll Changelog')
                employee_change_log_record.employee = row.employee_id
                employee_change_log_record.company = row.employee_company
                employee_change_log_record.shift_type = row.shift_assignment_shift_type
                employee_change_log_record.change_date = row.salary_structure_assignment_from_date
                employee_change_log_record.designation = row.designation
                employee_change_log_record.attendance_plan = row.attendance_plan
                employee_change_log_record.salary_structure_assignment = row.salary_structure_assignment
                employee_change_log_record.hour_rate = row.salary_structure_hour_rate or 0
                employee_change_log_record.insert(ignore_permissions=True)

        if len(employees_with_missing_data) > 0:
            employees_with_missing_data_ids = ", ".join(employees_with_missing_data)
            exp_msg = f"Employees ({employees_with_missing_data_ids}) don't have designated salary " \
                      "structure assignment and/or shift."
            frappe.log_error(message=f"Error message: '{exp_msg}'", title=PayrollLavaDo.batch_process_title)

    @staticmethod
    def get_employee_changelog_records(max_date: date, employee_id: str):
        employee_changelogs = frappe.get_all("Lava Employee Payroll Changelog",
                                             filters={'employee': employee_id, 'change_date': ['<=', max_date]},
                                             order_by="change_date desc", fields=['*'], limit_page_length=100)
        return employee_changelogs

    @staticmethod
    def get_employee_changelog_record(attendance_date: date, employee_change_log_records):
        for record in employee_change_log_records:
            if record['change_date'] <= attendance_date:
                return record
        return None

    @staticmethod
    def get_company_employees(company: str, batch_id: str, last_processed_employee_id: str = None):
        employee_list_query_str = """ 
                        SELECT 
                            name AS employee_id 
                        FROM 
                            `tabEmployee` 
                        WHERE company= %(company)s
                        AND name NOT IN 
                            (SELECT object_id
                            FROM `tabLava Batch Object` 
                            WHERE  object_type = "Employee" 
                                AND batch_id = %(batch_id)s
                        """

        if last_processed_employee_id:
            employee_list_query_str += f" AND employee_name != %(last_processed_employee_name)s)"
        else:
            employee_list_query_str += ")"

        employee_list = frappe.db.sql(employee_list_query_str,
                                      {'company': company, 'batch_id': batch_id,
                                       'last_processed_employee_name': last_processed_employee_id},
                                      as_dict=1)

        return employee_list

    @staticmethod
    def create_batch_object_record(batch_id, object_type, object_id, status=None, notes=None):
        batch_object_record = frappe.new_doc("Lava Batch Object")
        batch_object_record.batch_id = batch_id
        batch_object_record.object_type = object_type
        batch_object_record.object_id = object_id
        batch_object_record.status = status
        batch_object_record.notes = notes
        batch_object_record.insert(ignore_permissions=True)
        print(batch_object_record.name)

    @staticmethod
    def run_standard_auto_attendance(shift_type: fdict):
        shift_doc = frappe.get_doc('Shift Type', shift_type.name)
        shift_doc.process_auto_attendance()
        frappe.db.commit()

    @staticmethod
    def create_employee_timesheet(employee_id, company):
        timesheet = frappe.new_doc('Timesheet')
        # TODO:Future enhancement avoid duplications in timesheet creation
        timesheet.employee = employee_id
        timesheet.company = company
        return timesheet

    @staticmethod
    def calc_attendance_working_hours_breakdowns(attendance, employee_changelog_record):
        attendance_doc = frappe.get_doc("Attendance", attendance.name)
        if attendance_doc.shift:
            shift_type = PayrollLavaDo.get_shift_type_by_id(attendance.shift)
            if attendance_doc.status != 'Absent' and attendance_doc.status != 'On Leave' and \
                    attendance_doc.docstatus != 1:
                if attendance_doc.late_entry:
                    attendance_doc.lava_entry_duration_difference = time_diff_in_seconds(
                        get_datetime(attendance_doc.in_time), get_datetime(shift_type.start_time)) / 60
                if attendance_doc.early_exit:
                    attendance_doc.lava_exit_duration_difference = time_diff_in_seconds(
                        get_datetime(shift_type.end_time, attendance_doc.out_time)) / 60
            shift_time_diff = time_diff_in_seconds(shift_type.end_time, shift_type.start_time) / 60
            if shift_time_diff >= 0:
                attendance_doc.lava_planned_working_hours = time_diff_in_hours(shift_type.end_time,
                                                                               shift_type.start_time)
            else:
                attendance_doc.lava_planned_working_hours = time_diff_in_hours(to_timedelta("23:59:59"),
                                                                               shift_type.start_time) / 3600
                attendance_doc.lava_planned_working_hours += time_diff_in_hours(shift_type.end_time,
                                                                                to_timedelta("00:00:00")) / 3600
            attendance_doc.save(ignore_permissions=True)

    @staticmethod
    def add_timesheet_record(employee_timesheet, attendance, activity_type=None):

        if attendance.working_hours == 0:
            return
        frappe.db.delete("Timesheet Detail", filters={
            "parenttype": "Timesheet",
            "parentfield": "time_logs",
            "parent": attendance.name,
            "activity_type": activity_type,
            "from_time": attendance.in_time
        })
        employee_timesheet.append("time_logs", {
            "activity_type": activity_type,
            "hours": attendance.working_hours,
            "expected_hours": attendance.lava_planned_working_hours,
            "from_time": attendance.in_time,
            # "to_time": ,
            "description": "Created by Payroll LavaDo Batch Process"
        })

    @staticmethod
    def add_penalty_record(employee_changelog_record, batch_id,
                           attendance, policy,
                           policy_occurrence_number,
                           existing_penalty_record):

        penalty_record = existing_penalty_record
        if not penalty_record:
            penalty_record = frappe.new_doc("Lava Penalty Record")

        deduction_in_days_amount = employee_changelog_record.hourly_rate * policy['deduction_in_days']
        deduction_absolute_amount = policy.get('deduction_amount', 0)

        if policy['deduction_rule'] == "biggest":
            if deduction_absolute_amount > deduction_in_days_amount:
                applied_penalty_deduction_amount = deduction_absolute_amount
            else:
                applied_penalty_deduction_amount = deduction_in_days_amount
        elif policy['deduction_rule'] == "smallest":
            if deduction_absolute_amount < deduction_in_days_amount:
                applied_penalty_deduction_amount = deduction_absolute_amount
            else:
                applied_penalty_deduction_amount = deduction_in_days_amount
        elif policy['deduction_rule'] == "absolute amount":
            applied_penalty_deduction_amount = deduction_absolute_amount
        elif policy['deduction_rule'] == "deduction in days":
            applied_penalty_deduction_amount = deduction_in_days_amount
        else:
            exp_msg = "Unknown deduction rule '{}' for attendance date {} for employee {}".format(
                policy.deduction_rule,
                attendance.attendance_date,
                employee_changelog_record.employee
            )
            frappe.log_error(message="add_penalty_record; Error message: '{}'".format(exp_msg),
                             title=PayrollLavaDo.batch_process_title)
            return

        penalty_record.employee = employee_changelog_record.employee
        penalty_record.penalty_policy = policy['policy_name']
        penalty_record.penalty_date = attendance.attendance_date
        penalty_record.occurrence_number = policy_occurrence_number
        penalty_record.penalty_amount = applied_penalty_deduction_amount
        penalty_record.action_type = "Automatic"
        penalty_record.notes = ""
        penalty_record.lava_payroll_batch = batch_id

        penalty_record.save(ignore_permissions=True)

        PayrollLavaDo.create_batch_object_record(batch_id=batch_id, object_type="Lava Penalty Record",
                                                 object_id=penalty_record.name,
                                                 status="Created", notes="")
        if applied_penalty_deduction_amount > 0:
            PayrollLavaDo.add_additional_salary(penalty_record, batch_id)

    @staticmethod
    def add_additional_salary(penalty_record, batch_id):
        additional_salary_record = frappe.new_doc("Additional Salary")
        additional_salary_record.employee = penalty_record.employee
        additional_salary_record.payroll_date = penalty_record.penalty_date
        additional_salary_record.salary_component = "HR Policy Deduction"  # TODO: Add by patch
        additional_salary_record.overwrite_salary_structure_amount = 0
        additional_salary_record.amount = penalty_record.penalty_amount
        additional_salary_record.reason = f"Apply policy {penalty_record.penalty_policy}," \
                                          f" occurrence number {penalty_record.occurrence_number}."
        additional_salary_record.save(ignore_permissions=True)  # TODO: Submit
        PayrollLavaDo.create_batch_object_record(batch_id=batch_id, object_type="Additional Salary",
                                                 object_id=additional_salary_record.name,
                                                 status="Created", notes="")


@frappe.whitelist()
def run_payroll_lavado_batch(doc: str):
    doc_dict = json.loads(doc)
    company = doc_dict['company']
    start_date = getdate(doc_dict['start_date'])
    end_date = getdate(doc_dict['end_date'])
    batch_options = {
        "chk-clear-error-log-records": doc_dict['chk-clear-error-log-records'],
        "chk-clear-action-log-records": doc_dict['chk-clear-action-log-records'],
        "chk-batch-objects": doc_dict['chk-batch-objects'],
        "chk-biometric-process": doc_dict['chk-biometric-process']
    }
    result_status = ""
    try:
        PayrollLavaDo.add_batch_to_background_jobs(company=company,
                                                   start_date=start_date,
                                                   end_date=end_date,
                                                   new_batch_id="dummy id",
                                                   batch_options=batch_options)
        result_status = "Success"
    except Exception as ex:
        result_status = f"Failed '{ex}'"
        frappe.log_error(message=f"error occurred, '{ex}'", title=PayrollLavaDo.batch_process_title)
    finally:
        return result_status
