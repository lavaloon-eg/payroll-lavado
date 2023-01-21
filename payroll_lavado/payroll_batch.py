import datetime
import json
from datetime import date
from pymysql import ProgrammingError
import frappe
# noinspection PyProtectedMember
from frappe import _
from frappe import _dict as fdict
from frappe.utils import time_diff_in_hours, getdate, time_diff_in_seconds, to_timedelta

# TODO: Remove 'lava net working hours from lava_custom and all the code using.

batch_process_title: str = "LavaDo Payroll Process"
batch_biometric_process_title: str = "run_biometric_attendance_records_process"


class PayrollLavaDoManager:
    penalty_policy_groups: list = []
    penalty_policies: list = []
    shift_types: list = []
    debug_mode: bool = False
    clear_action_log_records: bool = False
    clear_error_log_records: bool = False
    clear_batch_objects: bool = False
    run_biometric_attendance_process: bool = False
    run_auto_attendance_batch_options: bool = False
    payroll_activity_type: str = "payroll_activity_type"
    running_batch_id: str
    running_batch_company: str
    running_batch_start_date: date
    running_batch_end_date: date

    def add_batch_to_background_jobs(self, company: str, start_date: date, end_date: date, batch_options):
        self.debug_mode = True if (batch_options["chk-batch-debug-mode"] == 1) else False

        try:
            if self.debug_mode:
                self.create_resume_batch_process(company=company, start_date=start_date,
                                                 end_date=end_date,
                                                 batch_options=batch_options)
            else:
                frappe.enqueue(method='payroll_lavado.payroll_batch.run_payroll_batch_process',
                               queue="long", timeout=36000000,
                               company=company, start_date=start_date,
                               end_date=end_date, batch_options=batch_options)
        except Exception as ex:
            PayrollLavaDoManager.update_last_running_batch_in_progress(batch_new_status="Failed")
            frappe.log_error(
                message=f"add the background job or run direct process; "
                        f"Error message: '{format_exception(ex)}'", title=batch_process_title)

    @staticmethod
    def run_biometric_attendance_records_process(start_date, end_date):
        limit_page_start = 0
        limit_page_length = 100
        while True:
            try:
                checkin_records = frappe.get_all("Lava Biometric Attendance Record",
                                                 {
                                                     'status': ["!=", 'Processed'],
                                                     'timestamp': ["between", [start_date.strftime('%Y-%m-%d'),
                                                                               end_date.strftime('%Y-%m-%d')]]
                                                 }, limit_start=limit_page_start, limit_page_length=limit_page_length)
            except ProgrammingError as ex:
                if ex.args != ('DocType', 'Lava Biometric Attendance Record'):
                    raise
                frappe.log_error(message="'Lava Biometric Attendance Record' doctype not found. "
                                         "Make sure lava_custom is installed or disable biometric attendance "
                                         "processing",
                                 title=batch_biometric_process_title)
                break

            if not checkin_records:
                break

            limit_page_start += limit_page_length
            for checkin_record in checkin_records:
                try:
                    checkin_doc = frappe.get_doc("Lava Biometric Attendance Record", checkin_record.name)
                    checkin_doc.process()
                except Exception as ex:
                    frappe.log_error(message=f"record id: '{checkin_record.name}', "
                                             f"employee_biometric_id: '{checkin_record.employee_biometric_id}'. "
                                             f"Error: '{format_exception(ex)}'", title=batch_biometric_process_title)

    def check_payroll_activity_type(self):
        if not frappe.db.exists("Activity Type", self.payroll_activity_type):
            activity_type = frappe.new_doc("Activity Type")
            activity_type.activity_type = self.payroll_activity_type
            activity_type.code = "pat"
            activity_type.insert(ignore_permissions=True)

    @staticmethod
    def update_last_running_batch_in_progress(batch_new_status: str, batch_id=None) -> bool:
        try:
            if batch_id:
                batch_doc = frappe.get_doc("Lava Payroll LavaDo Batch", batch_id)
            else:
                batches = frappe.get_all("Lava Payroll LavaDo Batch", filters={"status": "In Progress"},
                                         order_by='modified desc', limit_page_length=1)
                if not batches:
                    raise Exception("Expected at least one in-progress payroll batch but found none")

                batch_doc = frappe.get_doc("Lava Payroll LavaDo Batch", batches[0].name)

            if batch_new_status == "Completed":
                if frappe.db.exists("Lava Batch Object", {"batch_id": batch_id,
                                                          "object_type": "Employee",
                                                          "status": ["in", ["Failed", "In progress"]]}):
                    batch_new_status = "Incomplete"
            batch_doc.status = batch_new_status
            batch_doc.batch_process_end_time = datetime.datetime.now()
            batch_doc.save()
        except Exception as ex:
            frappe.log_error(message=f"update_last_running_batch_in_progress, error: {format_exception(ex)}",
                             title=batch_process_title)
        return True

    def get_penalty_policy_groups(self):
        if self.penalty_policy_groups:
            self.penalty_policy_groups.clear()
        penalty_policy_group_records = frappe.get_all("Lava Penalty Group", filters={"docstatus": 1},
                                                      order_by='title', fields=["name"],
                                                      limit_start=0,
                                                      limit_page_length=200)

        for record in penalty_policy_group_records:
            doc = frappe.get_doc("Lava Penalty Group", record.name).as_dict()
            self.penalty_policy_groups.append(doc)
        return

    def get_shift_types(self):
        if self.shift_types:
            self.shift_types.clear()
        self.shift_types = frappe.get_all("Shift Type", fields=["*"],
                                          limit_start=0, limit_page_length=100)
        return

    def get_policy_group_by_id(self, policy_group_id, groups=None):
        if not groups:
            groups = self.penalty_policy_groups

        for group in groups:
            if group.name.lower() == policy_group_id.lower():
                return group
        return None

    def get_policy_group_by_policy_subgroup(self, policy_sub_group_name: str, policies: []):
        for policy in policies:
            if policy.policy_subgroup.lower() == policy_sub_group_name.lower():
                group = self.get_policy_group_by_id(policy.penalty_group)
                if group:
                    return group
        return None

    def get_shift_type_by_id(self, shift_type_id, check_shift_types=None):
        if not check_shift_types:
            check_shift_types = self.shift_types

        for shift_type in check_shift_types:
            if shift_type.name.lower() == shift_type_id.lower():
                return self.get_shift_type_doc(shift_type_id)
        return None

    def get_shift_type_doc(self, shift_type_id):
        return frappe.get_doc('Shift Type', shift_type_id)

    def get_policy_by_id(self, policy_id, policies=None):
        if not policy_id:
            frappe.throw(msg=f"get_policy_by_id: no policy id", title=batch_process_title)

        if not policies:
            policies = self.penalty_policies

        for policy in policies:
            if (policy['policy_name']).lower() == policy_id.lower():
                return policy
        return None

    def get_policy_by_filters(self, policies, group_name, subgroup_name, occurrence_number,
                              gap_duration_in_minutes):
        # the policies are being assumed that they sorted by group, subgroup, tolerance_duration desc,
        # occurrence_number desc
        for policy in policies:
            if policy['penalty_group'].lower() == group_name.lower() and \
                    policy['policy_subgroup'].lower() == subgroup_name.lower() and \
                    policy['occurrence_number'] <= occurrence_number:
                if policy['policy_subgroup'].lower() == 'attendance check-in' or \
                        policy['policy_subgroup'].lower() == 'attendance check-out':
                    if policy['tolerance_duration'] < gap_duration_in_minutes:
                        return policy
                else:
                    return policy
        return None

    def get_penalty_policies(self, company):
        if self.penalty_policies:
            self.penalty_policies.clear()
        rows = frappe.db.sql("""SELECT 
                                p.name, p.title AS policy_title, p.penalty_group, p.occurrence_number,
                                p.deduction_factor, p.deduction_amount,
                                p.penalty_subgroup,
                                p.tolerance_duration,
                                p.salary_component,
                                g.deduction_rule, g.reset_duration
                            FROM 
                                `tabLava Penalty Policy` AS p 
                            INNER JOIN `tabLava Penalty Group` AS g
                                ON p.penalty_group = g.name
                            WHERE
                                p.docstatus=1 AND p.enabled= 1 AND p.company = %(company)s
                            ORDER BY p.penalty_group, p.penalty_subgroup, p.tolerance_duration desc,
                             p.occurrence_number desc
                                """, {'company': company}, as_dict=1)
        for row in rows:
            policy = fdict({'policy_name': row.name,
                            'policy_title': row.policy_title,
                            'penalty_group': row.penalty_group.lower(),
                            'deduction_rule': row.deduction_rule.lower(),
                            'reset_duration': row.reset_duration,
                            'occurrence_number': row.occurrence_number,
                            'policy_subgroup': row.penalty_subgroup.lower(),
                            'deduction_factor': row.deduction_factor,
                            'deduction_amount': row.deduction_amount,
                            'tolerance_duration': row.tolerance_duration,
                            "salary_component": row.salary_component,
                            'designations': []})
            self.get_policy_designations(company=company, policy_obj=policy)
            self.penalty_policies.append(policy)

    def get_policy_designations(self, company, policy_obj):
        rows = frappe.db.sql("""SELECT 
                                    d.designation AS designation_name
                                FROM 
                                   `tabPolicy Designations` AS d
                                WHERE
                                    d.parent = %(policy_id)s
                                ORDER BY designation
                                    """, {'policy_id': policy_obj.policy_name}, as_dict=1)
        for row in rows:
            policy_obj['designations'].append({'designation_name': row.designation_name})

    def create_resume_batch_process(self, company: str, start_date: date, end_date: date, batch_options):
        batch_action_type = batch_options['action_type']
        self.running_batch_company = company
        self.running_batch_start_date = start_date
        self.running_batch_end_date = end_date

        self.debug_mode = True if (batch_options['chk-batch-debug-mode'] == 1) else False
        self.clear_action_log_records = True if (batch_options['chk-clear-action-log-records'] == 1) else False
        self.clear_error_log_records = True if (batch_options['chk-clear-error-log-records'] == 1) else False
        self.clear_batch_objects = True if (batch_options['chk-batch-objects'] == 1) else False
        self.run_auto_attendance_batch_options = True if (batch_options['chk-auto-attendance'] == 1) else False
        self.run_biometric_attendance_process = True if (batch_options['chk-biometric-process'] == 1) else False

        if self.clear_action_log_records and batch_action_type != "Resume Batch":
            frappe.db.truncate("Lava Action Log")
            frappe.db.commit()
            add_action_log("cleared action log records", "Log")

        frappe.publish_realtime('msgprint', 'Starting create_resume_batch_process...')
        # this is the main entry point of the entire batch, and it can be called from a UI screen on desk,
        # or from a background scheduled job

        if self.clear_error_log_records and batch_action_type != "Resume Batch":
            add_action_log("clear old error records", "Log")
            frappe.db.sql(f"""
                            delete from `tabError Log` where method in (%(first_title)s,
                             %(second_title)s)
                            """, {'first_title': batch_biometric_process_title,
                                  'second_title': batch_process_title})
            frappe.db.commit()
            add_action_log("cleared old error records", "Log")

        if self.clear_batch_objects and batch_action_type != "Resume Batch":
            add_action_log("clear batch object records", "Log")
            frappe.db.truncate("Lava Batch Object")
            frappe.db.commit()
            add_action_log("cleared batch object records", "Log")

        if self.run_biometric_attendance_process:
            add_action_log("Start processing the new and failed biometric attendance records ", "Log")
            PayrollLavaDoManager.run_biometric_attendance_records_process(start_date=self.running_batch_start_date,
                                                                          end_date=self.running_batch_end_date)
            add_action_log("End processing the new and failed biometric attendance records ", "Log")

        add_action_log("Start batch", "Log")

        self.get_penalty_policy_groups()
        self.get_penalty_policies(company)
        self.get_shift_types()
        self.check_payroll_activity_type()

        add_action_log(action="Start validating shift types.")
        try:
            self.validate_shift_types(self.shift_types)
        except Exception as ex:
            add_action_log(
                action=f"Due to the batches validation issue, "
                       f"exit the Batch Process for Company: {self.running_batch_company}, "
                       f"start date: {self.running_batch_start_date}, "
                       f"end date: {self.running_batch_end_date}")
            frappe.log_error(message=f"validate_shift_types; Error message: '{format_exception(ex)}'",
                             title=batch_process_title)

        self.create_employees_first_changelog_records(self.running_batch_company)
        add_action_log(
            action=f"Created the first employees changelog records if missing, "
                   f"Batch Process for Company: {self.running_batch_company}, "
                   f"start date: {self.running_batch_start_date}, "
                   f"end date: {self.running_batch_end_date}")

        self.running_batch_id = ""

        if batch_action_type == "Resume Batch":
            self.running_batch_id = batch_options["batch_id"]
            old_batch = frappe.get_doc("Lava Payroll LavaDo Batch", self.running_batch_id)
            old_batch.batch_process_start_time = datetime.datetime.now()
            old_batch.batch_process_end_time = None
            old_batch.status = "In Progress"
            old_batch.save()
            add_action_log(action=f"Resume batch {self.running_batch_id} for company: {self.running_batch_company}")
            self.delete_failed_processed_employees_records(batch_id=self.running_batch_id)
        else:
            new_batch = frappe.new_doc("Lava Payroll LavaDo Batch")
            new_batch.company = self.running_batch_company
            new_batch.start_date = self.running_batch_start_date
            new_batch.batch_process_start_time = datetime.datetime.now()
            new_batch.batch_process_end_time = None
            new_batch.status = "In Progress"
            new_batch.end_date = self.running_batch_end_date
            new_batch.save(ignore_permissions=True)
            self.running_batch_id = new_batch.name
            add_action_log(
                action=f"Batch: {self.running_batch_id} for Company: {self.running_batch_company} "
                       f"created")
        if self.run_auto_attendance_batch_options:
            self.run_auto_attendance_process()
        self.process_employees()
        add_action_log(
            action=f"Batch: {self.running_batch_id} completed and will update the status")
        PayrollLavaDoManager.update_last_running_batch_in_progress(
            batch_new_status="Completed", batch_id=self.running_batch_id)
        frappe.publish_realtime('msgprint', 'Ending create_resume_batch_process...')

    def delete_failed_processed_employees_records(self, batch_id):
        batch_failed_processed_employees = self.get_batch_failed_processed_employees(batch_id)
        for failed_processed_employee in batch_failed_processed_employees:
            self.delete_employee_batch_records(employee_id=failed_processed_employee.name, batch_id=batch_id)
            add_action_log(
                action=f"Batch: {batch_id} for Company: {self.running_batch_company} removed the records of"
                       " employee {failed_processed_employee}")

    def run_auto_attendance_process(self):
        add_action_log(
            action=f"Batch: {self.running_batch_id} starting auto attendance")
        for shift_type in self.shift_types:
            if shift_type.enable_auto_attendance:
                add_action_log(
                    action=f"Batch: {self.running_batch_id} run auto attendance of "
                           f"shift type '{shift_type.name}'")
                try:
                    self.run_standard_auto_attendance(shift_type)
                except Exception as ex:
                    frappe.log_error(
                        message=f"Auto attendance of Shift Type: '{shift_type.name}' "
                                f"Error message: '{format_exception(ex)}'", title=batch_process_title)

    def get_batch_failed_processed_employees(self, batch_id):
        return frappe.get_all('Lava Batch Object',
                              filters={"batch_id": batch_id,
                                       "object_type": 'Employee',
                                       "status": ["in", ["Failed", "In progress"]]}
                              )

    def delete_employee_batch_records(self, employee_id: str, batch_id: str):
        batch_related_doctypes = ["Timesheet", "Lava Penalty Record", "Additional Salary"]
        limit_page_start = 0
        limit_page_length = 100
        while True:
            employee_batch_records = frappe.get_all("Lava Batch Object",
                                                    {'object_type': ['in', batch_related_doctypes],
                                                     'batch_id': batch_id, 'parent': employee_id,
                                                     'parenttype': 'Employee'},
                                                    ['object_type', 'object_id'], limit_start=limit_page_start,
                                                    limit_page_length=limit_page_length)
            if not employee_batch_records:
                break

            limit_page_start += limit_page_length
            for record in employee_batch_records:
                frappe.delete_doc(doctype=record.object_type, name=record.object_id, force=1)

            frappe.db.sql(f"""
                           delete from `tabLava Batch Object` where object_type = 'Employee' 
                           and parent = %(employee_id)s and batch_id = %(batch_id)s
                           """, {'employee_id': employee_id,
                                 'batch_id': batch_id})
            frappe.db.sql(f"""
                           delete from `tabLava Batch Object` where parenttype= 'Employee' 
                           and object_id = %(employee_id)s and batch_id = %(batch_id)s
                           """, {'employee_id': employee_id,
                                 'batch_id': batch_id})

            frappe.db.commit()

    def validate_shift_types(self, check_shift_types):
        invalid_shift_types = []
        for shift_type in check_shift_types:
            if (not shift_type.enable_auto_attendance or not shift_type.process_attendance_after
                    or not shift_type.last_sync_of_checkin):
                invalid_shift_types.append(shift_type.name)

        if invalid_shift_types:
            invalid_shift_types_ids = ""
            for invalid_shift_type in invalid_shift_types:
                invalid_shift_types_ids += "," + invalid_shift_type
            exp_msg = f"Shift types ({invalid_shift_types_ids}) have missing data"
            frappe.log_error(message=f"Error message: '{exp_msg}'", title=batch_process_title)

    def process_employees(self, selected_employees: [] = None):
        employees = None
        if selected_employees:
            employees = selected_employees
        else:
            employees = self.get_company_employees_not_processed(self.running_batch_company,
                                                                 self.running_batch_id)
        for employee in employees:
            self.process_employee(employee_id=employee.employee_id)

    def process_employee(self, employee_id):
        employee_batch_object_doc = self.create_batch_object_record(batch_id=self.running_batch_id,
                                                                    object_type="Employee",
                                                                    object_id=employee_id, status="In progress",
                                                                    notes="")
        add_action_log(
            action=f"Start process employee: {employee_id} "
                   f"for company: {self.running_batch_company} "
                   f"into batch : {self.running_batch_id}")
        try:
            attendance_list = self.get_attendance_list(employee_id, self.running_batch_start_date,
                                                       self.running_batch_end_date)
            employee_changelog_records = self.get_employee_changelog_records(max_date=self.running_batch_end_date,
                                                                             employee_id=employee_id)
            employee_timesheet = self.create_employee_timesheet(employee_id=employee_id,
                                                                company=self.running_batch_company)

            for attendance in attendance_list:
                self.process_employee_attendance(employee_id=employee_id, attendance=attendance,
                                                 employee_changelog_records=employee_changelog_records,
                                                 employee_timesheet=employee_timesheet)
            if attendance_list:
                try:
                    self.save_employee_timesheet(employee_id=employee_id, employee_timesheet=employee_timesheet)
                except Exception as ex:
                    if "is overlapping with" in format_exception(ex):
                        frappe.log_error(message=f"saving timesheet. Error: '{format_exception(ex)}'",
                                         title=batch_process_title)
            employee_batch_object_doc.status = "Completed"
            employee_batch_object_doc.save(ignore_permissions=True)
            add_action_log(
                action=f"End process employee: {employee_id} "
                       f"for company: {self.running_batch_company} "
                       f"into batch : {self.running_batch_id}")
        except Exception as ex:
            frappe.log_error(message=f"processing employee {employee_id}. Error: '{format_exception(ex)}'",
                             title=batch_process_title)
            employee_batch_object_doc.status = "Failed"
            employee_batch_object_doc.save(ignore_permissions=True)
            add_action_log(
                action=f"Failed processing employee: {employee_id} "
                       f"for company: {self.running_batch_company} "
                       f"into batch : {self.running_batch_id}")

    def process_employee_attendance(self, employee_id, attendance, employee_changelog_records, employee_timesheet):
        employee_changelog_record = self.get_employee_changelog_record(
            attendance_date=attendance.attendance_date,
            employee_change_log_records=employee_changelog_records)
        if not employee_changelog_record:
            exception_msg = f"Skipping {attendance.name} for the employee {attendance.employee} " \
                            f"as he hasn't changelog for this date"
            frappe.throw(msg=exception_msg, title=batch_process_title)
        try:
            self.calc_attendance_working_hours_breakdowns(attendance)
        except Exception as ex:
            frappe.throw(msg=f"calc_attendance_working_hours_breakdowns: Employee: {employee_id}, "
                             f"attendance ID: {attendance.name}; Error message: '{format_exception(ex)}'",
                         title=batch_process_title)
        try:
            if attendance.status.lower() != "absent":
                self.add_timesheet_record(employee_timesheet=employee_timesheet,
                                          attendance=attendance,
                                          activity_type=self.payroll_activity_type)
        except Exception as ex:
            frappe.throw(msg=f"add_timesheet_record: Employee: {employee_id}, attendance ID: {attendance.name}, "
                             f" Error message: '{format_exception(ex)}'", title=batch_process_title)
        employee_applied_policies = self.get_employee_applied_policies(
            employee_designation=employee_changelog_record['designation'] if employee_changelog_record else "")
        if employee_applied_policies:
            add_action_log(
                action=f"Start adding penalties for employee: {employee_id} "
                       f"for company: {self.running_batch_company} into batch : {self.running_batch_id}")
            self.add_penalties(employee_changelog_record, attendance, employee_applied_policies)

    def save_employee_timesheet(self, employee_id, employee_timesheet):
        try:
            if not hasattr(employee_timesheet, 'time_logs'):
                return  # no need to save a timesheet without records

            employee_timesheet.save(ignore_permissions=True)

            # employee_timesheet.submit()
            self.create_batch_object_record(batch_id=self.running_batch_id, object_type="Timesheet",
                                            object_id=employee_timesheet.name,
                                            status="Created", notes="", parent_id=employee_id)
        except frappe.MandatoryError as mandatory_error_ex:
            if "time_logs" in str(mandatory_error_ex):  # no need to save timesheet without time_logs
                frappe.log_error(message=f"process employee: {employee_id}, "
                                         f"save timesheet; "
                                         f"mandatory Error message: '{format_exception(mandatory_error_ex)}'",
                                 title=batch_process_title)
        # except Exception as ex:
        #     # FIXME: fix the modified doc issue
        #     timesheet_old_doc = employee_timesheet
        #     employee_timesheet.reload()
        #     timesheet_new_doc = employee_timesheet
        #     changes = get_changes(doctype_name='Timesheet', doc_old_version=timesheet_old_doc,
        #                           doc_new_version=timesheet_new_doc)
        #     frappe.log_error(message=f"process employee: {employee_id}, "
        #                              f"save timesheet; Error message: '{format_exception(ex)}',"
        #                              f" changes: '{changes}'",
        #                      title=batch_process_title)
        #     employee_timesheet.submit()

    def add_penalties(self, employee_changelog_record, attendance, applied_policies):
        existing_penalty_records = frappe.get_all("Lava Penalty Record",
                                                  filters={"employee": employee_changelog_record.employee,
                                                           "penalty_date": ['=', attendance.attendance_date]},
                                                  fields=['*'],
                                                  order_by='penalty_date', limit_page_length=100)

        for existing_penalty_record in existing_penalty_records:
            policy = self.get_policy_by_id(existing_penalty_record.penalty_policy, policies=applied_policies)
            if policy:
                self.get_policy_and_add_penalty_record(
                    employee_changelog_record=employee_changelog_record,
                    attendance=attendance,
                    policy_group_obj=self.get_policy_group_by_id(policy.penalty_group),
                    policy_subgroup=policy.penalty_subgroup,
                    applied_policies=applied_policies,
                    policy_id=existing_penalty_record.penalty_policy,
                    existing_penalty_record=existing_penalty_record)
            else:
                frappe.throw(msg=f"Cannot find policy '{existing_penalty_record.penalty_policy}' "
                                 f"for penalty record '{existing_penalty_record.name}'",
                             title=batch_process_title)
        policy_subgroups = []
        if attendance.status.lower() == "absent":
            policy_subgroups.append("attendance absence")
        if attendance.late_entry:
            policy_subgroups.append("attendance check-in")
        if attendance.early_exit:
            policy_subgroups.append("attendance check-out")

        for policy_subgroup in policy_subgroups:
            policy_group_obj = self.get_policy_group_by_policy_subgroup(policy_sub_group_name=policy_subgroup,
                                                                        policies=applied_policies)
            self.get_policy_and_add_penalty_record(
                employee_changelog_record=employee_changelog_record,
                attendance=attendance,
                policy_group_obj=policy_group_obj,
                policy_subgroup=policy_subgroup,
                applied_policies=applied_policies)

    def get_policy_and_add_penalty_record(self, employee_changelog_record,
                                          attendance,
                                          policy_group_obj,
                                          policy_subgroup,
                                          applied_policies,
                                          policy_id=None,
                                          existing_penalty_record=None):
        if not policy_group_obj:
            frappe.log_error(message=f"error: passing no policy group to function get_policy_and_add_penalty_record. "
                                     f"attendance: {attendance.name}, policy_subgroup: {policy_subgroup}",
                             title=batch_process_title)
            return None

        gap_duration_in_minutes = 0
        policy_group_id = None
        occurred_policy = None

        if policy_id:
            occurred_policy = self.get_policy_by_id(policy_id=policy_id)
            policy_group_id = occurred_policy.penalty_group
            policy_subgroup = occurred_policy.policy_subgroup
        elif policy_subgroup.lower() == "attendance check-in":
            gap_duration_in_minutes = attendance.lava_entry_duration_difference
            policy_group_obj = self.get_policy_group_by_policy_subgroup("attendance check-in", applied_policies)
        elif policy_subgroup.lower() == "attendance check-out":
            gap_duration_in_minutes = attendance.lava_exit_duration_difference
            policy_group_obj = self.get_policy_group_by_policy_subgroup("attendance check-out", applied_policies)
        elif policy_subgroup.lower() == "attendance absence":
            policy_group_obj = self.get_policy_group_by_policy_subgroup("attendance absence", applied_policies)

        policy_group_id = policy_group_obj.name

        policy_occurrence_number = self.get_penalty_records_number_within_duration(
            employee=employee_changelog_record.employee,
            check_date=attendance.attendance_date,
            duration_in_days=policy_group_obj.reset_duration,
            policy_subgroup=policy_subgroup,
            policy_group=policy_group_id)
        if not occurred_policy:
            occurred_policy = self.get_policy_by_filters(
                policies=applied_policies,
                group_name=policy_group_obj.name,
                subgroup_name=policy_subgroup,
                occurrence_number=policy_occurrence_number,
                gap_duration_in_minutes=gap_duration_in_minutes)
        if occurred_policy:
            self.add_update_penalty_record(employee_changelog_record=employee_changelog_record,
                                           batch_id=self.running_batch_id,
                                           attendance=attendance,
                                           policy=occurred_policy,
                                           policy_occurrence_number=policy_occurrence_number,
                                           existing_penalty_record=existing_penalty_record)
        else:
            frappe.throw(msg=f"get_policy_and_add_penalty_record. Error: unrecognized policy for "
                             f"attendance '{attendance.name}'",
                         title=batch_process_title)

    def get_penalty_records_number_within_duration(self, employee, check_date, duration_in_days, policy_subgroup,
                                                   policy_group):
        from_date = check_date - datetime.timedelta(days=duration_in_days)

        # FIXME: remove the check of the action type
        penalty_records_number_within_duration = frappe.db.sql(
            f"""SELECT COUNT(pr.name) AS 'records_number'
            FROM `tabLava Penalty Record` pr INNER JOIN `tabLava Penalty Policy` p
                ON pr.penalty_policy = p.name
            INNER JOIN `tabLava Penalty Group` g
                ON p.penalty_group = g.name and g.name = %(policy_group)s
            WHERE
                (#
                    (pr.occurrence_number >= 0 And pr.action_type='Manual')
                    or
                    (pr.occurrence_number >= 0 And pr.action_type='Automatic')
                )
                AND LOWER(pr.policy_subgroup) = %(policy_subgroup)s
                AND LOWER(pr.employee) = %(employee)s
                AND pr.penalty_date >= %(from_date)s
                AND pr.penalty_date <= %(to_date)s
            ORDER BY pr.penalty_date, pr.creation
            """
            , {'policy_subgroup': policy_subgroup.lower(),
               'policy_group': policy_group.lower(),
               'employee': employee,
               'from_date': from_date,
               'to_date': check_date}, as_dict=1)
        result = penalty_records_number_within_duration[0].records_number
        if result == 0:
            result = 1
        return result

    def get_employee_applied_policies(self, employee_designation):
        if not employee_designation:
            frappe.throw(
                "error: passing empty string of employee_designation to function get_employee_applied_policies")
        applied_policies = []
        for policy in self.penalty_policies:
            for designation in policy['designations']:
                if (designation['designation_name']).lower() == employee_designation.lower():
                    applied_policies.append(policy)
                    break
        return applied_policies

    def get_attendance_list(self, employee: str, start_date: date, end_date: date):
        return frappe.get_all("Attendance", filters={'employee': employee,
                                                     'attendance_date': ['between', (start_date, end_date)]},
                              fields=['*'], order_by="attendance_date asc", limit_page_length=365)

    def create_employees_first_changelog_records(self, company: str):
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
                employee_change_log_record.hourly_rate = row.salary_structure_hour_rate or 0
                employee_change_log_record.insert(ignore_permissions=True)

        if len(employees_with_missing_data) > 0:
            employees_with_missing_data_ids = ", ".join(employees_with_missing_data)
            exp_msg = f"Employees ({employees_with_missing_data_ids}) don't have designated salary " \
                      "structure assignment and/or shift."
            frappe.log_error(message=f"Error message: '{exp_msg}'", title=batch_process_title)

    def get_employee_changelog_records(self, max_date: date, employee_id: str):
        employee_changelogs = frappe.get_all("Lava Employee Payroll Changelog",
                                             filters={'employee': employee_id, 'change_date': ['<=', max_date]},
                                             order_by="change_date desc", fields=['*'], limit_page_length=100)
        return employee_changelogs

    def get_employee_changelog_record(self, attendance_date: date, employee_change_log_records):
        for record in employee_change_log_records:
            if record['change_date'] <= attendance_date:
                return record
        return None

    def get_company_employees_not_processed(self, company: str, batch_id: str):
        # TODO: enhance the performance by considering paging
        query_str = """ 
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
                                AND status= "Completed"
                        )
                        """

        employee_list = frappe.db.sql(query_str,
                                      {'company': company, 'batch_id': batch_id},
                                      as_dict=1)

        return employee_list

    def create_batch_object_record(self, batch_id, object_type, object_id, status=None, notes=None, parent_id=None):
        batch_object_record = frappe.new_doc("Lava Batch Object")
        batch_object_record.batch_id = batch_id
        batch_object_record.object_type = object_type
        batch_object_record.object_id = object_id
        batch_object_record.status = status
        batch_object_record.notes = notes
        if parent_id:
            batch_object_record.parenttype = "Employee"
            batch_object_record.parentfield = "name"
            batch_object_record.parent = parent_id
        batch_object_record.insert(ignore_permissions=True)
        return batch_object_record

    def run_standard_auto_attendance(self, shift_type: fdict):
        shift_doc = frappe.get_doc('Shift Type', shift_type.name)
        shift_doc.process_auto_attendance()
        frappe.db.commit()

    def create_employee_timesheet(self, employee_id, company):
        timesheet = frappe.new_doc('Timesheet')
        timesheet.employee = employee_id
        timesheet.company = company
        return timesheet

    def calc_attendance_working_hours_breakdowns(self, attendance):
        attendance_doc = frappe.get_doc("Attendance", attendance.name)
        if attendance_doc.shift:
            shift_type = self.get_shift_type_by_id(attendance.shift)
            if attendance_doc.status != 'Absent' and attendance_doc.status != 'On Leave' and attendance_doc.docstatus == 1:
                if attendance_doc.late_entry:
                    attendance_doc.lava_entry_duration_difference = int(time_diff_in_seconds(
                        attendance_doc.in_time.strftime("%H:%M:%S"),
                        str(shift_type.start_time)) / 60)
                    if attendance_doc.lava_entry_duration_difference == 0:
                        frappe.throw(msg=f"attendance_working_hours_breakdowns: "
                                         f"zero late check-in of attendance: {attendance.name}"
                                     , title=batch_process_title)
                if attendance_doc.early_exit:
                    attendance_doc.lava_exit_duration_difference = int(time_diff_in_seconds(
                        str(shift_type.end_time),
                        attendance_doc.out_time.strftime("%H:%M:%S")) / 60)
                    if attendance_doc.lava_exit_duration_difference == 0:
                        frappe.throw(msg=f"calc_attendance_working_hours_breakdowns: "
                                         f"zero early check-out of attendance: {attendance.name}"
                                     , title=batch_process_title)
                if attendance_doc.lava_entry_duration_difference < 0 \
                        or attendance_doc.lava_exit_duration_difference < 0:
                    frappe.throw(title=batch_process_title,
                                 msg=f"Negative time diff in attendance {attendance_doc.name}")
            shift_time_diff = time_diff_in_seconds(str(shift_type.end_time),
                                                   str(shift_type.start_time)) / 60
            if shift_time_diff >= 0:
                attendance_doc.lava_planned_working_hours = time_diff_in_hours(shift_type.end_time,
                                                                               shift_type.start_time)
            else:
                attendance_doc.lava_planned_working_hours = time_diff_in_hours(
                    str(to_timedelta("23:59:59")),
                    str(shift_type.start_time))
                attendance_doc.lava_planned_working_hours += time_diff_in_hours(
                    str(shift_type.end_time),
                    str(to_timedelta("00:00:00")))
            attendance_doc.save(ignore_permissions=True)
        else:
            frappe.throw(msg=f"calc_attendance_working_hours_breakdowns: "
                             f"unrecognized shift type of attendance: {attendance.name}"
                         , title=batch_process_title)

    def get_hr_settings_day_working_hours(self):
        doc = frappe.get_single("HR Settings")
        return doc.standard_working_hours

    def add_timesheet_record(self, employee_timesheet, attendance, activity_type):
        if attendance.working_hours == 0:
            return
        frappe.db.delete("Timesheet Detail", filters={
            "parenttype": "Timesheet",
            "parentfield": "time_logs",
            "parent": employee_timesheet.name,
            "activity_type": activity_type,
            "from_time": attendance.in_time
        })
        working_hours = 0
        if attendance.status == "Half Day":
            working_hours = 0.5 * self.get_hr_settings_day_working_hours()
        else:
            working_hours = attendance.working_hours

        employee_timesheet.append("time_logs", {
            "activity_type": activity_type,
            "hours": working_hours,
            "expected_hours": attendance.lava_planned_working_hours,
            "from_time": attendance.in_time,
            "to_time": attendance.out_time,
            "description": "Created by Payroll LavaDo Batch Process"
        })

    def add_update_penalty_record(self, employee_changelog_record, batch_id,
                                  attendance, policy,
                                  policy_occurrence_number,
                                  existing_penalty_record):
        if not policy:
            frappe.throw(msg=f"add_update_penalty_record: invalid inputs "
                             f"employee_changelog_record: {employee_changelog_record.name}", title=batch_process_title)
        if existing_penalty_record:
            existing_penalty_record = frappe.get_doc("Lava Penalty Record", existing_penalty_record.name)
        penalty_record = existing_penalty_record
        if not penalty_record:
            penalty_record = frappe.new_doc("Lava Penalty Record")

        deduction_in_days_amount = employee_changelog_record.hourly_rate * policy['deduction_factor'] * \
                                   self.get_hr_settings_day_working_hours()
        deduction_absolute_amount = policy.get('deduction_amount', 0)
        deduction_times_time_gap = 0
        if policy['policy_subgroup'] == "attendance check-in":
            deduction_times_time_gap = (attendance.lava_entry_duration_difference / 60) * \
                                       employee_changelog_record.hourly_rate * policy['deduction_factor']
        elif policy['policy_subgroup'] == "attendance check-out":
            deduction_times_time_gap = (attendance.lava_exit_duration_difference / 60) * \
                                       employee_changelog_record.hourly_rate * policy['deduction_factor']

        deductions = [deduction_absolute_amount, deduction_in_days_amount, deduction_times_time_gap]
        applied_penalty_deduction_amount: float = 0
        if policy['deduction_rule'] == "biggest":
            applied_penalty_deduction_amount = max(deductions)
        elif policy['deduction_rule'] == "smallest":
            applied_penalty_deduction_amount = min(deductions)
        elif policy['deduction_rule'] == "absolute amount":
            applied_penalty_deduction_amount = deduction_absolute_amount
        elif policy['deduction_rule'] == "deduction in days":
            applied_penalty_deduction_amount = deduction_in_days_amount
        elif policy['deduction_rule'] == "times time gap":
            applied_penalty_deduction_amount = deduction_times_time_gap
        else:
            exp_msg = f"add_penalty_record: Unknown deduction rule. subgroup '{policy['policy_subgroup']}' " \
                      f"of this deduction rule '{policy.deduction_rule}'" \
                      f" for attendance date {attendance.attendance_date} " \
                      f"for employee {employee_changelog_record.employee}"
            frappe.throw(msg=exp_msg, title=batch_process_title)

        penalty_record.occurrence_number = policy_occurrence_number
        penalty_record.penalty_amount = applied_penalty_deduction_amount
        if not existing_penalty_record:
            penalty_record.employee = employee_changelog_record.employee
            penalty_record.penalty_policy = policy['policy_name']
            penalty_record.policy_subgroup = policy['policy_subgroup']
            penalty_record.penalty_date = attendance.attendance_date
            penalty_record.action_type = "Automatic"
            penalty_record.notes = ""
            penalty_record.lava_payroll_batch = batch_id

        if not existing_penalty_record and not frappe.db.exists(
                "Lava Penalty Record",
                {"action_type": "Automatic",
                 "penalty_date": attendance.attendance_date,
                 "policy_subgroup": policy['policy_subgroup']}):
            penalty_record.save(ignore_permissions=True)
            self.create_batch_object_record(batch_id=batch_id, object_type="Lava Penalty Record",
                                            object_id=penalty_record.name,
                                            status="Created", notes="", parent_id=employee_changelog_record.employee)
        elif existing_penalty_record:
            penalty_record.save(ignore_permissions=True)

        frappe.db.commit()
        # FIXME: code doesn't save the record in db immediately which result a wrong occurrence number
        #  in the next record

        # if not existing_penalty_record:
        #     penalty_record.submit()

        if applied_penalty_deduction_amount > 0:
            self.add_additional_salary(penalty_record, batch_id)

    def add_additional_salary(self, penalty_record, batch_id):
        if frappe.db.exists("Additional Salary", {"employee": penalty_record.employee,
                                                  "payroll_date": penalty_record.penalty_date,
                                                  "reason": ["like", f"%{penalty_record.penalty_policy}%"]}):
            return  # no need to create salary addition for the already created record of the same penalty

        additional_salary_record = frappe.new_doc("Additional Salary")
        additional_salary_record.employee = penalty_record.employee
        additional_salary_record.payroll_date = penalty_record.penalty_date
        additional_salary_record.overwrite_salary_structure_amount = 0
        additional_salary_record.amount = penalty_record.penalty_amount
        applied_policy = self.get_policy_by_id(policy_id=penalty_record.penalty_policy)
        additional_salary_record.salary_component = applied_policy.salary_component

        additional_salary_record.reason = f"Apply policy {penalty_record.penalty_policy}," \
                                          f" occurrence number {penalty_record.occurrence_number}."
        additional_salary_record.save(ignore_permissions=True)
        # additional_salary_record.submit()
        self.create_batch_object_record(batch_id=batch_id, object_type="Additional Salary",
                                        object_id=additional_salary_record.name,
                                        status="Created", notes="", parent_id=penalty_record.employee)

    @staticmethod
    def parse_batch_options(doc: str):
        doc_dict = json.loads(doc)
        return {
            "chk-clear-error-log-records": doc_dict['chk-clear-error-log-records'],
            "chk-clear-action-log-records": doc_dict['chk-clear-action-log-records'],
            "chk-batch-objects": doc_dict['chk-batch-objects'],
            "chk-biometric-process": doc_dict['chk-biometric-process'],
            "chk-batch-debug-mode": doc_dict['chk-batch-debug-mode'],
            "chk-auto-attendance": doc_dict['chk-auto-attendance'],
            "batch_id": doc_dict['batch_id'],
            "action_type": doc_dict['action_type']
        }


@frappe.whitelist()
def run_payroll_lavado_batch(doc: str):
    doc_dict = json.loads(doc)
    batch_options = PayrollLavaDoManager.parse_batch_options(doc)

    company = doc_dict['company']
    start_date = getdate(doc_dict['start_date'])
    end_date = getdate(doc_dict['end_date'])

    result_status = ""
    try:
        payroll_lavado_manager = PayrollLavaDoManager()
        payroll_lavado_manager.add_batch_to_background_jobs(company=company,
                                                            start_date=start_date,
                                                            end_date=end_date,
                                                            batch_options=batch_options)
        result_status = "Success"
    except Exception as ex:
        result_status = f"Failed '{ex}'"
        frappe.log_error(message=f"error occurred, '{format_exception(ex)}'", title=batch_process_title)
    finally:
        return result_status


def run_payroll_batch_process(company: str, start_date: date, end_date: date, batch_options):
    payroll_lavado_manager = PayrollLavaDoManager()
    payroll_lavado_manager.create_resume_batch_process(company=company,
                                                       start_date=start_date,
                                                       end_date=end_date,
                                                       batch_options=batch_options)


def format_exception(ex: Exception) -> str:
    import traceback
    error = str(ex)
    trace = ''.join(traceback.format_exception(type(ex), ex, ex.__traceback__))
    return f'{error}\n{trace}'


def add_action_log(action: str, action_type: str = "LOG", notes: str = None):
    new_doc = frappe.new_doc("Lava Action Log")
    new_doc.action = action
    new_doc.action_type = action_type
    new_doc.notes = notes
    new_doc.insert(ignore_permissions=True)
    # new_doc.commit


def get_changes(doctype_name: str, doc_old_version, doc_new_version):
    meta_data = frappe.get_meta(doctype_name)
    gap = ''
    for field in meta_data.fields:
        if doc_old_version[field] != doc_new_version[field]:
            gap = gap + f"field: '{field}', old value: '{doc_old_version[field]}'," \
                        f" new value: '{doc_new_version[field]}'" + "\n"
    return gap
