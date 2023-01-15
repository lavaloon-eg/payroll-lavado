import datetime
import json
from datetime import date
from pymysql import ProgrammingError
import frappe
# noinspection PyProtectedMember
from frappe import _
from frappe import _dict as fdict
from frappe.utils import time_diff_in_hours, getdate, time_diff_in_seconds, to_timedelta

# from frappe.utils.logger import get_logger

penalty_policy_groups: list = []
penalty_policies: list = []
shift_types: list = []
debug_mode: bool = False
clear_action_log_records: bool = False
clear_error_log_records: bool = False
clear_batch_objects: bool = False
run_biometric_attendance_process: bool = False
payroll_activity_type: str = "payroll_activity_type"
batch_process_title: str = "LavaDo Payroll Process"
batch_biometric_process_title: str = "run_biometric_attendance_records_process"
running_batch_id: str
running_batch_company: str
running_batch_start_date: date
running_batch_end_date: date


# TODO: Remove 'lava net working hours from lava_custom and all the code using.

def add_batch_to_background_jobs(company: str, start_date: date, end_date: date,
                                 new_batch_id: str, batch_options):
    global debug_mode
    debug_mode = True if (batch_options["chk-batch-debug-mode"] == 1) else False

    try:
        if debug_mode:
            create_resume_batch_process(company=company, start_date=start_date,
                                        end_date=end_date, new_batch_id=new_batch_id,
                                        batch_options=batch_options)
        else:
            frappe.enqueue(method='payroll_lavado.payroll_batch.create_resume_batch_process',
                           queue="long", timeout=36000000,
                           company=company, start_date=start_date,
                           end_date=end_date, new_batch_id=new_batch_id, batch_options=batch_options)
    except Exception as ex:
        update_last_running_batch_in_progress(batch_new_status="Failed")
        frappe.log_error(
            message="add the background job or run direct process; Error message: '{}'".format(
                format_exception(ex)),
            title=batch_process_title)


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
                frappe.log_error(message="record id: '{}', employee_biometric_id: '{}'. Error: '{}'".format(
                    checkin_record.name, checkin_record.employee_biometric_id, format_exception(ex)),
                    title=batch_biometric_process_title)


def check_payroll_activity_type():
    if not frappe.db.exists("Activity Type", payroll_activity_type):
        activity_type = frappe.new_doc("Activity Type")
        activity_type.activity_type = payroll_activity_type
        activity_type.code = "pat"
        activity_type.insert(ignore_permissions=True)


def update_last_running_batch_in_progress(batch_new_status: str, batch_id=None):
    try:
        if batch_id:
            batch_doc = frappe.get_doc("Lava Payroll LavaDo Batch", batch_id)
        else:
            batches = frappe.get_all("Lava Payroll LavaDo Batch", filters={"status": "In Progress"},
                                     order_by='modified desc', limit_page_length=1)
            if not batches:
                raise Exception("Expected at least one in-progress payroll batch but found none")

            batch_doc = frappe.get_doc("Lava Payroll LavaDo Batch", batches[0].name)

        batch_doc.status = batch_new_status
        batch_doc.batch_process_end_time = datetime.datetime.now()
        batch_doc.save()
    except Exception as ex:
        frappe.log_error(message="update_last_running_batch_in_progress, error: {}".format(format_exception(ex)),
                         title=batch_process_title)


def get_penalty_policy_groups():
    global penalty_policy_groups
    if penalty_policy_groups:
        penalty_policy_groups.clear()
    penalty_policy_group_records = frappe.get_all("Lava Penalty Group", filters={"docstatus": 1},
                                                  order_by='title', fields=["name"],
                                                  limit_start=0,
                                                  limit_page_length=200)

    for record in penalty_policy_group_records:
        doc = frappe.get_doc("Lava Penalty Group", record.name).as_dict()
        penalty_policy_groups.append(doc)
    return


def get_shift_types():
    global shift_types
    if shift_types:
        shift_types.clear()
    shift_types = frappe.get_all("Shift Type", fields=["*"],
                                 limit_start=0, limit_page_length=100)
    return


def get_policy_group_by_id(policy_group_id, groups=None):
    if not groups:
        groups = penalty_policy_groups

    for group in groups:
        if group.name.lower() == policy_group_id.lower():
            return group
    return None


def get_policy_group_by_policy_subgroup(policy_sub_group_name: str, policies: []):
    for policy in policies:
        if policy.policy_subgroup.lower() == policy_sub_group_name.lower():
            group = get_policy_group_by_id(policy.penalty_group)
            if group:
                return group
    return None


def get_shift_type_by_id(shift_type_id, check_shift_types=None):
    global shift_types
    if not check_shift_types:
        check_shift_types = shift_types

    for shift_type in check_shift_types:
        if shift_type.name.lower() == shift_type_id.lower():
            return get_shift_type_doc(shift_type_id)
    return None


def get_shift_type_doc(shift_type_id):
    try:
        return frappe.get_doc('Shift Type', shift_type_id)
    except Exception:
        return None


def get_policy_by_id(policy_id, policies=None):
    if not policy_id:
        frappe.log_error(message=f"get_policy_by_id: no policy id", title=batch_process_title)
        return

    global penalty_policies
    if not policies:
        policies = penalty_policies

    for policy in policies:
        if (policy['policy_name']).lower() == policy_id.lower():
            return policy
    return None


def get_policy_by_filters(policies, group_name, subgroup_name, occurrence_number,
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


def get_penalty_policies(company):
    global penalty_policies
    if penalty_policies:
        penalty_policies.clear()
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
        get_policy_designations(company=company, policy_obj=policy)
        penalty_policies.append(policy)


def get_policy_designations(company, policy_obj):
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


def create_resume_batch_process(company: str, start_date: date, end_date: date,
                                new_batch_id: str, batch_options):
    # TODO: improve this function by considering the action_type and batch_id of the batch_options
    global running_batch_company
    running_batch_company = company
    global running_batch_start_date
    running_batch_start_date = start_date
    global running_batch_end_date
    running_batch_end_date = end_date

    global debug_mode
    global clear_action_log_records
    global clear_error_log_records
    global clear_batch_objects
    global run_biometric_attendance_process

    debug_mode = True if (batch_options['chk-batch-debug-mode'] == 1) else False
    clear_action_log_records = True if (batch_options['chk-clear-action-log-records'] == 1) else False
    clear_error_log_records = True if (batch_options['chk-clear-error-log-records'] == 1) else False
    clear_batch_objects = True if (batch_options['chk-batch-objects'] == 1) else False
    run_biometric_attendance_process = True if (batch_options['chk-biometric-process'] == 1) else False

    if clear_action_log_records:
        frappe.db.truncate("Lava Action Log")
        frappe.db.commit()
        add_action_log("cleared action log records", "Log")

    frappe.publish_realtime('msgprint', 'Starting create_resume_batch_process...')
    # this is the main entry point of the entire batch, and it can be called from a UI screen on desk,
    # or from a background scheduled job

    if clear_error_log_records:
        add_action_log("clear old error records", "Log")
        frappe.db.sql(f"""
                        delete from `tabError Log` where method in (%(first_title)s,
                         %(second_title)s)
                        """, {'first_title': batch_biometric_process_title,
                              'second_title': batch_process_title})
        frappe.db.commit()
        add_action_log("cleared old error records", "Log")

    if clear_batch_objects:
        add_action_log("clear batch object records", "Log")
        frappe.db.truncate("Lava Batch Object")
        frappe.db.commit()
        add_action_log("cleared batch object records", "Log")

    if run_biometric_attendance_process:
        add_action_log("Start processing the new and failed biometric attendance records ", "Log")
        run_biometric_attendance_records_process(start_date=running_batch_start_date, end_date=running_batch_end_date)
        add_action_log("End processing the new and failed biometric attendance records ", "Log")

    add_action_log("Start batch", "Log")

    last_processed_employee_id = ""
    get_penalty_policy_groups()
    get_penalty_policies(company)
    get_shift_types()
    check_payroll_activity_type()

    add_action_log(action="Start validating shift types.")
    try:
        validate_shift_types(shift_types)
    except Exception as ex:
        add_action_log(
            action="Due to the batches validation issue, exit the Batch Process for Company: {}, start date: {}, "
                   "end date: {}".format(running_batch_company, running_batch_start_date, running_batch_end_date))
        frappe.log_error(message="validate_shift_types; Error message: '{}'".format(format_exception(ex)),
                         title=batch_process_title)

    create_employees_first_changelog_records(running_batch_company)
    add_action_log(
        action="Created the first employees changelog records if missing, Batch Process for Company: {}, "
               "start date: {}, "
               "end date: {}".format(running_batch_company, running_batch_start_date, running_batch_end_date))

    add_action_log(
        action="Decide to resume or create a new Batch Process for Company: {}, start date: {},"
               "end date: {}".format(running_batch_company, running_batch_start_date, running_batch_end_date))
    progress_batches = frappe.get_all("Lava Payroll LavaDo Batch", {"status": "In Progress",
                                                                    "company": running_batch_company},
                                      ['name', 'start_date', 'end_date'], limit_page_length=40)
    global running_batch_id
    running_batch_id = ""

    if len(progress_batches) > 1:
        exp_msg = _("Company {} has more than a batch in progress, Please check".format(running_batch_company))
        frappe.log_error(message="Error message: '{}'".format(exp_msg), title=batch_process_title)

        frappe.throw(exp_msg)

    elif len(progress_batches) == 1 and new_batch_id == progress_batches[0].name:
        running_batch_id = progress_batches[0].name
        if progress_batches[0].start_date != start_date or progress_batches[0].end_date != end_date:
            exp_msg = _(
                "Company {} has  batch {} in progress but with different date range than requested,"
                " Please check".format(
                    running_batch_company, progress_batches[0].name))
            frappe.log_error(message="Error message: '{}'".format(exp_msg), title=batch_process_title)

            frappe.throw(exp_msg)
        else:
            old_batch = frappe.get_doc("Lava Payroll LavaDo Batch", running_batch_id)
            old_batch.batch_process_start_time = datetime.datetime.now()
            old_batch.batch_process_end_time = None
            old_batch.status = "In Progress"
            old_batch.save()
            last_processed_employee_id = get_batch_last_processed_employee_id(running_batch_id)
            add_action_log(action="Resume batch {} for company: {}".format(running_batch_id, running_batch_company))
            if last_processed_employee_id:
                delete_employee_batch_records(last_processed_employee_id, running_batch_id)
                add_action_log(
                    action="Batch: {} for Company: {} removed the records of"
                           " employee {}".format(running_batch_id, running_batch_company,
                                                 last_processed_employee_id))

    if running_batch_id == "":
        new_batch = frappe.new_doc("Lava Payroll LavaDo Batch")
        new_batch.company = company
        new_batch.start_date = running_batch_start_date
        new_batch.batch_process_start_time = datetime.datetime.now()
        new_batch.batch_process_end_time = None
        new_batch.status = "In Progress"
        new_batch.end_date = running_batch_end_date
        new_batch.save(ignore_permissions=True)
        running_batch_id = new_batch.name
        add_action_log(
            action="Batch: {} for Company: {} created".format(running_batch_id, company))

    run_auto_attendance_process()
    process_employees(last_processed_employee_id=last_processed_employee_id)
    add_action_log(
        action="Batch: {} completed and will update the status".format(running_batch_id))
    update_last_running_batch_in_progress(batch_new_status="Completed", batch_id=running_batch_id)
    frappe.publish_realtime('msgprint', 'Ending create_resume_batch_process...')


def run_auto_attendance_process():
    add_action_log(
        action="Batch: {} starting auto attendance".format(running_batch_id))
    for shift_type in shift_types:
        if shift_type.enable_auto_attendance:
            add_action_log(
                action="Batch: {} run auto attendance of shift type '{}'".format(running_batch_id, shift_type.name))
            try:
                run_standard_auto_attendance(shift_type)
            except Exception as ex:
                frappe.log_error(
                    message="Auto attendance of Shift Type: '{}' Error message: '{}'".format(shift_type.name,
                                                                                             format_exception(ex)),
                    title=batch_process_title)


def add_action_log(action: str, action_type: str = "LOG", notes: str = None):
    new_doc = frappe.new_doc("Lava Action Log")
    new_doc.action = action
    new_doc.action_type = action_type
    new_doc.notes = notes
    new_doc.insert(ignore_permissions=True)
    # new_doc.commit


def get_batch_last_processed_employee_id(batch_id):
    try:
        # TODO: replace by getting all failed employees
        employee_id = frappe.get_last_doc('Lava Batch Object',
                                          filters={"batch_id": batch_id,
                                                   "object_type": 'Employee',
                                                   "status": ["in", ["Failed", "In progress"]]}
                                          ).object_id
    except frappe.DoesNotExistError:
        employee_id = None

    return employee_id


def delete_employee_batch_records(employee_id: str, batch_id: str):
    batch_related_doctypes = ["Timesheet", "Lava Penalty Record", "Additional Salary"]
    limit_page_start = 0
    limit_page_length = 100
    while True:
        employee_batch_records = frappe.get_all("Lava Batch Object",
                                                {'object_type': ['in', batch_related_doctypes],
                                                 'batch_id': batch_id, 'parent': employee_id, 'parenttype': 'Employee'},
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


def validate_shift_types(check_shift_types):
    invalid_shift_types = []
    for shift_type in check_shift_types:
        if (not shift_type.enable_auto_attendance or not shift_type.process_attendance_after
                or not shift_type.last_sync_of_checkin):
            invalid_shift_types.append(shift_type.name)

    if invalid_shift_types:
        invalid_shift_types_ids = ""
        for invalid_shift_type in invalid_shift_types:
            invalid_shift_types_ids += "," + invalid_shift_type
        exp_msg = "Shift types ({}) have missing data".format(invalid_shift_types_ids)
        frappe.log_error(message="Error message: '{}'".format(exp_msg), title=batch_process_title)


def process_employees(last_processed_employee_id: str = None, selected_employees: [] = None):
    employees = None
    if selected_employees:
        employees = selected_employees
    else:
        employees = get_company_employees(running_batch_company, running_batch_id, last_processed_employee_id)
    for employee in employees:
        process_employee(employee_id=employee.employee_id)


def process_employee(employee_id):
    employee_batch_object_doc = create_batch_object_record(batch_id=running_batch_id, object_type="Employee",
                                                           object_id=employee_id, status="In progress", notes="")
    add_action_log(
        action=f"Start process employee: {employee_id} "
               f"for company: {running_batch_company} "
               f"into batch : {running_batch_id}")
    try:
        attendance_list = get_attendance_list(employee_id, running_batch_start_date, running_batch_end_date)
        employee_changelog_records = get_employee_changelog_records(max_date=running_batch_end_date,
                                                                    employee_id=employee_id)
        employee_timesheet = create_employee_timesheet(employee_id=employee_id,
                                                       company=running_batch_company)

        for attendance in attendance_list:
            process_employee_attendance(employee_id=employee_id, attendance=attendance,
                                        employee_changelog_records=employee_changelog_records,
                                        employee_timesheet=employee_timesheet)
        if attendance_list:
            try:
                save_employee_timesheet(employee_id=employee_id, employee_timesheet=employee_timesheet)
            except Exception as ex:
                if "is overlapping with" in format_exception(ex):
                    frappe.log_error(message=f"saving timesheet. Error: '{format_exception(ex)}'",
                                     title=batch_process_title)
        employee_batch_object_doc.status = "Completed"
        employee_batch_object_doc.save(ignore_permissions=True)
        add_action_log(
            action=f"End process employee: {employee_id} "
                   f"for company: {running_batch_company} "
                   f"into batch : {running_batch_id}")
    except Exception as ex:
        frappe.log_error(message=f"processing employee {employee_id}. Error: '{format_exception(ex)}'",
                         title=batch_process_title)
        add_action_log(
            action=f"Failed processing employee: {employee_id} "
                   f"for company: {running_batch_company} "
                   f"into batch : {running_batch_id}")


def process_employee_attendance(employee_id, attendance, employee_changelog_records, employee_timesheet):
    employee_changelog_record = get_employee_changelog_record(
        attendance_date=attendance.attendance_date,
        employee_change_log_records=employee_changelog_records)
    if not employee_changelog_record:
        exception_msg = f"Skipping {attendance.name} for the employee {attendance.employee} " \
                        f"as he hasn't changelog for this date"
        frappe.log_error(message=exception_msg, title=batch_process_title)
        return
    try:
        calc_attendance_working_hours_breakdowns(attendance)
    except Exception as ex:
        frappe.log_error(message="calc_attendance_working_hours_breakdowns:"
                                 " Employee: {}, attendance ID: {}; Error message: '{}'".format(employee_id,
                                                                                                attendance.name,
                                                                                                format_exception(ex)),
                         title=batch_process_title)
    try:
        if attendance.status.lower() != "absent":
            add_timesheet_record(employee_timesheet=employee_timesheet,
                                 attendance=attendance,
                                 activity_type=payroll_activity_type)
    except Exception as ex:
        frappe.log_error(message="add_timesheet_record:"
                                 " Employee: {}, attendance ID: {};"
                                 " Error message: '{}'".format(employee_id,
                                                               attendance.name, format_exception(ex)),
                         title=batch_process_title)
    employee_applied_policies = get_employee_applied_policies(
        employee_designation=employee_changelog_record['designation'] if employee_changelog_record else "")
    if employee_applied_policies:
        add_action_log(
            action="Start adding penalties for employee: {} for company: {} into batch : {}".format(
                employee_id,
                running_batch_company,
                running_batch_id))
        add_penalties(employee_changelog_record, attendance, employee_applied_policies)


def save_employee_timesheet(employee_id, employee_timesheet):
    try:
        if not hasattr(employee_timesheet, 'time_logs'):
            return  # no need to save a timesheet without records

        employee_timesheet.save(ignore_permissions=True)

        # employee_timesheet.submit()
        create_batch_object_record(batch_id=running_batch_id, object_type="Timesheet",
                                   object_id=employee_timesheet.name,
                                   status="Created", notes="", parent_id=employee_id)
    except frappe.MandatoryError as mandatory_error_ex:
        if "time_logs" in str(mandatory_error_ex):  # no need to save timesheet without time_logs
            frappe.log_error(message="process employee: {}, save timesheet; mandatory Error message: '{}'".format(
                employee_id, str(mandatory_error_ex)), title=batch_process_title)
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


def get_changes(doctype_name: str, doc_old_version, doc_new_version):
    meta_data = frappe.get_meta(doctype_name)
    gap = ''
    for field in meta_data.fields:
        if doc_old_version[field] != doc_new_version[field]:
            gap = gap + f"field: '{field}', old value: '{doc_old_version[field]}'," \
                        f" new value: '{doc_new_version[field]}'" + "\n"
    return gap


def add_penalties(employee_changelog_record, attendance, applied_policies):
    existing_penalty_records = frappe.get_all("Lava Penalty Record",
                                              filters={"employee": employee_changelog_record.employee,
                                                       "penalty_date": ['=', attendance.attendance_date],
                                                       "docstatus": 1,
                                                       "action_type": "Manual"},
                                              fields=['*'],
                                              order_by='penalty_date', limit_page_length=100)

    for existing_penalty_record in existing_penalty_records:
        policy = get_policy_by_id(existing_penalty_record.penalty_policy, policies=applied_policies)
        if policy:
            get_policy_and_add_penalty_record(
                employee_changelog_record=employee_changelog_record,
                attendance=attendance,
                policy_group_obj=get_policy_group_by_id(policy.penalty_group),
                policy_subgroup=policy.penalty_subgroup,
                applied_policies=applied_policies,
                policy_id=existing_penalty_record.penalty_policy,
                existing_penalty_record=existing_penalty_record)
        else:
            frappe.log_error(message=f"Cannot find policy '{existing_penalty_record.penalty_policy}' "
                                     f"for penalty record '{existing_penalty_record.name}'",
                             title=batch_process_title)
    policy_subgroup = None
    if attendance.status.lower() == "absent":
        policy_subgroup = "attendance absence"
    if attendance.late_entry:
        policy_subgroup = "attendance check-in"
    if attendance.early_exit:
        policy_subgroup = "attendance check-out"

    if policy_subgroup:
        policy_group_obj = get_policy_group_by_policy_subgroup(policy_sub_group_name=policy_subgroup,
                                                               policies=applied_policies)
        get_policy_and_add_penalty_record(
            employee_changelog_record=employee_changelog_record,
            attendance=attendance,
            policy_group_obj=policy_group_obj,
            policy_subgroup=policy_subgroup,
            applied_policies=applied_policies)


def get_policy_and_add_penalty_record(employee_changelog_record,
                                      attendance,
                                      policy_group_obj: dict,
                                      policy_subgroup,
                                      applied_policies,
                                      policy_id=None,
                                      existing_penalty_record=None):
    if not policy_group_obj:
        frappe.log_error(message=f"error: passing no policy group to function get_policy_and_add_penalty_record",
                         title=batch_process_title)
        return None

    gap_duration_in_minutes = 0
    policy_group_id = None
    occurred_policy = None

    if policy_id:
        occurred_policy = get_policy_by_id(policy_id=policy_id)
        policy_group_id = occurred_policy.penalty_group
        policy_subgroup = occurred_policy.policy_subgroup
    elif policy_subgroup.lower() == "attendance check-in":
        gap_duration_in_minutes = attendance.lava_entry_duration_difference
        policy_group_id = "Attendance"
    elif policy_subgroup.lower() == "attendance check-out":
        gap_duration_in_minutes = attendance.lava_exit_duration_difference
        policy_group_id = "Attendance"
    elif policy_subgroup.lower() == "attendance absence":
        policy_group_id = "Attendance"

    policy_occurrence_number = get_penalty_records_number_within_duration(
        employee=employee_changelog_record.employee,
        check_date=attendance.attendance_date,
        duration_in_days=policy_group_obj.reset_duration,
        policy_subgroup=policy_subgroup,
        policy_group=policy_group_id) + 1
    if not occurred_policy:
        occurred_policy = get_policy_by_filters(
            policies=applied_policies,
            group_name=policy_group_obj.name,
            subgroup_name=policy_subgroup,
            occurrence_number=policy_occurrence_number,
            gap_duration_in_minutes=gap_duration_in_minutes)
    if occurred_policy:
        add_update_penalty_record(employee_changelog_record=employee_changelog_record,
                                  batch_id=running_batch_id,
                                  attendance=attendance,
                                  policy=occurred_policy,
                                  policy_occurrence_number=policy_occurrence_number,
                                  existing_penalty_record=existing_penalty_record)
    else:
        frappe.log_error(message=f"get_policy_and_add_penalty_record. Error: unrecognized policy for "
                                 f"attendance '{attendance.name}'",
                         title=batch_process_title)


def get_penalty_records_number_within_duration(employee, check_date, duration_in_days, policy_subgroup, policy_group):
    from_date = check_date - datetime.timedelta(days=duration_in_days)

    penalty_records_number_within_duration = frappe.db.sql(
        f"""SELECT pr.name
        FROM `tabLava Penalty Record` pr INNER JOIN `tabLava Penalty Policy` p
            ON pr.penalty_policy = p.name
        INNER JOIN `tabLava Penalty Group` g
            ON p.penalty_group = g.name and g.name = %(policy_group)s
        WHERE pr.docstatus=1
            AND LOWER(pr.policy_subgroup) = %(policy_subgroup)s
            AND LOWER(pr.employee) = %(employee)s
            AND pr.penalty_date >= %(from_date)s
        ORDER BY pr.penalty_date
        """
        , {'policy_subgroup': policy_subgroup.lower(),
           'policy_group': policy_group.lower(),
           'employee': employee,
           'from_date': from_date}, as_dict=1)
    return len(penalty_records_number_within_duration)


def get_employee_applied_policies(employee_designation):
    if not employee_designation:
        frappe.throw("error: passing empty string of employee_designation to function get_employee_applied_policies")
    applied_policies = []
    for policy in penalty_policies:
        for designation in policy['designations']:
            if (designation['designation_name']).lower() == employee_designation.lower():
                applied_policies.append(policy)
                break
    return applied_policies


def get_attendance_list(employee: str, start_date: date, end_date: date):
    return frappe.get_all("Attendance", filters={'employee': employee,
                                                 'attendance_date': ['between', (start_date, end_date)]},
                          fields=['*'], order_by="attendance_date asc", limit_page_length=365)


def create_employees_first_changelog_records(company: str):
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


def get_employee_changelog_records(max_date: date, employee_id: str):
    employee_changelogs = frappe.get_all("Lava Employee Payroll Changelog",
                                         filters={'employee': employee_id, 'change_date': ['<=', max_date]},
                                         order_by="change_date desc", fields=['*'], limit_page_length=100)
    return employee_changelogs


def get_employee_changelog_record(attendance_date: date, employee_change_log_records):
    for record in employee_change_log_records:
        if record['change_date'] <= attendance_date:
            return record
    return None


def get_company_employees(company: str, batch_id: str, last_processed_employee_id: str = None):
    # TODO: enhance the performance by considering paging
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


def create_batch_object_record(batch_id, object_type, object_id, status=None, notes=None, parent_id=None):
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


def run_standard_auto_attendance(shift_type: fdict):
    shift_doc = frappe.get_doc('Shift Type', shift_type.name)
    shift_doc.process_auto_attendance()
    frappe.db.commit()


def create_employee_timesheet(employee_id, company):
    timesheet = frappe.new_doc('Timesheet')
    timesheet.employee = employee_id
    timesheet.company = company
    return timesheet


def calc_attendance_working_hours_breakdowns(attendance):
    attendance_doc = frappe.get_doc("Attendance", attendance.name)
    if attendance_doc.shift:
        shift_type = get_shift_type_by_id(attendance.shift)
        if attendance_doc.status != 'Absent' and attendance_doc.status != 'On Leave' and attendance_doc.docstatus == 1:
            if attendance_doc.late_entry:
                attendance_doc.lava_entry_duration_difference = int(time_diff_in_seconds(
                    attendance_doc.in_time.strftime("%H:%M:%S"),
                    str(shift_type.start_time)) / 60)
            if attendance_doc.early_exit:
                attendance_doc.lava_exit_duration_difference = int(time_diff_in_seconds(
                    str(shift_type.end_time),
                    attendance_doc.out_time.strftime("%H:%M:%S")) / 60)
            if attendance_doc.lava_entry_duration_difference < 0 or attendance_doc.lava_exit_duration_difference < 0:
                frappe.log_error(title=batch_process_title,
                                 message="Negative time diff in attendance {}".format(attendance_doc.name))
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


def get_hr_settings_day_working_hours():
    doc = frappe.get_single("HR Settings")
    return doc.standard_working_hours


def add_timesheet_record(employee_timesheet, attendance, activity_type=None):
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
        working_hours = 0.5 * get_hr_settings_day_working_hours()
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


def add_update_penalty_record(employee_changelog_record, batch_id,
                              attendance, policy,
                              policy_occurrence_number,
                              existing_penalty_record):
    if existing_penalty_record:
        existing_penalty_record = frappe.get_doc("Lava Penalty Record", existing_penalty_record.name)
    penalty_record = existing_penalty_record
    if not penalty_record:
        penalty_record = frappe.new_doc("Lava Penalty Record")

    deduction_in_days_amount = employee_changelog_record.hourly_rate * policy['deduction_factor'] * \
                               get_hr_settings_day_working_hours()
    deduction_absolute_amount = policy.get('deduction_amount', 0)
    deduction_times_time_gap = 0
    if policy['policy_subgroup'] == "attendance check-in":
        deduction_times_time_gap = (attendance.lava_entry_duration_difference / 60) * \
                                   employee_changelog_record.hourly_rate * policy['deduction_factor']
    elif policy['policy_subgroup'] == "attendance check-out":
        deduction_times_time_gap = (attendance.lava_exit_duration_difference / 60) * \
                                   employee_changelog_record.hourly_rate * policy['deduction_factor']

    deductions = [deduction_absolute_amount, deduction_in_days_amount, deduction_times_time_gap]

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
        exp_msg = f"Unknown deduction rule. subgroup '{policy['policy_subgroup']}' " \
                  f"of this deduction rule '{policy.deduction_rule}'" \
                  f" for attendance date {attendance.attendance_date} " \
                  f"for employee {employee_changelog_record.employee}"
        frappe.log_error(message="add_penalty_record; Error message: '{}'".format(exp_msg),
                         title=batch_process_title)
        return

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

    penalty_record.save(ignore_permissions=True)

    # if not existing_penalty_record:
    #     penalty_record.submit()

    if not existing_penalty_record:
        create_batch_object_record(batch_id=batch_id, object_type="Lava Penalty Record",
                                   object_id=penalty_record.name,
                                   status="Created", notes="", parent_id=employee_changelog_record.employee)
    if applied_penalty_deduction_amount > 0:
        add_additional_salary(penalty_record, batch_id)


def add_additional_salary(penalty_record, batch_id):
    additional_salary_record = frappe.new_doc("Additional Salary")
    additional_salary_record.employee = penalty_record.employee
    additional_salary_record.payroll_date = penalty_record.penalty_date
    additional_salary_record.overwrite_salary_structure_amount = 0
    additional_salary_record.amount = penalty_record.penalty_amount
    applied_policy = get_policy_by_id(policy_id=penalty_record.penalty_policy)
    additional_salary_record.salary_component = applied_policy.salary_component

    additional_salary_record.reason = f"Apply policy {penalty_record.penalty_policy}," \
                                      f" occurrence number {penalty_record.occurrence_number}."
    additional_salary_record.save(ignore_permissions=True)
    # additional_salary_record.submit()
    create_batch_object_record(batch_id=batch_id, object_type="Additional Salary",
                               object_id=additional_salary_record.name,
                               status="Created", notes="", parent_id=penalty_record.employee)


def parse_batch_options(doc: str):
    doc_dict = json.loads(doc)
    return {
        "chk-clear-error-log-records": doc_dict['chk-clear-error-log-records'],
        "chk-clear-action-log-records": doc_dict['chk-clear-action-log-records'],
        "chk-batch-objects": doc_dict['chk-batch-objects'],
        "chk-biometric-process": doc_dict['chk-biometric-process'],
        "chk-batch-debug-mode": doc_dict['chk-batch-debug-mode'],
        "batch_id": doc_dict['batch_id'],
        "action_type": doc_dict['action_type']
    }


@frappe.whitelist()
def run_payroll_lavado_batch(doc: str):
    doc_dict = json.loads(doc)
    batch_options = parse_batch_options(doc)
    batch_id = doc_dict['batch_id'] or "dummy id"

    company = doc_dict['company']
    start_date = getdate(doc_dict['start_date'])
    end_date = getdate(doc_dict['end_date'])

    result_status = ""
    try:
        add_batch_to_background_jobs(company=company,
                                     start_date=start_date,
                                     end_date=end_date,
                                     new_batch_id=batch_id,
                                     batch_options=batch_options)
        result_status = "Success"
    except Exception as ex:
        result_status = f"Failed '{ex}'"
        frappe.log_error(message=f"error occurred, '{format_exception(ex)}'", title=batch_process_title)
    finally:
        return result_status


def format_exception(ex: Exception) -> str:
    import traceback
    error = str(ex)
    trace = ''.join(traceback.format_exception(type(ex), ex, ex.__traceback__))
    return f'{error}\n{trace}'
