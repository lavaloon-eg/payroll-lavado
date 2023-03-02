import json

import frappe


@frappe.whitelist()
def get_branches_by_company(filters: str = None):
    result = {}
    message = ""
    filters_dict = json.loads(filters)
    try:
        result = frappe.db.sql(f"""
                                    select distinct branch
                                    from `tabLava Employee Payroll Changelog`
                                    where branch is not Null and company = %(company)s
                                    order by branch
                                    LIMIT 100
                                """, {'company': filters_dict['company']}, as_dict=1)
        message = "Success"
    except Exception as ex:
        message = "During getting the branches, Error occurred: '{}'".format(str(ex))
    finally:
        return {'result': result, 'message': message}


@frappe.whitelist()
def get_shifts_by_company(filters: str = None):
    result = {}
    message = ""
    filters_dict = json.loads(filters)
    try:
        result = frappe.db.sql(f"""
                                    select distinct shift_type as shift
                                    from `tabLava Employee Payroll Changelog`
                                    where shift_type is not Null and company = %(company)s
                                    order by shift_type
                                    LIMIT 100
                                """, {'company': filters_dict['company']}, as_dict=1)
        message = "Success"
    except Exception as ex:
        message = "During getting the shift types, Error occurred: '{}'".format(str(ex))
    finally:
        return {'result': result, 'message': message}


@frappe.whitelist()
def get_employees_by_filters(filters: str = None):
    result = {}
    message = ""
    try:
        query_string = ""
        filters_dict = json.loads(filters)
        company = filters_dict['company']
        branches = filters_dict['branches']
        shifts = filters_dict['shifts']
        # TODO: handle the result's limit
        query_string = f"""
                            select distinct emp.name as employee_id, emp.employee_name
                            from `tabLava Employee Payroll Changelog` as chg 
                            inner join `tabEmployee` as emp
                            on chg.employee= emp.name
                            where chg.company = %(company)s
                        """
        if branches:
            query_string += f"""
                                and chg.branch in ({branches})
                            """
        if shifts:
            query_string += f"""
                               and chg.shift_type in ({shifts})
                           """

        query_string += f"""
                            order by emp.employee_name
                            LIMIT 10000
                        """

        result = frappe.db.sql(query_string, {'company': company
                                              }, as_dict=1)
        message = "Success"
    except Exception as ex:
        message = "During getting the employees, Error occurred: '{}'".format(str(ex))
    finally:
        return {'result': result, 'message': message}


@frappe.whitelist()
def get_payroll_lavado_batches(filters: str = None):
    result = {}
    message = ""
    filters_dict = json.loads(filters)
    try:
        result = frappe.db.sql(f"""
                                    select name as 'batch_id',
                                    company, start_date, end_Date , status,
                                    batch_process_start_time,
                                    batch_process_end_time
                                    from `tabLava Payroll LavaDo Batch`
                                    where company = %(company)s
                                    order by name desc, start_date desc,
                                    modified desc
                                    LIMIT 50
                                """, {'company': filters_dict['company']}, as_dict=1)
        message = "Success"
    except Exception as ex:
        message = "During getting the batches, Error occurred: '{}'".format(str(ex))
    finally:
        return {'result': result, 'message': message}
