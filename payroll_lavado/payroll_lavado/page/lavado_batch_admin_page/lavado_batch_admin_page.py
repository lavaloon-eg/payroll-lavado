import frappe


@frappe.whitelist()
def get_payroll_lavado_batches(filters: dict = None):
    result = {}
    message = ""
    try:
        result = frappe.db.sql(f"""
                                    select name as 'batch_id',
                                    company, start_date, end_Date , status,
                                    batch_process_start_time,
                                    batch_process_end_time
                                    from `tabPayroll LavaDo Batch`
                                    where company = %(company)s
                                    order by start_date desc, batch_process_start_time desc
                                    LIMIT 30
                                """, {'company': filters['company']}, as_dict=1)
        message = "Success"
    except Exception as ex:
        message = "During getting the batches, Error occurred: '{}'".format(str(ex))
    finally:
        return result, message
