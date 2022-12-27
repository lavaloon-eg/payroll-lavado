import frappe


@frappe.whitelist()
def get_payroll_lavado_batches(filters: dict = None):
    return frappe.db.sql(f"""
                                select name as 'batch_id',
                                company, start_date, end_Date , status,
                                batch_process_start_time,
                                batch_process_end_time
                                from `tabPayroll LavaDo Batch`
                                where company = '{filters['company']}'
                                order by start_date desc, batch_process_start_time desc
                                LIMIT 30
                            """
                         , as_dict=1)
