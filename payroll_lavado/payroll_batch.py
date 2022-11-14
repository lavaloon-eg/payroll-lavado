
#time sheet  , timesheet record and additional salary and penaltyrecord should be llinkedwith batch no
#We can addd a button in shift type screen to run the batch
#FIXME: ask about the difference between "Last Sync of Checkin" and "Process Attendance After" in the shift type
#add screen payroll lavado screen
    -user should select the company
    - select start date
- select end adte

class PayrollLavaDo:
    def __init__(self):
        self.batch =""

    def action(self):
        #just for ordering
        self.create_resume_batch()
        self.run_auto_attendance() //apply on all shift types in the selected company
        # self.create_timesheet_records()
        self.create_penalty_records()
        self.create_deductions()
    def create_resume_batch(self, start_Date, end_Date, company):
        if there is a batch older than the start date and end date with unsuccessful status:
            raise exception with msg and log in error log screen
            the resume batch should be identical with the batch inputs
            "batch in progress and start date and date not equal the current batch"
        batch should be filtered by company
        batch_ref = ""
        if not exist or last_batch_successed or last patch terminated:
            create anew batch record and set as the current patch
            #batch_attrs:
                # start date
                # end date(end of batch runtime) set in the complete success only
                # Start time(creation)
                #
            batch_ref =  the new created record
        else:
            resume this patch and set as the current running batch

        get batch_ref

        #TODO:separate the logic
        shift_types = get all shift_types filtered by company
        for shift_type in shift_types:
            if there is any missing data" raise exception"
            self.run_auto_attendance(shift_type)

            update the "last sync on, Process Attendance After" to be the batch end date
        for employee in get_all employees filtered by company and
            if last_proccessed employee >employee and  resuming_abatch and it is the first employee in the new batch :
                delete_employee_batch_record(employee, batch_ref)#delete penalty records and additional salary timesheets linked to penalty record
            #TODO: Should we check the salary structure as on that date (upon the employee changelog)
            timesheet_record =self.create_resume_employee_timesheet(self, employee, attendance_list, start_date,end_date)

            for attendance in employee.get_attendance_list(between start_Date and end_Date ):
                employee_record = get employee changelog record(date= most recent < = record near the attendance.date, employee)
                calc_attendance_working_hours_breakdowns(attendance)
                self.add_timesheet_record( timesheet_record, activity_type=hardcoded, date, duration_in_hours="equation")
                self.add_penalties(self, employee_record, batch, date)
            self.update_batch(employee)

        update_batch_status(batch)


    def update_batch(employee):
            last
    def add_penalties(self, employee_record, batch, date):
        for penalty_record in the date for employee :
            self.add_additional_salary(penalty_record) if needed
        for penalty_policy related to employee(designation in penalty policy equals designation in the employee_record):
            """adding logic of appying penalty policy"""
            self.add_penalty_record(self, employee)


    def add_penalty_record(self, employee):
        adds penalties records for the employee
        call self.add_additional_salary(penalty_record) if needed


    def add_additional_salary(penalty_record):

    def calc_attendance_working_hours_breakdowns(self,attendance):
        if not set:
            sum the values and update into the attendance
    def create_resume_employee_timesheet(self, employee, attendance_list, start_date,end_date):
        #TODO:Future enhancement avoid duplications in timesheet creation
        if the timesheet exists identical start_Date and end_date for the employee:
            get_timesheet_refrence
            update batch in timesheet
        else:
            create
        for  attendance in attendance_list:
            calc_attendance_working_hours_breakdowns(attendance)

            self.add_timesheet_record(attendance)

    def calc_attendance_working_hours(self, attendance_record)
        update the attendance record


    def add_timesheet_record(self, parent_timesheet, activity_type, date, duration_in_hours):
        add timesheet record





    def run_auto_attendance(self):
        """ CAll the standard function creates attendance records"""
        attendance_records = get_standard_Attendance_records
         self.create_penalty_records(attendance_records)

    def create_timesheet_records(self, attendance_records):
        1- Group attendance records by employee
        for record in attendance_records:

            # 2- timesheets = reate time sheet for each employee then append all attendance records as child task in the time sheet.(start time is checkin time , end is checkout) #I think it is redundant
            # self.create_penalty_records(timesheets)
            self.create_penalty_records(attendance_records)



    def create_penalty_records(self, attendance_records):
        penalty_list = []
        for record in attendance_records:
            penalty = create penalty_record
            penalty_list.append(penalty)
        self.create_deductions(penalty_list)

    def create_deductions(self, penalty_list):
        for penalty in penalties:
            get total deductions
                create deduction record(salary_additional for the total)








