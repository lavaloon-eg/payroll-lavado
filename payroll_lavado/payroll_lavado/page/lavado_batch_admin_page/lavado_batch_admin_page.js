
frappe.pages['lavado_batch_admin_page'].on_page_load = function (wrapper) {
	new MyPage(wrapper);
}

MyPage = Class.extend({
	init: function (wrapper) {
		this.page = frappe.ui.make_app_page({
			parent: wrapper,
			title: 'Payroll LavaDo Batch Manager',
			single_column: false
		});
		this.make();
	},
	make: function () {
		$(frappe.render_template("lavado_batch_admin_page", this)).appendTo(this.page.main);
        $(document).ready(function(){
             frappe.db.get_list('Company', {
                    fields: ['name']
                }).then(records => {
                    let option = new Option('Select a company', 'Select a company');
                    $("#select-company").append(option);
                    for (var counter in records){
                        option = new Option(records[counter]['name'], records[counter]['name']);
                        $("#select-company").append(option);
                    }
                });
        });
        $('#select-company').change(function(){
            company_changed();
        });

        $('#select-shift').change(function(){
            get_employees_by_filters();
        });

        $('#select-branch').change(function(){
            get_employees_by_filters();
        });

        $('#btn-refresh').click(function(){
            get_batches();
        });
		$('#btn-run-batch').click(function(){
		    let action_type = "New Batch";
		    let doc_data = get_validate_inputs(batch_id = null,action_type = "action_type");
    		run_batch(doc_data);
    		get_batches();
		 });
	}
})

function company_changed(){
        let batch_company = $("#select-company :selected").text();
        get_batches(batch_company);
        get_branches_by_company(batch_company);
        get_shifts_by_company(batch_company);
        get_employees_by_filters();
}

function process_batch_action(action_type, batch_id){
    // alert('action: ' + action_type + ' for batch: ' + batch_id );
    let doc_data = get_validate_inputs(batch_id = batch_id, action_type = action_type);
    run_batch(doc_data);
}

function get_validate_inputs(batch_id, action_type){
    let batch_company = $("#select-company :selected").text();
    let selectedBranches = convert_selected_options_into_csv("select-branch");
    let selectedShifts = convert_selected_options_into_csv("select-shift");
    let selectedEmployees = convert_selected_options_into_csv("select-employee");
    let batch_start_date = new Date($('#batch-start-date').val());
    let batch_end_date = new Date($('#batch-end-date').val());
    let chk_batch_debug_mode =(($("#chk-batch-debug-mode").is(":checked"))? 1 : 0);
    let chk_clear_error_log_records =(($("#chk-clear-error-log-records").is(":checked"))? 1 : 0);
    let chk_clear_action_log_records = (($("#chk-clear-action-log-records").is(":checked"))? 1 : 0);
    let chk_auto_attendance = (($("#chk-auto-attendance").is(":checked"))? 1 : 0);
    let chk_biometric_process = (($("#chk-biometric-process").is(":checked"))? 1 : 0);
    let chk_batch_objects = (($("#chk-batch-objects").is(":checked"))? 1 : 0);
    let error_msg = "";
    if (action_type == "New Batch"){
        if (isNaN(batch_end_date) || isNaN(batch_start_date)){
            error_msg += ", select dates";
        }
        if (batch_end_date <= batch_start_date){
            error_msg += ", end date must be > start date";
        }
    }

    if (error_msg.length >0){
        frappe.msgprint(__("error message: " + error_msg));
        return;
    }
    let doc_data={
        "company": batch_company,
        "branches": selectedBranches,
        "shifts": selectedShifts,
        "employees": selectedEmployees,
        "start_date": batch_start_date,
        "end_date": batch_end_date,
        "chk-batch-debug-mode": chk_batch_debug_mode,
        "chk-clear-error-log-records": chk_clear_error_log_records,
        "chk-clear-action-log-records": chk_clear_action_log_records,
        "chk-batch-objects": chk_batch_objects,
        "chk-auto-attendance": chk_auto_attendance,
        "chk-biometric-process": chk_biometric_process,
        "batch_id": batch_id,
        "action_type": action_type
    }
    return doc_data;
}

function convert_selected_options_into_csv(select_control_id){
    let selected_options = Array();
    $(`#${select_control_id} :selected`).each(function(index){
        selected_options[index] = "'" + $(this).val() + "'" ;
    });
    if (selected_options.length == 0){
        return null;
    }
    let csv_value =  selected_options.join(',');
    return csv_value;
}

function run_batch(doc_data){
	frappe.call(
		method= 'payroll_lavado.payroll_batch.run_payroll_lavado_batch',
		args= {
			doc: doc_data,
		},
		freeze= false,
		callback= function (r) {
            if (r.message == "Success") {
                frappe.msgprint(__("Action Done"));
            } else {
                frappe.throw(__(r.message));
            }
		}
	);
}

function get_batches(batch_company){
    doc_data = {"company": batch_company};
    frappe.call({
            method:
                "payroll_lavado.payroll_lavado.page.lavado_batch_admin_page.lavado_batch_admin_page.get_payroll_lavado_batches",
            args: {
                filters: doc_data,
            },
            callback: function (r) {
            if (r.message.message == "Success") {
                if(r.message.result){
                    render_batches_data(r.message.result);
                }
            } else {
                frappe.throw(__(r.message.message));
            }
            }
        })
}

function get_employees_by_filters(){
    $("#select-employee").empty();
    let batch_company = $("#select-company :selected").text();
    let selectedBranches = convert_selected_options_into_csv("select-branch");
    let selectedShifts = convert_selected_options_into_csv("select-shift");
    //alert(`branches: ${selectedBranches}`);
    //alert(`shifts: ${selectedShifts}`);

    let filters = {"company": batch_company,
                    "branches": selectedBranches,
                    "shifts": selectedShifts};
    frappe.call({
            method:
                "payroll_lavado.payroll_lavado.page.lavado_batch_admin_page.lavado_batch_admin_page.get_employees_by_filters",
            args: {
                filters: filters,
            },
            callback: function (r) {
            if (r.message.message == "Success") {
                if(r.message.result){
                    render_select_options("select-employee", "employee_id","employee_name",r.message.result);
                }
            } else {
                frappe.throw(__(r.message.message));
            }
            }
        })
}

function get_branches_by_company(company){
    $("#select-branch").empty();
    let filters = {"company": company};
    frappe.call({
            method:
                "payroll_lavado.payroll_lavado.page.lavado_batch_admin_page.lavado_batch_admin_page.get_branches_by_company",
            args: {
                filters: filters,
            },
            callback: function (r) {
            if (r.message.message == "Success") {
                if(r.message.result){
                    render_select_options("select-branch", "branch","branch",r.message.result);
                }
            } else {
                frappe.throw(__(r.message.message));
            }
            }
        })
}


function get_shifts_by_company(company){
    $("#select-shift").empty();
    let filters = {"company": company};
    frappe.call({
            method:
                "payroll_lavado.payroll_lavado.page.lavado_batch_admin_page.lavado_batch_admin_page.get_shifts_by_company",
            args: {
                filters: filters,
            },
            callback: function (r) {
            if (r.message.message == "Success") {
                if(r.message.result){
                    render_select_options("select-shift", "shift", "shift",r.message.result);
                }
            } else {
                frappe.throw(__(r.message.message));
            }
            }
        })
}
function render_select_options(select_tag, text_field_name, value_field_name, records){
    for (var record of records){
        //alert(record[field_name]);
        let option = new Option(record[value_field_name], record[text_field_name]);
        $(`#${select_tag}`).append(option);
    }
}

function render_batches_data(records){

    if ($("#tbody-batches").length ){
        if ($("#tbody-batches").rows)
            for (let i=0;i< $("#tbody-batches").rows.length ;i++){
                $('#tbody-batches').deleteRow(i);
            }
    }
    if (records.length == 0){
        return;
    }
    if (!$("#tr-head").length){
        let first_record = records[0];
        $("#table-batches").append('<thead>').append("<tr id='tr-head'>");

        for(let key of Object.keys(first_record)){
            $("#tr-head").append(`<th class="col grid-static-col col-xs-4 ">${key}</th>`);
        }
        $("#tr-head").append(`<th class="col grid-static-col col-xs-4 ">Actions</th>`);
        $("#table-batches").append('<tbody id="tbody-batches">');
    }
    rowIndex = 0;
    for (var record of records){
       //console.log(record)
       if ($(`#tr${rowIndex}`).length)
       {
            $(`#tr${rowIndex}`).remove();
       }
       $('#tbody-batches').append(`<tr id=tr${rowIndex}>`);
       $(`#tr${rowIndex}`).append(`<td>${record.batch_id}</td>`);
       $(`#tr${rowIndex}`).append(`<td>${record.company}</td>`);
       $(`#tr${rowIndex}`).append(`<td>${record.start_date}</td>`);
       $(`#tr${rowIndex}`).append(`<td>${record.end_Date}</td>`);       //TODO: fix field name (should rename it in doc)
       $(`#tr${rowIndex}`).append(`<td>${record.status}</td>`);
       $(`#tr${rowIndex}`).append(`<td>${record.batch_process_start_time}</td>`);
       $(`#tr${rowIndex}`).append(`<td>${record.batch_process_end_time}</td>`);
       if (record.status != "Completed"){
           let button_tag = '<td><button onclick="process_batch_action(action_type=`Resume Batch`, batch_id=`'+ record.batch_id +  '`)">Resume</button></td>'
           $(`#tr${rowIndex}`).append($(button_tag));
       }
       rowIndex += 1;
    }
}