frappe.pages['lavado_batch_admin_page'].on_page_load = function (wrapper) {
	new MyPage(wrapper);
}

MyPage = Class.extend({
	init: function (wrapper) {
		this.page = frappe.ui.make_app_page({
			parent: wrapper,
			title: 'Payroll LavaDo Batch Manager',
			single_column: true
		});
		this.make();
	},
	make: function () {
		$(frappe.render_template("lavado_batch_admin_page", this)).appendTo(this.page.main);
		$('#btn-run-batch').onclick(function(){

		    //FIXME: reading html controls
		    let batch_company = $("#select-company").value
		    let batch_start_date = $("#batch-start-date").value
		    let batch_end_date = $("#batch-end-date").value
		    let chk-clear-error-log-records = $("#chk-clear-error-log-records").checked
		    let chk-clear-action-log-records = $("#chk-clear-action-log-records").checked
		    let chk-batch-objects = $("#chk-batch-objects").checked
		    //TODO: validate data
		    let doc_data={
		        "company": batch_company,
		        "start_date": batch_start_date,
		        "end_date": batch_end_date,
		        "chk-clear-error-log-records": chk-clear-error-log-records,
		        "chk-clear-action-log-records": chk-clear-action-log-records,
		        "chk-batch-objects": chk-batch-objects
		    }
    		run_batch(doc_data);
		 });
	}
})


function run_batch(doc_data) {
	frappe.call({
		method:
			"payroll_lavado.payroll_lavado.page.lavado_batch_admin_page.lavado_batch_admin_page.run_payroll_lavado_batch",
		args: {
			doc: doc_data,
		},
		callback: function (r) {
		if (r.message == "Success") {
            frappe.msgprint(__("Ran batch"));
		} else {
            frappe.throw(__(r.message));
		}
		}
	})
}