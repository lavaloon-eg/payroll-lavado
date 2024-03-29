from . import __version__ as app_version

app_name = "payroll_lavado"
app_title = "Payroll Lavado"
app_publisher = "LavaLoon"
app_description = "Define & Apply Payroll Penalties"
app_icon = "octicon octicon-file-directory"
app_color = "grey"
app_email = "sales@lavaloon.com"
app_license = "GNU Affero General Public License v3.0"

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/payroll_lavado/css/payroll_lavado.css"
# app_include_js = "/assets/payroll_lavado/js/payroll_lavado.js"

# include js, css files in header of web template
# web_include_css = "/assets/payroll_lavado/css/payroll_lavado.css"
# web_include_js = "/assets/payroll_lavado/js/payroll_lavado.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "payroll_lavado/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
#	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Installation
# ------------

# before_install = "payroll_lavado.install.before_install"
# after_install = "payroll_lavado.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "payroll_lavado.uninstall.before_uninstall"
# after_uninstall = "payroll_lavado.uninstall.after_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "payroll_lavado.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

doc_events = {
    "Employee": {
        "on_update": "payroll_lavado.payroll_lavado.standard_doctypes.hooks.employee.employee_create_employee_change_log",
    },
    "Employee Transfer": {
        "on_update": "payroll_lavado.payroll_lavado.standard_doctypes.hooks.employee_transfer.employee_transfer_create_employee_change_log"
    },
    "Shift Assignment": {
        "on_update": "payroll_lavado.payroll_lavado.standard_doctypes.hooks.shift_assignment.shift_assignment_create_employee_change_log",
    },
    "Salary Structure Assignment": {
        "on_update": "payroll_lavado.payroll_lavado.standard_doctypes.hooks.salary_structure_assignment.salary_structure_assignment_create_employee_change_log",
    }
}

# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"payroll_lavado.tasks.all"
# 	],
# 	"daily": [
# 		"payroll_lavado.tasks.daily"
# 	],
# 	"hourly": [
# 		"payroll_lavado.tasks.hourly"
# 	],
# 	"weekly": [
# 		"payroll_lavado.tasks.weekly"
# 	]
# 	"monthly": [
# 		"payroll_lavado.tasks.monthly"
# 	]
# }

# Testing
# -------

# before_tests = "payroll_lavado.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "payroll_lavado.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "payroll_lavado.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]


# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"payroll_lavado.auth.validate"
# ]

# Translation
# --------------------------------

# Make link fields search translated document names for these DocTypes
# Recommended only for DocTypes which have limited documents with untranslated names
# For example: Role, Gender, etc.
# translated_search_doctypes = []
