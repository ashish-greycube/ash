# Copyright (c) 2013, GreyCube Technologies and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
from re import DEBUG
import frappe
from frappe import _, scrub

def execute(filters=None):
	columns, data = [], []
	data =get_data(filters)
	columns = get_columns(filters)

	return columns, data


def get_columns(filters):
	columns = [
		{
			"fieldname":"name",
			"label": _("SO Refernce"),
			"fieldtype": "Link",
			"options": "Sales Order",
			"width": 80
		},		
		{
			"fieldname":"transaction_date",
			"label": _("SO Date"),
			"fieldtype": "Date",
			"width": 90
		},		
		{
			"fieldname":"customer",
			"label": _("Customer"),
			"fieldtype": "Link",
			"options": "Customer",
			"width": 140
		},		
		{
			"fieldname":"customer_name",
			"label": _("Customer Name"),
			"fieldtype": "Data",
			"width": 140
		},			
		{
			"fieldname":"customer_group",
			"label": _("Customer Group"),
			"fieldtype": "Link",
			"options": "Customer Group",
			"width": 130
		},
		{
			"fieldname":"id_cf",
			"label": _("ID"),
			"fieldtype": "Data",
			"width": 60
		},			
		{
			"fieldname":"mobile_number_cf",
			"label": _("Mobile No"),
			"fieldtype": "Data",
			"width": 80
		}	,
		{
			"fieldname":"plot_no_cf",
			"label": _("Plot No"),
			"fieldtype": "Data",
			"width": 60
		},
		{
			"fieldname":"land_no_cf",
			"label": _("Land No"),
			"fieldtype": "Data",
			"width": 60
		},		
		{
			"fieldname":"so_total_amount",
			"label": _("Total SO Amount"),
	  	"fieldtype": "Float",
			"width": 100
		},		
		{
			"fieldname":"so_paid_amount",
			"label": _("Paid Amount"),
	  	"fieldtype": "Float",
			"width": 100
		},
		{
			"fieldname":"so_outstanding_amount",
			"label": _("Outstanding Amount"),
	  	"fieldtype": "Float",
			"width": 100
		}								
	]

	return columns

def get_data(filters):
	conditions = []

	conditions.append(" where 1=1")
	if filters.get("company"):
		conditions.append(" and so.company = %(company)s")
	if filters.get("customer"):
		conditions.append(" and so.customer = %(customer)s")
	if filters.get("id_cf"):
		conditions.append(" and so.id_cf = %(id_cf)s")
	if filters.get("mobile_number_cf"):
		conditions.append(" and so.mobile_number_cf = %(mobile_number_cf)s")
	if filters.get("from_date"):
		conditions.append(" and transaction_date >= %(from_date)s")
	if filters.get("to_date"):
		conditions.append(" and transaction_date <= %(to_date)s")				
	if filters.get("customer_group"):
				lft, rgt = frappe.db.get_value("Customer Group",filters.get("customer_group"), ["lft", "rgt"])
				conditions.append(""" and so.customer in (select name from tabCustomer
					where exists(select name from `tabCustomer Group` where lft >= {0} and rgt <= {1}
						and name=tabCustomer.customer_group))""".format(lft, rgt))

	conditions=" ".join(conditions)
	data= frappe.db.sql(
			"""select so.name, so.transaction_date,so.customer,so.customer_name,so.customer_group,
	so.id_cf, so.mobile_number_cf, so.plot_no_cf,so.land_no_cf,
	so.grand_total as so_total_amount,
	sum(si.base_rounded_total-si.outstanding_amount) as so_paid_amount, (so.grand_total- sum(si.base_rounded_total-si.outstanding_amount)) as so_outstanding_amount
	from `tabSales Order` so  left outer join `tabSales Invoice Item` si_item 
	inner join `tabSales Invoice` si on si.name=si_item.parent 
	on so.name=si_item.sales_order 
	{conditions}
	group by so.name
	order by so.transaction_date desc 
					""".format(
				conditions=conditions),
			filters, as_dict=1)	

	return data
	