from __future__ import unicode_literals
from frappe import _
import frappe


def get_data():
	return [
		{
			"label": _("Reports"),
			"items": [
				{
					"type": "report",
					"name": "Sales Order Paid Detail",
					"is_query_report": True,
					"doctype": "Sales Order"
				}
			]
		}		
    ]
