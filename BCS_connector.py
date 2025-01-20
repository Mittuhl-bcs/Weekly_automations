import pyodbc
import pandas as pd
import warnings
import dask.dataframe as dd
import csv


# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)



def connect_db(query):

    server = "10.240.1.129"
    database = "asp_BUILDCONT"
    username = "buildcont_reports"
    password = "ASP4664bu"


    # connect with credentials
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    connection = pyodbc.connect(connection_string)

    print("Connected to the BCS SSMS database!!")

    # read data into DataFrame
    df = pd.read_sql_query(query, connection)

        
    # return df
    return df, connection



def reader_df(query):
    
      
	df, connection = connect_db(query)

	connection.close()

	return df  
	


def pre_reader(id, query_template):

    # Format the query with the current id
    query = f"""
        SELECT 
        bcs_view_all_quote_detail.location_id, 
        bcs_view_all_quote_detail.sales_location, 
        bcs_view_all_quote_detail.source_loc_id, 
        bcs_view_all_quote_detail.source_location, 
        bcs_view_all_quote_detail.order_date, 
        bcs_view_all_quote_detail.date_booked, 
        bcs_view_all_quote_detail.requested_date, 
        bcs_view_all_quote_detail.customer_id, 
        bcs_view_all_quote_detail.Customer_Name, 
        bcs_view_all_quote_detail.order_no, 
        bcs_view_all_quote_detail.po_no, 
        bcs_view_all_quote_detail.job_name, 
        bcs_view_all_quote_detail.will_call, 
        bcs_view_all_quote_detail.front_counter, 
        bcs_view_all_quote_detail.carrier_name, 
        bcs_view_all_quote_detail.ship_to_id, 
        bcs_view_all_quote_detail.ship_to_name, 
        bcs_view_all_quote_detail.ship2_add1, 
        bcs_view_all_quote_detail.ship2_city, 
        bcs_view_all_quote_detail.ship2_state, 
        bcs_view_all_quote_detail.ship2_zip, 
        bcs_view_all_quote_detail.ship2_country, 
        bcs_view_all_quote_detail.Created_By, 
        bcs_view_all_quote_detail.line_no, 
        bcs_view_all_quote_detail.item_id, 
        bcs_view_all_quote_detail.part_number, 
        bcs_view_all_quote_detail.item_desc, 
        bcs_view_all_quote_detail.extended_desc, 
        bcs_view_all_quote_detail.unit_cost, 
        bcs_view_all_quote_detail.unit_price, 
        bcs_view_all_quote_detail.extended_price, 
        bcs_view_all_quote_detail.unit_quantity, 
        bcs_view_all_quote_detail.qty_ordered, 
        bcs_view_all_quote_detail.approved, 
        bcs_view_all_quote_detail.cancel_flag, 
        bcs_view_all_quote_detail.completed, 
        bcs_view_all_quote_detail.projected_order, 
        bcs_view_all_quote_detail.disposition, 
        bcs_view_all_quote_detail.qty_allocated, 
        bcs_view_all_quote_detail.supplier_id, 
        bcs_view_all_quote_detail.supplier_name, 
        bcs_view_all_quote_detail.salesrep_id, 
        bcs_view_all_quote_detail.first_name, 
        bcs_view_all_quote_detail.last_name, 
        bcs_view_all_quote_detail.validation_status, 
        bcs_view_all_quote_detail.rma_flag, 
        bcs_view_all_quote_detail.Order_Type, 
        bcs_view_all_quote_detail.Order_Source, 
        bcs_view_all_quote_detail.lot_bill, 
        bcs_view_all_quote_detail."# of Lines", 
        bcs_view_all_quote_detail.Total_Price, 
        bcs_view_all_quote_detail.Open_Value, 
        bcs_view_all_quote_detail.Total_Estimated_Cost

        FROM asp_BuildCont.dbo.bcs_view_all_quote_detail

        WHERE 
        bcs_view_all_quote_detail.location_id = {id}    
    """
    print(f"Executing query for id {id}:\n{query}")
    
    df = reader_df(query)
    if df.empty:
        print(f"Query for id {id} returned an empty DataFrame.")
    else:
        print(f"First 5 rows of data for id {id}:\n", df.head())

        
    return df



def pre_reader_orders(id, query_template):

    # Format the query with the current id
    query = f"""SELECT * FROM asp_BuildCont.dbo.Rel_View_Open_Order_Detail Rel_View_Open_Order_Detail
    where (location_id = {id})"""
    
    print(f"Executing query for id {id}:\n{query}")
    
    df = reader_df(query)
    if df.empty:
        print(f"Query for id {id} returned an empty DataFrame.")
    else:
        print(f"First 5 rows of data for id {id}:\n", df.head())
    
    return df


transfers = """SELECT
   transfer_hdr.transfer_no,
   p21_view_location.location_name AS from_name,
   pvl.location_name AS to_name,
   convert(date, transfer_hdr.transfer_date) AS transfer_date,
   convert(date,transfer_hdr.shipping_date) AS shipping_date,
   transfer_hdr.shipped_flag,
   convert(date,transfer_hdr.received_date) AS received_date,
   MAX(oe_line_po.order_number) AS Order_number,
   transfer_hdr.created_by,
   convert(date,transfer_hdr.date_created) AS date_created,
   COALESCE(address.name, '') carrier_name,
   transfer_hdr.approved,
   CASE transfer_hdr.complete_flag WHEN 'Y' THEN 'Y' ELSE 'N' END AS completed,
   transfer_hdr.complete_flag,
   transfer_hdr_ud.transfer_tracking_no,
   transfer_hdr.delete_flag
FROM
   transfer_hdr
   LEFT JOIN address ON address.id = transfer_hdr.carrier_id
   LEFT JOIN oe_line_po ON transfer_hdr.transfer_no = oe_line_PO.po_no
   LEFT JOIN p21_view_location ON transfer_hdr.from_location_id = p21_view_location.location_id
   LEFT JOIN transfer_hdr_ud ON transfer_hdr.transfer_no = transfer_hdr_ud.transfer_no
   LEFT JOIN p21_view_location pvl ON transfer_hdr.to_location_id = pvl.location_id

WHERE
transfer_hdr.delete_flag <>'Y'
AND transfer_hdr.complete_flag <> 'Y'




GROUP BY
transfer_hdr.transfer_no,
   p21_view_location.location_name,
   pvl.location_name,
   convert(date, transfer_hdr.transfer_date),
   convert(date,transfer_hdr.shipping_date),
   transfer_hdr.shipped_flag,
   convert(date,transfer_hdr.received_date),
   transfer_hdr.created_by,
   convert(date,transfer_hdr.date_created),
   COALESCE(address.name, ''),
   transfer_hdr.approved,
   CASE transfer_hdr.complete_flag WHEN 'Y' THEN 'Y' ELSE 'N' END,
   transfer_hdr.complete_flag,
   transfer_hdr_ud.transfer_tracking_no,
   transfer_hdr.delete_flag

   ORDER BY shipping_date, received_date"""



rma_created_not_recorded = """SELECT
p21_view_open_rma_report.order_no AS RMA_Number,
p21_view_open_rma_report.line_no,
p21_view_open_rma_report.item_id,
p21_view_open_rma_report.item_desc,
p21_view_open_rma_report.order_date,
p21_view_open_rma_report.source_location_id,
p21_view_open_rma_report.location_id AS sales_location_id,
p21_view_open_rma_report.taker,
p21_view_open_rma_report.order_salesrep_last_name,
p21_view_open_rma_report.customer_id,
p21_view_open_rma_report.customer_name,
p21_view_open_rma_report.unit_size,
p21_view_open_rma_report.unit_of_measure,
p21_view_open_rma_report.qty_ordered,
p21_view_open_rma_report.qty_canceled,
p21_view_open_rma_report.unit_price,
p21_view_open_rma_report.open_line_value,
p21_view_open_rma_report.approved,
p21_view_open_rma_report.job_id,
p21_view_open_rma_report.qty_open,
p21_view_open_rma_report.location_name AS sales_location_name,
p21_view_open_rma_report.confirmed_receipt_type

FROM
p21_view_open_rma_report

WHERE 
--p21_view_open_rma_report.location_id = '166559' AND
p21_view_open_rma_report.qty_open > 0

Order BY
p21_view_open_rma_report.order_no ASC, p21_view_open_rma_report.line_no;"""


ir_created_not_shipped = """SELECT 
IRH.buyer_id, 
IRH.date_created, 
IRH.inventory_return_hdr_uid, 
IRH.location_id, 
IRH.return_number, 
IRH.rma_number, 
IRH.ship2_name, 
IRH.supplier_id, 
IRL.extended_price, 
-- if extended price = 0, then calculate based upon MOC for item & location
CASE
	WHEN IRL.extended_price = 0 THEN ROUND((IRL.qty_returned-IRL.qty_vouched) * INVLOC.moving_average_cost,2) ELSE 0
END calc_ext_price,
UNVPO.extended_cost_home unvouch_cost,
IRL.item_id, 
IRL.line_number, 
IRL.qty_picked, 
IRL.qty_returned, 
IRL.qty_to_return, 
IRL.qty_vouched, 
--IRL.row_status_flag l_row_status_flag,
--CASE -- Change flag 
--	WHEN IRL.row_status_flag = '702' then 'Open' 
--	WHEN IRL.row_status_flag = '974' then 'Shipped' 
--	WHEN IRL.row_status_flag = '975' then 'Vouched' 
--	WHEN IRL.row_status_flag = '976' then 'Canceled' 
--	else 'unknown'
--END line_stat_desc,
CL.code_description line_status,
--IRH.row_status_flag h_row_status_flag,
CH.code_description header_status,
IRL.supplier_part_number, 
IRL.unit_price l_unit_price, 
IRL.unit_quantity l_unit_quantity,
LOC.location_name

 

FROM 
asp_BuildCont.dbo.p21_view_inventory_return_hdr IRH 
INNER JOIN dbo.p21_view_inventory_return_line IRL WITH (NOLOCK) ON IRH.inventory_return_hdr_uid = IRL.inventory_return_hdr_uid
INNER JOIN p21_view_location LOC ON IRH.location_id = LOC.location_id
INNER JOIN dbo.inv_loc INVLOC WITH (NOLOCK) ON IRH.location_id = INVLOC.location_id and IRL.inv_mast_uid = INVLOC.inv_mast_uid
INNER JOIN code_p21 CL on irl.row_status_flag = CL.code_no
INNER JOIN code_p21 CH on irH.row_status_flag = CH.code_no
LEFT JOIN p21_view_unvouched_po_currency_report UNVPO on convert(varchar(20), IRH.return_number) = UNVPO.unvouched_document_no and IRL.line_number = UNVPO.line_number

 

 

WHERE 
IRH.inventory_return_hdr_uid = IRL.inventory_return_hdr_uid 
AND IRL.row_status_flag not in ('974','975','976')
--AND CL.code_description = 'Shipped'
--AND (IRH.supplier_id=$133602)
--and IRH.return_number > '1085'

 

Order by return_number asc"""


quote_detailed = f"""
SELECT 
bcs_view_all_quote_detail.location_id, 
bcs_view_all_quote_detail.sales_location, 
bcs_view_all_quote_detail.source_loc_id, 
bcs_view_all_quote_detail.source_location, 
bcs_view_all_quote_detail.order_date, 
bcs_view_all_quote_detail.date_booked, 
bcs_view_all_quote_detail.requested_date, 
bcs_view_all_quote_detail.customer_id, 
bcs_view_all_quote_detail.Customer_Name, 
bcs_view_all_quote_detail.order_no, 
bcs_view_all_quote_detail.po_no, 
bcs_view_all_quote_detail.job_name, 
bcs_view_all_quote_detail.will_call, 
bcs_view_all_quote_detail.front_counter, 
bcs_view_all_quote_detail.carrier_name, 
bcs_view_all_quote_detail.ship_to_id, 
bcs_view_all_quote_detail.ship_to_name, 
bcs_view_all_quote_detail.ship2_add1, 
bcs_view_all_quote_detail.ship2_city, 
bcs_view_all_quote_detail.ship2_state, 
bcs_view_all_quote_detail.ship2_zip, 
bcs_view_all_quote_detail.ship2_country, 
bcs_view_all_quote_detail.Created_By, 
bcs_view_all_quote_detail.line_no, 
bcs_view_all_quote_detail.item_id, 
bcs_view_all_quote_detail.part_number, 
bcs_view_all_quote_detail.item_desc, 
bcs_view_all_quote_detail.extended_desc, 
bcs_view_all_quote_detail.unit_cost, 
bcs_view_all_quote_detail.unit_price, 
bcs_view_all_quote_detail.extended_price, 
bcs_view_all_quote_detail.unit_quantity, 
bcs_view_all_quote_detail.qty_ordered, 
bcs_view_all_quote_detail.approved, 
bcs_view_all_quote_detail.cancel_flag, 
bcs_view_all_quote_detail.completed, 
bcs_view_all_quote_detail.projected_order, 
bcs_view_all_quote_detail.disposition, 
bcs_view_all_quote_detail.qty_allocated, 
bcs_view_all_quote_detail.supplier_id, 
bcs_view_all_quote_detail.supplier_name, 
bcs_view_all_quote_detail.salesrep_id, 
bcs_view_all_quote_detail.first_name, 
bcs_view_all_quote_detail.last_name, 
bcs_view_all_quote_detail.validation_status, 
bcs_view_all_quote_detail.rma_flag, 
bcs_view_all_quote_detail.Order_Type, 
bcs_view_all_quote_detail.Order_Source, 
bcs_view_all_quote_detail.lot_bill, 
bcs_view_all_quote_detail."# of Lines", 
bcs_view_all_quote_detail.Total_Price, 
bcs_view_all_quote_detail.Open_Value, 
bcs_view_all_quote_detail.Total_Estimated_Cost

FROM asp_BuildCont.dbo.bcs_view_all_quote_detail

WHERE 
bcs_view_all_quote_detail.location_id = {id}    
"""


open_orders = f"""SELECT * FROM asp_BuildCont.dbo.Rel_View_Open_Order_Detail Rel_View_Open_Order_Detail
where (location_id = {id})"""