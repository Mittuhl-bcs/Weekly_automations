import pandas as pd
import numpy as np
import pyodbc
import pandas as pd
import warnings
import dask.dataframe as dd
import csv
import cgna_mailer


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


query = """select il.item_id, 
il.location_id, 
il.qty_on_hand-il.qty_allocated-il.qty_backordered qty_available, 
il.primary_supplier_id, 
im.item_desc AS Description, 
s.supplier_name AS Supplier,
iss.supplier_part_no AS Supplier_PART_Number,
il.standard_cost,
il.moving_average_cost,
il.stockable,
ilt.std_cost highest_std_cost, 
ilt.highest_mac, 
case when ilt.std_cost > ilt.highest_mac then ilt.std_cost else ilt.highest_mac end highest_cost
--'1.11' margin, (case when ilt.std_cost > ilt.highest_mac then ilt.std_cost else ilt.highest_mac end)  ABShighest_cost

from p21_view_inv_loc il
inner join (select il.item_id, max(il.moving_average_cost) highest_mac, sum((il.qty_on_hand-il.qty_allocated-il.qty_backordered)) as total_qty_avail, max(il.standard_cost) std_cost
			from p21_view_inv_loc il
			where il.qty_on_hand <> 0
			group by il.item_id
			) ilt on il.item_id = ilt.item_id
inner join p21_view_inv_mast im on il.inv_mast_uid = im.inv_mast_uid
inner join p21_view_supplier s on s.supplier_id = il.primary_supplier_id
inner join p21_view_inventory_supplier iss on s.supplier_id = iss.supplier_id and il.inv_mast_uid = iss.inv_mast_uid
where il.qty_on_hand <> 0
AND (il.primary_supplier_id NOT IN ($130001,$130002, $130003, $130020, $130031, $133986, $130032, $130006, $133377, $130007, $149445, $130027, $133986, $130032, $168923, $130036, $130037, $130039, $130040, $134250, $130008, $130006, $130031, $130014, $130033, $130038, $133672, $175097, $134139, $130044, $166773, $133923, $183570, $183716))
AND (il.location_id NOT IN ($170607,$10007))
AND (il.item_id NOT IN ('HWI-ML7984A4009U'))
order by il.item_id, il.location_id"""




def get_location_name(location_id):
    location_map = {
        166553: "AUSTIN",
        166557: "DALLAS",
        166559: "HOUSTON",
        166560: "NEW ORLEANS",
        166561: "SAN ANTONIO",
        10006: "PHOENIX",
        10008: "SALT LAKE CITY",
        10010: "SAN DIEGO",
        173042: "BOSTON",
        175886: "CHARLOTTE",
        175890: "NEW YORK",
        175888: "NEW JERSEY",
        175883: "CHARLOTTE",
        10750: "WEST ALLIS",
        10520: "ST PAUL",
        10510: "MINNEAPOLIS",
        176046: "SALT LAKE CITY",
        175892: "GREEN BAY",
        175891: "MILWAUKEE"
    }
    
    # Return the city name if found, otherwise return 'NULL'
    return location_map.get(location_id, "NULL")


def calculate_cgna_price(row):
    if row['Supplier_PART_Number'] == "FX-PCG2611-0G":
        return row['highest_mac'] * 1.1
    elif row['Supplier_PART_Number'] == "GDK4080":
        return 240
    elif row['Supplier_PART_Number'] == "801111301":
        return 59
    elif row['primary_supplier_id'] == 133921:
        return row['highest_cost'] / 0.75
    elif row['primary_supplier_id'] == 134012:
        return row['highest_cost'] / 0.85
    elif row['primary_supplier_id'] == 134065:
        return row['highest_cost'] / 0.87
    elif row['primary_supplier_id'] == 134342:
        return row['highest_cost'] / 0.87
    elif row['primary_supplier_id'] == 134496:
        return row['highest_cost'] / 0.87
    elif row['primary_supplier_id'] == 133634:
        return row['highest_cost'] / 0.87
    elif row['primary_supplier_id'] == 133922:
        return row['highest_cost'] / 0.87
    elif row['Supplier_PART_Number'] == "F4-CVM09090":
        return row['highest_cost'] / 0.87
    elif row['primary_supplier_id'] == 133476:
        return row['highest_cost'] / 0.87
    elif row['primary_supplier_id'] == 166773:
        return row['highest_cost'] / 0.84
    else:
        return row['highest_cost'] / 0.9


# Define the function to determine if location is "SELLABLE STOCK"
def location_for_stockable(row):
    sellable_locations = [
        "NEW YORK", "NEW JERSEY", "MINNEAPOLIS", "ST PAUL", "SAN ANTONIO", 
        "PHOENIX", "BOSTON", "SALT LAKE CITY", "AUSTIN", "DALLAS", "HOUSTON", 
        "NEW ORLEANS"
    ]
    
    if row['Location Name'] in sellable_locations:
        return "SELLABLE STOCK"
    else:
        return "NO"


# Define the function to apply the logic for 'CC' column
def calculate_cc(row):
    if row['concated'] == "SELLABLE STOCK-N":
        return "Y"
    else:
        return "N"

def filter_final(row):
    if row['CC'] == "Y":
        return "yes"
    elif row["GM $Margin"] < 10:
        return "no"
    else:
        return "yes"


df = reader_df(query)
# Apply the function to create a new column 'city_name'
df['Location Name'] = df['location_id'].apply(get_location_name)

# Apply the function to calculate the CGNA price and create a new column 'cgna_price'
df['CGNA Price'] = df.apply(calculate_cgna_price, axis=1)

# Apply the function to create the new column 'Location for stockable'
df['Location for stockable'] = df.apply(location_for_stockable, axis=1)

# Create the new column 'column3' by concatenating 'Location for stockable' and 'stockable'
df['column3'] = df['Location for stockable'] + '-' + df['stockable'].astype(str)

# Create the new column 'GM $Margin' by subtracting 'moving_average_cost' from 'CGNA Price'
df['GM $Margin'] = df['CGNA Price'] - df['moving_average_cost']

# Concatenating the columns with a hyphen and creating a new column 'concated'
df['concated'] = df['Location For Stockable'] + '-' + df['stockable']

# Apply the function to create the 'CC' column
df['CC'] = df.apply(calculate_cc, axis=1)

# Create the new column 'GM%' by dividing 'GM $Margin' by 'CGNA Price'
df['GM%'] = df['GM $Margin'] / df['CGNA Price']



# filter out the data (no null, GREEN BAY), 
df = df[df["Location Name"].notnull()]
df = df[~df["Location Name"].isin(["GREEN BAY"])]
df.to_excel("D:\\Brian's report automation\\CGNA\\CGNA_report_original.xlsx", index=False)


# then filter out the data (N in CC)
df = df[df["CC"] == "Y"]
df["Final_check"] = df.apply(filter_final, axis=1)

df = df[df["Final_check"] == "yes"]

df = df[["item_id", "Description", "qty_available", "Supplier", "CGNA Price", "Location Name"]]

print(df.head())

df.rename(columns={
    "Description": "item_desc", 
    "qty_available": "Qty_Available", 
    "Supplier": "supplier_name", 
    "CGNA Price": "MAC+10%", 
    "Location Name" :"Location_name"
}, inplace=True)

df[["item_id", "item_desc", "Qty_Available", "supplier_name", "MAC+10%", "Location_name"]]
df.to_excel("D:\\Brian's report automation\\CGNA\\BCS_CGNA.xlsx", index=False)

cgna_mailer.sender("D:\\Brian's report automation\\CGNA")