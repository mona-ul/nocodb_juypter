import requests
from datetime import datetime, timezone
from urllib.parse import quote

# Configuration
#xc_token = "KVwjEgUQOwJkjYih9tef5J7A54z05RHliR_1GoSs"
#table_id = "me77dgzmhjqvqgv"
#view_id = "vwdmq4dujnk9ekbg"
#noco_base_url = "https://tables.rhizome.org"
#headers = {
#    'accept': 'application/json',
#    'xc-token': xc_token,
#}

# set timestamp
now = datetime.now(timezone.utc)
formatted_datetime = now.strftime("%Y%m%d%H%M%S")
print("Formatted datetime:", formatted_datetime)

# Specify the output file path
output_file = f"media_urls{formatted_datetime}.txt"

# Specify Log File
log_path = f"log_file_{formatted_datetime}.txt"

## get rows
# Function to fetch data from the NocoDB table
def fetch_rows_from_nocodb(noco_base_url, headers, table_id, view_id, offset, limit):
    request_url = f"{noco_base_url}/api/v2/tables/{table_id}/records?viewId={view_id}&offset={offset}&limit={limit}"
    print(f"Fetching data from offset {offset}...")
    response = requests.get(request_url, headers=headers)
    if response.status_code == 200:
        print("test3")
        data = response.json()
        return data.get('list', [])
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return []


# Fetch all posts (you can execute this cell to fetch posts)
def fetch_all_rows_from_nocodb(noco_base_url, headers, table_id, view_id):
    limit = 1000
    all_posts = []
    offset = 0

    while True:
        print("test1")
        posts = fetch_rows_from_nocodb(noco_base_url, headers, table_id, view_id, offset, limit)
        if not posts:
            print("test2")
            break
        all_posts.extend(posts)
        print(f"Fetched {len(posts)} posts from offset {offset}")
        offset += limit
        if len(posts) < limit:
            break
    print(f"Total posts fetched: {len(all_posts)}")
    return all_posts


# Get Value to key from rows
def get_key_values_from_extracted_rows_dict(rows_data, fieldname):
    ids_in_table = {}
    if rows_data:
        for row in rows_data:
            # ids_in_table.append(row['Id'])
            ids_in_table[row[fieldname]] = (row[fieldname])
        return ids_in_table

def get_key_values_from_extracted_rows_list(rows_data, fieldname):
    return [row[fieldname] for row in rows_data if fieldname in row]

## get data
# from nocodb_get_column_from_rows_by_primary_key in link_tables
def get_row_by_key_value_bu(noco_base_url, headers, table_id, fieldname, value):
    url = f'{noco_base_url}/api/v2/tables/{table_id}/records?fields=Id%2C{fieldname}&where=%3D%28{fieldname}%2Ceq%2C{value}%29'
    #print(url)

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if 'list' in data and data['list']:
            print(data)
            return data  # Return matching records
        else:
            print(f"No records found for {fieldname} = {value}.")
            return None
    else:
        print(f"Failed to fetch data for {fieldname} = {value}. Status code: {response.status_code}")
        return None

def get_row_by_key_value(noco_base_url, headers, table_id, fieldname, value):
    url = f'{noco_base_url}/api/v2/tables/{table_id}/records?fields=Id%2C{fieldname}&where=%3D%28{fieldname}%2Ceq%2C{value}%29'
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'list' in data and data['list']:
                #print("list is there")
                return data['list'][0]  # Return the first matching record
            else:
                print("The list is empty or does not exist in the data.")
        else:
            print(f"Failed to fetch data for {fieldname}={value}. Status: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {fieldname}={value}: {e}")

    return None  # Always return something

def get_row_by_key_value_view_id(noco_base_url, headers, table_id, view_id, fieldname, value):
    url = f'{noco_base_url}/api/v2/tables/{table_id}/records?viewId={view_id}&fields=id%2C{fieldname}&where=where%3D%28{fieldname}%2Ceq%2C{value}%29'
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'list' in data and data['list']:
                #print("list is there")
                return data['list'][0]  # Return the first matching record
            else:
                print("The list is empty or does not exist in the data.")
        else:
            print(f"Failed to fetch data for {fieldname}={value}. Status: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {fieldname}={value}: {e}")

    return None  # Always return something
    
def get_row_by_key_value_full_row(noco_base_url, headers, table_id, fieldname, value):
    url = f'{noco_base_url}/api/v2/tables/{table_id}/records?where=%28{fieldname}%2Ceq%2C{value}%29'
    #print(url)

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            #print(data)
            if 'list' in data and data['list']:
                #print("list is there")
                return data['list'][0]  # Return the first matching record
            else:
                print("The list is empty or does not exist in the data.")
        else:
            print(f"Failed to fetch data for {fieldname}={value}. Status: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {fieldname}={value}: {e}")

    return None  # Always return something

def get_row_by_key_value_full_row_list(noco_base_url, headers, table_id, fieldname, value):
    url = f'{noco_base_url}/api/v2/tables/{table_id}/records?where=%28{fieldname}%2Ceq%2C{value}%29'
    #print(url)

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            #print(data)
            if 'list' in data and data['list']:
                #print("list is there")
                return data  # Return the first matching record
            else:
                print("The list is empty or does not exist in the data.")
        else:
            print(f"Failed to fetch data for {fieldname}={value}. Status: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {fieldname}={value}: {e}")

    return None  # Always return something
    
def get_row_by_primary_key_bu(noco_base_url, headers, table_id, id):
    url = f'{noco_base_url}/api/v2/tables/{table_id}/records/{id}'
    url_encoded = quote(url)
    #print(url)
    #print(url_encoded)

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Failed to fetch data for {id} {3974}. Status code: {response.status_code}")

    return data
    
def get_row_by_primary_key(noco_base_url, headers, table_id, id):
    url = f"{noco_base_url}/api/v2/tables/{table_id}/records/{id}"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch data for {id}. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error for {id}: {e}")
    
    return None  # Ensure a return value even on failure


def link_tables(noco_base_url, headers, table_id, table_id_for_link, link_field_id, id):
    url = f'{noco_base_url}/api/v2/tables/{table_id}/links/{link_field_id}/records/{id}'
    data = {
        "id": table_id_for_link
    }

    response = requests.post(url, headers=headers, json=data)
    print(response.text)

def update_row(noco_base_url, headers, table_id, record_id, index_status, index_check_field):
    """Update the index_check column in NocoDB."""
    request_url = (
        f"{noco_base_url}/api/v2/tables/{table_id}/records"
    )
    data = {
        "Id": record_id,
        index_check_field: index_status
    }
    #print(request_url)
    #print(data)
    try:
        response = requests.patch(request_url, headers=headers, json=data)
        if response.status_code == 200:
            pass
            #print(f"Updated record {record_id} -> index_check: {index_status}")
        else:
            print(f"Failed to update record {record_id}. Status: {response.status_code}")
            print(f"Response text: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error updating record {record_id}: {e}")

def update_row_textfield(noco_base_url, headers, table_id, record_id, column, value):
    """Update the index_check column in NocoDB."""
    request_url = (
        f"{noco_base_url}/api/v2/tables/{table_id}/records"
    )
    data = {
        "Id": record_id,
        column: value
    }
    #print(request_url)
    #print(data)
    try:
        response = requests.patch(request_url, headers=headers, json=data)
        if response.status_code == 200:
            pass
            #print(f"Updated record {record_id} -> index_check: {index_status}")
        else:
            print(f"Failed to update record {record_id}. Status: {response.status_code}")
            print(f"Response text: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error updating record {record_id}: {e}")
        

def create_row(noco_base_url, headers, table_id, record_id, index_status, index_check_field):
    request_url = (
        f"{noco_base_url}/api/v2/tables/{table_id}/records"
    )
    data = {
        "Id": record_id,
        index_check_field: index_status
    }
    
    try:
        response = requests.post(request_url, headers=headers, json=data)
        if response.status_code == 200:
            print(f"Updated record {record_id} -> new value: {index_status}")
        else:
            print(f"Failed to update record {record_id}. Status: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error updating record {record_id}: {e}")
