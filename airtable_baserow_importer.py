import re, json, requests, os
from pyairtable import Table

DATETIME_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}))?Z")
DATE_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
NUMERIC_PATTERN = re.compile(r"(\D*)([\d,]+(?:\.\d+)?)(\D*)")

def __prefer_single_value(value):
    """
    Returns the value unchanged, unless the value is a single element list, then returns the element.
    If the value is an empty list, returns None.
    """

    if type(value) is list:
        if len(value) == 0:
            return None
        elif len(value) == 1:
            value = value[0]

    return value

def __require_single_value(value):
    """
    If the value is a single element list, return the element.
    If the value is an empty list, returns None.
    Otherwise, throws an error.
    """

    value = __prefer_single_value(value)
    if type(value) is list:
        raise Exception("Single value required")

    return value

def __require_numeric_value(value):
    value = __require_single_value(value)
    if value is None or value == "":
        return None
    
    if type(value) in (int, float):
        return value

    match = NUMERIC_PATTERN.match(value)
    if not match:
        raise Exception("Invalid numeric format: " + value)
    
    return match.group(2).replace(",", "")

def __require_single_value_string(value, field_data):
    value = __require_single_value(value)
    if value is None:
        return None

    return str(value)

def __to_text(value, field_data):
    value = __prefer_single_value(value)
    if type(value) is list:
        return ", ".join(value.map(str)).replace("\n", " ")
    elif value is None:
        return ""
    else:
        return str(value).replace("\n", " ")
    
def __to_long_text(value, field_data):
    value = __prefer_single_value(value)
    if type(value) is list:
        return "\n".join(value.map(str))
    elif value is None:
        return ""
    else:
        return str(value)

def __to_number(value, field_data):
    value = __require_numeric_value(value)
    if value is None:
        return None

    precision = field_data["number_decimal_places"]
    if precision == 0:
        value = int(value)

    else:
        value = int(float(value) * 10 ** precision) / 10 ** precision

    if not field_data["number_negative"] and value < 0:
        value = 0
    
    return value

def __to_rating(value, field_data):
    value = __require_numeric_value(value)
    if value is None:
        return None

    value = int(value)
    if value < 1:
        value = 1
    elif value > field_data["max_value"]:
        value = field_data["max_value"]
    
    return value

def __to_boolean(value, field_data):
    value = __require_single_value(value)
    return bool(value)

def __to_date(value, field_data):
    value = __require_single_value(value)
    if value is None or value == "":
        return None
    
    if field_data["date_include_time"]:
        if not DATETIME_PATTERN.match(value):
            raise Exception("Invalid datetime format")
        
    else:
        if not DATE_PATTERN.match(value):
            raise Exception("Invalid date format")
    
    return value

def __find_select_option_id(value, select_options):
    value = str(value)
    for option in select_options:
        if option["value"] == value:
            return option["id"]
    
    raise Exception("Invalid select option: " + value)

def __to_single_select(value, field_data):
    value = __require_single_value(value)
    if value is None or value == "":
        return None
    
    return __find_select_option_id(value, field_data["select_options"])

def __to_multi_select(value, field_data):
    if value is None or value == "":
        return None
    
    selected = []
    for v in value:
        selected.append(__find_select_option_id(v, field_data["select_options"]))
    
    return selected


CONVERSION_FUNCTIONS = {
    "text": __to_text,
    "long_text": __to_long_text,
    "url": __require_single_value_string,
    "email": __require_single_value_string,
    "number": __to_number,
    "rating": __to_rating,
    "boolean": __to_boolean,
    "date": __to_date,
    "single_select": __to_single_select,
    "multiple_select": __to_multi_select,
    "phone_number": __require_single_value_string,
}

def __convert_fields(fields, fields_map, fields_data, conversion_functions, links, files):
    """
    given the Airtable fields, the mapping of Airtable field names to Baserow field ids,
    the Baserow fields data, and the user defined conversion functions,
    returns an object that can be submitted to the Baserow API to create a new row with the data from the Airtable fields.
    """

    converted_fields = {}
    for at_field_name, at_field_value in fields.items():
        if at_field_name not in fields_map:
            continue
        
        br_field_id = fields_map[at_field_name]
        if br_field_id not in fields_data:
            raise Exception("Baserow field not found: field_" + str(br_field_id))
        
        br_field_type = fields_data[br_field_id]["type"]

        # store the linked records for later conversion
        if br_field_type == "link_row":
            if type(at_field_value) is not list or (len(at_field_value) > 0 and type(at_field_value[0]) is not str):
                raise Exception("Baserow link fields can only be mapped from Airtable link fields")

            links[br_field_id] = at_field_value
            continue

        # store the file for later upload
        if br_field_type == "file":
            if type(at_field_value) is not list or (len(at_field_value) > 0 and (type(at_field_value[0]) is not dict or "url" not in at_field_value[0])):
                raise Exception("Baserow file fields can only be mapped from Airtable attachment fields")
            
            files[br_field_id] = at_field_value
            continue

        if br_field_type not in CONVERSION_FUNCTIONS:
            raise Exception("Can't import into Baserow field: field_" + str(br_field_id) + ", unsupported field type: " + br_field_type)

        # run the custom conversion function if one is defined, otherwise run the default conversion function
        default_conversion_function = CONVERSION_FUNCTIONS[br_field_type]
        br_field_value = None
        if br_field_id in conversion_functions:
            br_field_value = conversion_functions[br_field_id](at_field_value, fields_data[br_field_id], lambda val : default_conversion_function(val, fields_data[br_field_id]))
        
        else:
            br_field_value = default_conversion_function(at_field_value, fields_data[br_field_id])
        
        if br_field_value is not None:
            converted_fields["field_" + str(br_field_id)] = br_field_value
    
    return converted_fields

def do_import(field_map_fp: str, airtable_token: str, baserow_token:str, conversion_functions: dict[int, callable] = {}, batch_size: int = 200, baserow_url: str = "https://api.baserow.io", quiet: bool = False):
    """
    Imports data from Airtable into Baserow.
    Uses the JSON from the file at the provided `field_map_fp` to map Airtable bases, tables, and fields to Baserow.
    A template JSON file can be generated with the `generate_template_field_map` function.

    For self-hosted instances of Baserow, the `baserow_url` parameter can be used to specify the URL of the Baserow instance.

    Custom conversion functions can be provided for each Baserow field with the `conversion_functions` parameter.
    The keys should be Baserow field IDs and the values should be functions with the following signature:
    ```
    def conversion_function(airtable_field_value, baserow_field_data, default_conversion_function)
    ```
    * `airtable_field_value` is the value returned by the Airtable API for the field.
    * `baserow_field_data` is the data returned by the Baserow API's "List fields" endpoint for the field.
    * `default_conversion_function` is a function that takes a value and runs the default conversion function for this field on it. Typically you would want to either call this on the airtable value first and modify the result, or modify the airtable value first and then call this on that value.
    """

    with open(field_map_fp) as f:
        field_map = json.load(f)
    
    api_url = baserow_url.strip("/") + "/api"

    for base_id, base_data in field_map["bases"].items():
        if not quiet: print(f"Importing records from {base_id}...")
        created_records = {}
        record_map = {}
        links = {}
        files = {}
        # first pass, create all the records without filling in the link fields or file fields
        for at_table_id, table_data in base_data["tables"].items():
            br_table_id = table_data["id"]
            created_records[br_table_id] = []
            links[br_table_id] = {}
            files[br_table_id] = {}

            # get the baserow fields data for the table, and convert it to a mapping of field ids to the data
            response = requests.get(api_url + f"/database/fields/table/{br_table_id}/", headers={ "Authorization": "Token " + baserow_token })
            if response.status_code >= 400:
                raise Exception("Error getting Baserow field data: " + response.text)

            br_fields_data_arr = response.json()
            br_fields_data = {}
            for field_data in br_fields_data_arr:
                br_fields_data[field_data["id"]] = field_data
            
            # get all the records from airtable, and create them in Baserow
            # keep track of which airtable records map to which baserow records, so we can fill in the link fields later
            at_ids = []
            create_records = []
            at_table = Table(airtable_token, base_id, at_table_id)
            iterator = (v for v in at_table.all())
            while True:
                record = next(iterator, None)
                if record is not None:
                    links[br_table_id][record["id"]] = {}
                    files[br_table_id][record["id"]] = {}
                    item = __convert_fields(record["fields"], table_data["fields"], br_fields_data, conversion_functions, links[br_table_id][record["id"]], files[br_table_id][record["id"]])
                    at_ids.append(record["id"])
                    create_records.append(item)

                if len(create_records) > 0 and (len(create_records) > batch_size - 1 or record is None):
                    response = requests.post(api_url + f"/database/rows/table/{br_table_id}/batch/", headers={ "Authorization": "Token " + baserow_token }, json={ "items": create_records })
                    if(response.status_code >= 400):
                        raise Exception("Error creating records: " + response.text)

                    create_records = []

                    for i, item in enumerate(response.json()["items"]):
                        created_records[br_table_id].append(item["id"])
                        record_map[at_ids[i]] = item["id"]
                    
                    at_ids = []
                
                if record is None:
                    break
    
        # second pass, fill in the link fields
        if not quiet: print("Mapping linked records...")
        for br_table_id, table_data in links.items():
            new_table_data = {}
            for at_record_id, record_data in table_data.items():
                br_record_id = record_map[at_record_id]
                new_table_data[br_record_id] = record_data

            links[br_table_id] = new_table_data
            for br_record_id, record_data in new_table_data.items():
                for br_field_id, at_linked_record_ids in record_data.items():
                    br_linked_record_ids = []
                    for at_linked_record_id in at_linked_record_ids:
                        br_linked_record_ids.append(record_map[at_linked_record_id])
                    
                    record_data[br_field_id] = br_linked_record_ids
            
            # now that we have mapped the baserow record ids, we can fill in the link fields
            update_records = []
            already_linked = set()
            iterator = (v for v in created_records[br_table_id])
            while True:
                record_id = next(iterator, None)
                if record_id is not None:
                    if record_id in already_linked or record_id not in links[br_table_id] or len(links[br_table_id][record_id]) == 0:
                        continue

                    record = {
                        "id": record_id,
                    }

                    for br_field_id, linked_field_ids in links[br_table_id][record_id].items():
                        record["field_" + str(br_field_id)] = linked_field_ids

                        for linked_field_id in linked_field_ids:
                            already_linked.add(linked_field_id)

                    update_records.append(record)

                if len(update_records) > 0 and (len(update_records) > batch_size - 1 or record_id is None):
                    response = requests.patch(api_url + f"/database/rows/table/{br_table_id}/batch/", headers={ "Authorization": "Token " + baserow_token }, json={ "items": update_records })
                    if(response.status_code >= 400):
                        raise Exception("Error adding linked records: " + response.text)

                    update_records = []
                
                if record_id is None:
                    break

        # third pass, fill in the file fields
        if not quiet: print("Uploading files...")
        for br_table_id, table_data in files.items():
            new_table_data = {}
            for at_record_id, record_data in table_data.items():
                br_record_id = record_map[at_record_id]
                new_table_data[br_record_id] = record_data

            files[br_table_id] = new_table_data
            for br_record_id, record_data in new_table_data.items():
                for br_field_id, at_files in record_data.items():
                    for i, file_data in enumerate(at_files):
                        file_content = requests.get(file_data["url"]).content
                        response = requests.post(api_url + "/user-files/upload-file/", headers={ "Authorization": "Token " + baserow_token }, files={ "file": (file_data["filename"], file_content, file_data["type"]) })
                        if(response.status_code >= 400):
                            raise Exception("Error uploading file: " + response.text)

                        at_files[i] = response.json()["name"]
        
            # now that we have mapped the baserow record ids and uploaded the files, we can fill in the file fields
            update_records = []
            iterator = (v for v in files[br_table_id].keys())
            while True:
                record_id = next(iterator, None)
                if record_id is not None:
                    file_fields = files[br_table_id][record_id]
                    if len(file_fields) == 0:
                        continue

                    record = {
                        "id": record_id,
                    }

                    for br_field_id, file_names in file_fields.items():
                        record["field_" + str(br_field_id)] = list(map(lambda file_name : { "name": file_name }, file_names))
                    
                    update_records.append(record)

                if len(update_records) > 0 and (len(update_records) > batch_size - 1 or record_id is None):
                    response = requests.patch(api_url + f"/database/rows/table/{br_table_id}/batch/", headers={ "Authorization": "Token " + baserow_token }, json={ "items": update_records })
                    if(response.status_code >= 400):
                        raise Exception("Error adding files to records: " + response.text)

                    update_records = []
                
                if record_id is None:
                    break
    
    if not quiet: print("Done!")

def generate_template_field_map():
    """
    Generates the proper JSON structure for the importer, and saves it to a file.
    """

    field_map = {
        "bases": {
            "(Airtable Base ID)": {
                "tables": {
                    "(Airtable Table ID/Name)": {
                        "id": 1,
                        "fields": {
                            "(Airtable Field ID/Name)": 1,
                        }
                    }
                }
            }
        }
    }

    if os.path.exists("field_map.json"):
        print("field_map.json already exists, not overwriting")
        return

    with open("field_map.json", "w") as f:
        json.dump(field_map, f, indent=4)
