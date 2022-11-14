# Airtable -> Baserow Importer

This is an Airtable to Baserow importer, which works a bit differently from Baserow's built-in Airtable import feature.

## How this is different

Unlike the built-in Airtable import feature, which creates a new database with tables and fields identical to the ones in Airtable (minus some field types), this script imports data from Airtable into existing Baserow tables, with a fully customizable mapping of Airtable fields to Baserow fields.

It can import linked records correctly as long as the fields are mapped, and data can even be imported between different field types. The importer will do its best to intelligently convert the data type, but it won't always be able to. For example, you can import data from a text field in Airtable into a number field in Baserow, as long as the records actually contain numeric values.

## Basic Usage

The importer reads your field mapping from a JSON file. You can generate a template for this by running the `generate_template_field_map` function.

```python
from airtable_baserow_importer import generate_template_field_map

generate_template_field_map()
```

Then after putting in the Airtable & Baserow IDs of each base, table, and field in this file, you can run the importer.

```python
from airtable_baserow_importer import do_import

do_import("my_field_map.json", my_airtable_key, my_baserow_key)
```

## Customizable Behavior

You can customize the import behavior per-field by passing functions into the `do_import` function. The `conversion_functions` parameter takes a dictionary mapping Baserow field IDs to functions with the following signature:

```python
def conversion_function(airtable_field_value, baserow_field_data, default_conversion_function)
```

* `airtable_field_value` is the value returned by the Airtable API for the field.
* `baserow_field_data` is the data returned by the Baserow API's "List fields" endpoint for the field.
* `default_conversion_function` is a function that takes a value and runs the default conversion function for this field on it. Typically you would want to either call this on the airtable value first and modify the result, or modify the airtable value first and then call this on that value.

## Potential Issues

If you get errors while running the importer, make sure that:

* You have filled in the field map json file correctly, with string Airtable table/field IDs (or names) mapping to integer Baserow table/field IDs.
* You are not trying to import data into a field type that cannot accept it, e.g. importing a text field containing non-numeric strings into a number field.
* You are not trying to import into a field that is read-only (formulas, lokups, etc). You can import data *from* these field types in Airtable, but not into them in Baserow.
* You are not trying to import multiple Airtable fields into the same Baserow field.

Also, an unfortunate side-effect of the way this importer works is that if the import fails due to an error halfway through, you will have to delete all the data in Baserow that has been imported so far, and then start it over after solving the issue.
