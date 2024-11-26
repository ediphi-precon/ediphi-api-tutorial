"""
Helper functions for the ediphi data pipeline
"""

import os
import json
import re
import requests
from dotenv import load_dotenv

load_dotenv()


def call_pipe(payload):
    """
    This function is used to call the Ediphi API pipeline. All calls are POSTS to the same
    endpoint with different payloads that serve as the request for the data.
    """

    url = "https://api.ediphi.com/api/external/data/pipeline"
    headers = {
        "api-token": os.getenv("API_KEY"),
        "api-tenant": os.getenv("TENANT").lower(),
        "Content-Type": "application/json",
    }
    res = requests.post(url, headers=headers, json=payload, timeout=30)
    if res.status_code != 200:
        error_msg = (
            res.json().get("error", {}).get("message", "Unknown pipeline error")
        ) + json.dumps(payload)
        raise requests.exceptions.HTTPError(error_msg)
    return res.json().get("data", {}).get("load", {})


def get_table(table_name, properties=None):
    """
    This function. given a table name and any properties used to filter the data, will
    return the data from the table using the datapipeline.
    """

    payload = {
        f"#{table_name}#": {
            "table": table_name,
            "operation": {"method": "load.multiple()"},
        }
    }
    if properties:
        payload[f"#{table_name}#"]["operation"]["properties"] = properties

    data = call_pipe(payload)
    return data.get(table_name, {})


def get_line_items_for_estimate(estimate_id):
    """
    An example of using the pipe class to return all line items for a table for a given estimate
    """

    line_items = get_table("line_items", {"estimate": estimate_id})
    line_items = decode_sort_fields(line_items)
    line_items = flatten_mfuf(line_items)
    return line_items


def get_sort_codes_for(properties):
    """
    Given a properties of a sort field, return it with all the sort codes associated with it.
    Common properties are {"id": "sort_field.id"} or {"name": "sort_field.name"}
    """

    payload = {
        "#sort_field#": {
            "table": "sort_fields",
            "operation": {
                "method": "load",
                "properties": properties,
            },
        },
        "#sort_code_{index.count()}#": {
            "table": "sort_codes",
            "operation": {
                "method": "load.multiple()",
                "properties": {"sort_field": "#sort_field#"},
            },
        },
    }
    try:
        data = call_pipe(payload)
        sort_codes = {sc["id"]: sc for sc in data.get("sort_codes", [])}
        res = {
            "sort_field": data.get("sort_fields", {})[0],
            "sort_codes": sort_codes,
        }
        return res
    except requests.exceptions.HTTPError:
        properties["error"] = "Sort field not found"
        res = {
            "sort_field": properties,
            "sort_codes": {},
        }
        return res


def get_line_items_sort_fields(line_items):
    """
    Given a list of line items, this function will return a dictionary with the sort fields
    and sort codes for them. Line item sort fields and codes are within the line_item.extras key
    where the key is the sort field id and the value is the sort code id.
    """

    unique_sort_field_ids = set()

    for line_item in line_items:
        extras = line_item.get("extras", {})
        unique_sort_field_ids.update(extras.keys())

    sort_fields = {}
    for sf_id in list(unique_sort_field_ids):
        sort_fields[sf_id] = get_sort_codes_for(properties={"id": sf_id})

    return sort_fields


def decode_sort_fields(line_items):
    """
    This function is used to decode the sort fields for the line items or products.
    It takes a list of line items or products as input and returns the same list with the
    sorfields decoded, meaning that the extras key with value {sort_field_id: sort_code_id}
    will be decoded into new line item props {sort_field.name: sort_code.name}
    """

    sort_fields = get_line_items_sort_fields(line_items)
    for li in line_items:
        extras = li.get("extras", {})
        for sf_id, sc_id in extras.items():
            sort_field = sort_fields[sf_id]["sort_field"]
            sort_codes = sort_fields[sf_id]["sort_codes"]
            if sort_codes == {}:
                continue
            li[sort_field["name"] + "_desc_"] = sort_codes.get(sc_id, {}).get(
                "description", sc_id
            )
            li[sort_field["name"]] = sort_codes.get(sc_id, {}).get("code", sc_id)

    return line_items


def flatten_mfuf(line_items_or_products, top_level_only=True):
    """
    Master Format and UniFormat properties of a line_item are the "mf" and "uf" keys.
    They are nested hierachies of codes in this format:
    {"mf1": "03.00.00", "mf2": "03.30.00", "mf3": "03.30.13"}

    This function flattens them into separate keys
    """

    def extract_level(mfuf_key):
        match = re.search(r"\d+", mfuf_key)
        return int(match.group()) if match else 0

    for li in line_items_or_products:
        mf = li.get("mf", {})
        uf = li.get("uf", {})

        if top_level_only:
            if mf:
                max_key = max(mf, key=extract_level)
                li[max_key] = mf[max_key]
            if uf:
                max_key = max(uf, key=extract_level)
                li[max_key] = uf[max_key]
        else:
            for k, v in mf.items():
                li[k] = v
            for k, v in uf.items():
                li[k] = v
        del li["mf"]
        del li["uf"]

    return line_items_or_products
