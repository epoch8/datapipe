import gzip
import mimetypes
import re
from datetime import date, datetime
from typing import Dict, List, Optional, Text, Union
import fsspec

import lxml
import pandas as pd
from lxml import etree

from pipe import Source

def get_first_or_none_from_list(items: list):
    return items.pop() if items else None


def get_first_or_none(els):
    texts = [el.text for el in els]
    return get_first_or_none_from_list(texts)


STRING_TYPE = str
INT_TYPE = int
UNIT_TYPE = {STRING_TYPE, INT_TYPE}
DICT_TYPE = dict

LIST_STRING_TYPE = List[STRING_TYPE]
LIST_INT_TYPE = List[INT_TYPE]
LIST_UNIT_TYPE = {LIST_STRING_TYPE, LIST_INT_TYPE}
LIST_DICT_TYPE = List[DICT_TYPE]

# https://support.google.com/merchants/answer/7052112?hl=en
GFEED_SCHEMA = [
    dict(zip(("name", "type", "syntax"), el))
    for el in [
        # Basic product data
        ("g:id", STRING_TYPE, "Max 50 characters"),
        ("g:title", STRING_TYPE, "Max 150 characters"),
        ("g:description", STRING_TYPE, "Max 5000 characters"),
        ("g:link", STRING_TYPE, "1-2000"),
        ("g:image_link", STRING_TYPE, "1-2000"),
        ("g:additional_image_link", LIST_STRING_TYPE, "1-2000"),
        ("g:mobile_link", STRING_TYPE, "1-200"),
        # Price & availability
        ("g:availability", STRING_TYPE, None),
        ("g:availability_date", STRING_TYPE, "25"),
        # ("g:expiration_date", STRING_TYPE, ""),
        ("g:price", STRING_TYPE, None),
        ("g:sale_price", STRING_TYPE, None),
        ("g:sale_price_effective_date", STRING_TYPE, None),
        # ("g:cost_of_goods_sold", STRING_TYPE, ""),
        ("g:unit_pricing_measure", STRING_TYPE, ""),
        ("g:unit_pricing_base_measure", STRING_TYPE, ""),
        # ("g:installment", DICT_TYPE, ""),
        # ("g:subscription_cost", ...),
        # ("g:loyalty_points", ...),
        # Product category
        ("g:google_product_category", STRING_TYPE, None),
        ("g:product_type", STRING_TYPE, None),
        # Product identifiers
        ("g:brand", STRING_TYPE, "Max 70 characters"),
        ("g:gtin", STRING_TYPE, None),
        ("g:mpn", STRING_TYPE, None),
        ("g:identifier_exists", STRING_TYPE, "'no' or 'yes'"),
        # Detailed product description
        ("g:condition", STRING_TYPE, "'new', 'refurbished' or 'used'"),
        ("g:age_group", STRING_TYPE, None),
        ("g:color", STRING_TYPE, "Max 40 characters per color"),
        ("g:gender", STRING_TYPE, "'male', 'female' or 'unisex'"),
        ("g:material", STRING_TYPE, ""),
        ("g:pattern", STRING_TYPE, ""),
        ("g:size", STRING_TYPE, "Max 100 characters"),
        ("g:size_type", STRING_TYPE, "regular, petite, oversize, maternity"),
        ("g:size_system", STRING_TYPE, "US, UK etc"),
        ("g:item_group_id", STRING_TYPE, "Max 50 alphanumeric characters"),
        ("g:product_detail", LIST_DICT_TYPE, "1-1000"),
        ("g:product_highlight", STRING_TYPE, "Max 150 characters"),
        # Shopping campaigns and other configurations
        ## ads_redirect, custom_label_0 - custom_label_4, promotion_id
        # Destinations
        ## excluded_destination, shopping_ads_excluded_country, included_destination
        # Shipping
        ("g:shipping", DICT_TYPE, None),
        ("g:shipping_label", STRING_TYPE, "Max 100 characters"),
        ("g:shipping_weight", STRING_TYPE, "Number + unit (lb, oz, g, kg)"),
        ("g:shipping_length", STRING_TYPE, "Number + unit (in, cm)"),
        ("g:shipping_height", STRING_TYPE, "Number + unit (in, cm)"),
        ("g:shipping_width", STRING_TYPE, "Number + unit (in, cm)"),
        ## transit_time_label, max_handling_time, min_handling_time
        # Tax
        ## tax, tax_category
    ]
]

def extract_element(element):
    tag = etree.QName(element).localname
    result = element.text
    return tag, result

def extract_elements(elements):
    tag = etree.QName(elements[0]).localname
    result = [element.text for element in elements]
    return tag, result


def extract_dict_of_element(element):
    tag = etree.QName(element).localname

    result = dict()
    for children in element.getchildren():
        tag, text = extract_element(children)
        result[tag] = text

    return tag, result

def extract_dict_of_elements(elements):
    tag = etree.QName(elements[0]).localname

    result = []
    for element in elements:
        dict_element = extract_dict_of_element(element)
        result.append(dict_element)

    return tag, result

def _extract_element(element):
    namespaces = namespaces = {"g": "http://base.google.com/ns/1.0"}
    for schema_el in GFEED_SCHEMA:
        el = element.xpath(schema_el["name"], namespaces=namespaces)
        if not el:
            continue
        elif schema_el["type"] in UNIT_TYPE:
            el = extract_element(el[0])
            if el:
                yield el
        elif schema_el["type"] in LIST_UNIT_TYPE:
            el = extract_elements(el)
            if el:
                yield el
        elif schema_el["type"] == DICT_TYPE:
            yield extract_dict_of_element(el[0])
        elif schema_el["type"] == LIST_DICT_TYPE:
            yield extract_dict_of_elements(el)


def get_elements(f):
    for _, element in etree.iterparse(f, tag="item"):
        content = dict()
        for tag, text in _extract_element(element):
            content[tag] = text
        
        content['_id'] = content['id']

        yield content


def gfeed(url):
    with fsspec.open(url) as f:
        return pd.DataFrame.from_records(
            get_elements(f), 
            index='_id'
        )


def SrcGfeed(url):
    return Source(gfeed, args=(url,))