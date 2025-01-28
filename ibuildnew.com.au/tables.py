#!/usr/bin/env python3
"""
File: tables.py
Author: ro9naja
Email: ro9naja@email.com
Github: https://github.com/ro9naja
Description: airtables models
"""

from os import getenv

from pyairtable.orm import Model, fields as F
from pyairtable import Api
from pyairtable.api.table import Table
from dotenv import load_dotenv

load_dotenv()


def get_secret(key: str) -> str:
    secret = getenv(key)
    if not secret:
        raise ValueError(f"Missing environment variable: {key}")
    return secret


class ATVendor(Model):
    builder_id = F.IntegerField("builder_id")
    name = F.TextField("Name")
    info = F.TextField("Info")
    description = F.TextField("Description")
    phone_number = F.PhoneNumberField("Phone Number")
    primary_contact = F.TextField("Primary Contact")
    primary_contact_phone = F.PhoneNumberField("Primary Contact Phone")
    website = F.UrlField("Website")
    facebook = F.UrlField("Facebook")
    twitter = F.UrlField("Twitter")
    instagram = F.UrlField("Instagram")
    video = F.UrlField("Video")
    awards = F.TextField("Awards")

    class Meta:
        @staticmethod
        def base_id():
            return get_secret("BASE_ID")

        @staticmethod
        def table_name():
            return get_secret("TBL_ID_VENDOR")

        @staticmethod
        def api_key():
            return get_secret("ACCESS_TOKEN")


class ATProduct(Model):
    name = F.TextField("Name")
    base_price = F.CurrencyField("Base Price")
    gallery = F.AttachmentsField("Gallery")
    vendor = F.LinkField("Vendor", ATVendor)
    bedrooms = F.IntegerField("Bedrooms")
    bathrooms = F.IntegerField("Bathrooms")
    living_spaces = F.IntegerField("Living Spaces")
    car_spaces = F.IntegerField("Car Spaces")
    floors = F.IntegerField("Floors")
    study = F.SelectField("Study")
    alfresco = F.SelectField("Alfresco")
    duplex = F.SelectField("Duplex")
    floor_plan = F.AttachmentsField("Floor Plan")
    house_size = F.FloatField("House Size")
    block_width = F.FloatField("Block Width")
    block_length = F.FloatField("Block Length")
    description = F.TextField("Description")
    tour_3d = F.UrlField("3D Tour")
    build_locations = F.TextField("Build Locations")

    class Meta:
        @staticmethod
        def base_id():
            return get_secret("BASE_ID")

        @staticmethod
        def table_name():
            return get_secret("TBL_ID_PRODUCT")

        @staticmethod
        def api_key():
            return get_secret("ACCESS_TOKEN")


def get_vendor_table() -> Table:
    return Api(api_key=get_secret("ACCESS_TOKEN")).table(
        get_secret("BASE_ID"), get_secret("TBL_ID_VENDOR")
    )


def generate_vendor_mapping() -> dict[int, str]:
    vdb = get_vendor_table().all(fields=["builder_id"])
    return {v["fields"]["builder_id"]: v["id"] for v in vdb}
