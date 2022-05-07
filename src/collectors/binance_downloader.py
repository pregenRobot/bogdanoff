import requests
from peewee import *
import shutup
from datetime import datetime
import uuid
import configparser
from typing import *
import boto3
import tempfile
from zipfile import ZipFile
import io
import re

data_dump_root = "../../dumps"
db = SqliteDatabase(f"{data_dump_root}/binance.db")
shutup.please()


class Prefix(Model):
    uuid = CharField()
    parent_uuid = CharField()

    name = CharField()
    child_count = IntegerField()
    path = CharField()

    created_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        database = db

    @staticmethod
    def create(parent_uuid: str, name: str, path:str, child_count: int = -1):
        return Prefix(
                uuid=uuid.uuid4(),
                parent_uuid = parent_uuid,
                name = name,
                path = path,
                child_count = child_count,
                created_at = datetime.now(),
                updated_at = datetime.now()
                )


class Key(Model):
    uuid = CharField()
    parent_uuid = CharField()

    name = CharField()
    path = CharField()

    created_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        database = db

    @staticmethod
    def create(parent_uuid: str, name: str, path: str):
        return Key(
                uuid = uuid.uuid4(),
                parent_uuid = parent_uuid,
                name  = name,
                path = path,
                created_at = datetime.now(),
                updated_at = datetime.now()
                )


class CrawlLog(Model):
    uuid = CharField()
    command = CharField()
    response_body = CharField()
    created_at = DateTimeField()

    class Meta:
        database = db


db.connect()


config = configparser.ConfigParser()
config.read("./config.ini")

crawldata_config = {
    "aws_access_key_id": config["BUCKETS"]["CrawlDataAccessKey"],
    "aws_secret_access_key": config["BUCKETS"]["CrawlDataAccessSecret"],
    "endpoint_url": config["BUCKETS"]["EndpointUrl"]
}

client = boto3.client("s3", **crawldata_config)

query = (Key.select(Key.path, Key.name))

def extract_zip(input_zip) -> bytes:
    input_zip=ZipFile(io.BytesIO(input_zip))
    return input_zip.read(input_zip.namelist()[0])

def download_csv(key:Key):
    url = f"https://data.binance.vision/{key.path}"
    response = requests.get(url, stream=True)
    extracted_bytes = extract_zip(response.content)
    return extracted_bytes.decode("utf-8", "ignore")

for key in query:
    if key.path.endswith("CHECKSUM"):
        continue

    csv = download_csv(key)
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as fp:
        fp.write(csv)

        bucket_file_path = "/".join(key.path.split("/")[:-1])
        bucket_file_name = re.sub(".zip$", ".csv", key.path.split("/")[-1])

        file_key = bucket_file_path + "/" + bucket_file_name
        client.upload_file(
            Filename=fp.name,
            Bucket='crawldata',
            Key=file_key
        )

