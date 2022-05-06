import requests
import os
from pathlib import Path
from peewee import *
import shutup
from datetime import datetime
import uuid


from typing import *

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

query = (Key.select(Key.path, Key.name))

for key in query:

    # Example url
    # https://data.binance.vision/data/futures/um/daily/klines/1000BTTCUSDT/12h/1000BTTCUSDT-12h-2022-04-11.zip

    url = f"https://data.binance.vision/{key.path}"
    print(url)
    response = requests.get(url, stream = True)

    local_folder_path = data_dump_root + "/downloads/" + "/".join(key.path.split("/")[:-2])
    print(local_folder_path)
    Path(local_folder_path).mkdir(parents=True, exist_ok=True)

    local_file_path = local_folder_path + "/" + key.name
    print(local_file_path)
    
    with open(local_file_path, "wb+") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                f.write(chunk)




