import subprocess
from peewee import *
import subprocess
from bs4 import BeautifulSoup as Soup
from datetime import datetime
import time
import shutup
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
db.create_tables([Prefix, Key, CrawlLog], safe=True)

def curl_request(path:str):

    url = f"https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix={path}"

    curl_command = f"""
    curl '{url}' \
      -H 'Connection: keep-alive' \
      -H 'sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"' \
      -H 'Accept: */*' \
      -H 'sec-ch-ua-mobile: ?0' \
      -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36' \
      -H 'Origin: https://data.binance.vision' \
      -H 'Sec-Fetch-Site: cross-site' \
      -H 'Sec-Fetch-Mode: cors' \
      -H 'Sec-Fetch-Dest: empty' \
      -H 'Referer: https://data.binance.vision/' \
      -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
      --compressed

    """

    time.sleep(1)
    curl_execution = subprocess.run(curl_command, shell=True, stdout = subprocess.PIPE).stdout.decode("utf-8")
    CrawlLog(uuid=uuid.uuid4(),command=curl_command, response_body = curl_execution, created_at = datetime.now()).save()

    return curl_execution

def get_subdirs(html: str,selector:str="Prefix"):
    soup = Soup(html)
    return list(map(lambda node: node.getText() ,soup.select(selector)[1:]))


def crawl_process(parent_prefix: Prefix):

    print(parent_prefix)

    response = curl_request(str(parent_prefix.path))
    child_prefixes = get_subdirs(response, selector="Prefix")
    child_keys = get_subdirs(response, selector="Key")

    childcount = len(child_prefixes) + len(child_keys)
    Prefix.update({Prefix.child_count: childcount}).where(Prefix.uuid == str(parent_prefix.uuid))

    for child_prefix in child_prefixes:
        file_name = child_prefix.split("/")[-2]

        db_child_prefix_instance = None

        try:
            db_child_prefix_instance = Prefix.get(Prefix.name == file_name)
        except DoesNotExist as e:
            Prefix.create(
                parent_uuid=str(parent_prefix.uuid),
                name = file_name,
                path = child_prefix
            ).save()
            db_child_prefix_instance = Prefix.get(Prefix.name == file_name)

        crawl_process(db_child_prefix_instance)

    for child_key in child_keys:
        file_name = child_key.split("/")[-1]
        
        db_child_prefix_instance = None

        try:
            Key.get(Key.name == file_name)
        except DoesNotExist as e:
            Key.create(
            parent_uuid = str(parent_prefix.uuid),
            name = file_name,
            path = child_key
            ).save()


fetch_root_path = "data/"
try:
    root_db_instance = Prefix.get(Prefix.name == "data")
except DoesNotExist as e:
    Prefix.create("ROOT", "data", fetch_root_path).save()

root_db_instance = Prefix.get(Prefix.name == "data")
crawl_process(root_db_instance)









