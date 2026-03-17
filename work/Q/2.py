import datetime
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from io import BytesIO

from elasticsearch.helpers import bulk
from elasticsearch_dsl import Search
from loguru import logger
from PIL import Image
from prefect import flow

from dm.connector.mongo.manager2 import get_database_with_alias_name
from dm.connector.elasticsearch.manager import get_es_with_alias_name
from dm.connector.minio.manager import get_minio_client_with_alias_name
from dm.connector.minio.minio_opt import MinIOOPT
from dm.connector.site import Site
from dm.utils.dt import get_date

from feature_extractor_resnet50 import FeatureExtractorResnet

TEST = os.environ.get("TEST") == "1"


class Solutions:
    def __init__(self, site: Site) -> None:
        self.site = site

        main_db = get_database_with_alias_name(
            f"main_{self.site}", check_connection=False
        )
        self.sku_table = main_db.get_collection(name="sku")

        self.ml_db = get_database_with_alias_name("ml")
        # monday = get_date(rule="W", when="start")
        # logger.info(f"{monday=}")
        # monday_str = monday.strftime("%Y%m%d")
        # self.buffer_table_name = f"sku30d_{self.site}_{monday_str}"
        # self.exists_buffer_table_flag = self.exists_buffer_table()
        self.buffer_table_name = "sku30d_ml_ar_20260223"
        self.exists_buffer_table_flag = self.exists_buffer_table()
        logger.info(f"buffer_table_name={self.buffer_table_name}")

        self.minio_client, self.bucket_name = get_minio_client_with_alias_name(
            f"pic_search_{self.site}"
        )
        self.minio_opt = MinIOOPT(client=self.minio_client)
        self.minio_opt.bucket_name = self.bucket_name

        self.es, self.index_name = get_es_with_alias_name(f"pic_search_{self.site}")
        self.index_name = f"{self.index_name}_2"
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name)

        self.fe = FeatureExtractorResnet()

        self.fields = [
            "pic_url",
            "sku_id",
            "stock_type",
            "outstock",
            "datetime",
            "total_order",
            "category_id",
            "active_price",
        ]

    def exists_buffer_table(self):
        return bool(
            self.ml_db.list_collection_names(filter={"name": self.buffer_table_name})
        )

    @property
    def buffer_table(self):
        if self.exists_buffer_table_flag:
            _buffer_table = self.ml_db.get_collection(name=self.buffer_table_name)
            _buffer_table.create_index("feature_extract_s3_to_es")
            _buffer_table.create_index("feature_extract_s3_to_es.status")
            return _buffer_table
        return None

    def get_index_actions(self, object_name: str, sku_item: dict, image_bytes: bytes):
        image = Image.open(image_bytes).convert("RGB")
        feature = self.fe.extract(image=image)

        source = {
            "sku_id": sku_item["sku_id"],
            "box": None,
            "object_name": object_name,
            "feature": feature,
            "stock_type": sku_item["stock_type"],
            "outstock": sku_item["outstock"],
            "total_order": sku_item["total_order"],
            "cat_id": sku_item["category_id"],
            "active_price": sku_item["active_price"],
            "datetime": datetime.datetime.fromtimestamp(
                sku_item["datetime"] // 1000
            ),
        }
        return [{"_index": self.index_name, "_source": source}]

    def get_update_actions(self, hits, sku_item: dict):
        actions = []
        for hit in hits:
            source = {
                "stock_type": sku_item["stock_type"],
                "outstock": sku_item["outstock"],
                "total_order": sku_item["total_order"],
                "cat_id": sku_item["category_id"],
                "active_price": sku_item["active_price"],
                "datetime": datetime.datetime.fromtimestamp(
                    sku_item["datetime"] // 1000
                ),
            }
            actions.append(
                {
                    "_op_type": "update",
                    "_index": self.index_name,
                    "_id": hit.meta.id,
                    "doc": source,
                }
            )
        return actions

    def _update_status(self, sku_id: str, status: str, message: str):
        self.buffer_table.update_many(
            {"sku_id": sku_id},
            {
                "$set": {
                    "feature_extract_s3_to_es": {
                        "status": status,
                        "message": message,
                        "dT": datetime.datetime.now(),
                    }
                }
            },
        )

    def _handle_one_sku_id(self, sku_id: str):
        logger.debug(f"{sku_id=}")
        sku_item: dict | None = self.sku_table.find_one(
            {"sku_id": sku_id}, {k: 1 for k in self.fields}
        )

        if sku_item is None:
            self._update_status(sku_id, "success", "sku_id not found in sku_table")
            return

        if not all(i in sku_item for i in self.fields):
            self._update_status(sku_id, "success", "sku_item haven't all fields")
            return

        s = Search(using=self.es, index=self.index_name).params(size=1)
        s = s.filter("term", **{"sku_id.keyword": sku_id})

        _fields = list(self.fields)
        with suppress(ValueError):
            _fields.remove("pic_url")
            _fields.remove("datetime")
        _fields.append("object_name")
        s = s.source(includes=_fields)

        hits = s.execute().hits
        existed_es_data = hits[0].to_dict() if hits else None

        pic_url: str = sku_item["pic_url"]
        object_name = f"{sku_id}/{pic_url.split('/')[-1]}"

        if not existed_es_data or object_name != existed_es_data["object_name"]:
            if not self.minio_opt.object_exists(object_name=object_name):
                self._update_status(sku_id, "failed", "image not stored in minio")
                return

            image_content = self.minio_opt.get_object(object_name=object_name)
            if not image_content:
                self._update_status(sku_id, "failed", "image download failed")
                return

            actions = self.get_index_actions(
                object_name=object_name,
                sku_item=sku_item,
                image_bytes=BytesIO(image_content),
            )
        else:
            actions = self.get_update_actions(hits=hits, sku_item=sku_item)

        bulk(client=self.es, actions=actions)
        self._update_status(sku_id, "success", "everything is look like OK")

    def handle_one_sku_id(self, sku_id: str):
        try:
            self._handle_one_sku_id(sku_id=sku_id)
        except Exception as e:
            self._update_status(sku_id, "failed", repr(e))

    def run(self):
        if self.buffer_table is None:
            return

        n = 0
        while True:
            exit_flag = True
            with ThreadPoolExecutor(max_workers=2) as executor:
                for buffer_item in (
                    self.buffer_table.find(
                        {"feature_extract_s3_to_es": {"$exists": False}}
                    )
                    .batch_size(20)
                    .limit(10 if TEST else 1000)
                ):
                    exit_flag = False
                    n += 1
                    sku_id = buffer_item.get("sku_id")
                    executor.submit(self.handle_one_sku_id, sku_id=sku_id)

            logger.info(f"{n=}")
            if exit_flag:
                break
            if TEST:
                break

        logger.info(f"handle buffer table done, {n=}")


@flow(log_prints=True)
def feature_extract_s3_to_es_flow(site: Site):
    log_fmt = "feature_extract_s3_to_es_flow - {time} - {level} - {message}"
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    logger.remove()
    logger.add(sink=sys.stdout, level=log_level, format=log_fmt)

    print("feature_extract_s3_to_es_flow start.")
    ss = Solutions(site=site)
    if not ss.exists_buffer_table_flag:
        print(f"{ss.buffer_table_name} not exists.")
        logger.info(f"{ss.buffer_table_name} not exists.")
        return
    ss.run()
    print("feature_extract_s3_to_es_flow done.")


if __name__ == "__main__":
    feature_extract_s3_to_es_flow(site="ml_ar")