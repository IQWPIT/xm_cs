import os

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection
from dm.connector.site2 import Site
from dm.connector.site import Site as Site_str


type SiteLike = Site_str | Site


def _parse_site(site: SiteLike) -> Site:
    if isinstance(site, Site):
        return site
    if isinstance(site, str):
        mapping = {
            "ml_mx": Site.ml_mx,
            "ml_br": Site.ml_br,
            "ml_cl": Site.ml_cl,
            "ml_co": Site.ml_co,
            "ml_ar": Site.ml_ar,
        }
        try:
            return mapping[site]
        except KeyError:
            raise ValueError(f"Unrecognized site string: {site!r}")
    raise TypeError(f"Invalid site type: {type(site).__name__}")


class M:
    def __init__(self, site: SiteLike):
        self.site = _parse_site(site)

        self.c_md_item_buffer = get_collection(
            f"slave_{self.site.value}", "ml_scrapy_buffer", f"{self.site.value}_md_item_buffer"
        )
        self.c_md_item_buffer_bak = get_collection(
            f"slave_{self.site.value}", "ml_scrapy_buffer", f"{self.site.value}_md_item_buffer_bak"
        )

        self.c_midea_item = get_collection("media_ml_mx", f"{self.site.value}_v3", "item")
        self.c_midea_every_day_item = get_collection("media_ml_mx", f"{self.site.value}_v3", "every_day_item")
        self.c_sku_dm_model = get_collection(f"main_ml_mx", "ml_mx", "sku_dm_model")

        ############# main #############
        self.c_every_day_item = get_collection(f"main_{self.site.value}", self.site.value, "every_day_item")
        self.c_every_day_offer = get_collection(f"main_{self.site.value}", self.site.value, "every_day_offer")
        self.c_sku = get_collection(f"main_{self.site.value}", self.site.value, "sku")
        self.c_new_every_day_sku_2 = get_collection(f"main_{self.site.value}", self.site.value, "new_every_day_sku_2")
        self.c_every_day_sku = get_collection(f"main_{self.site.value}", self.site.value, "every_day_sku")
        self.c_item_stock_queue = get_collection(f"main_{self.site.value}", self.site.value, "ml_mx_item_stock_queue")



        ############task_buffer##########################
        self.c_new_buffer_v2 = get_collection("task_buffer", "task_buffer", f"{self.site.value}_new_buffer_v2")
        self.c_item_stock_queue = get_collection(f"task_buffer", "task_buffer", f"{self.site.value}_item_stock_queue")
        self.c_item_stock = get_collection(f"task_buffer", "task_buffer", f"{self.site.value}_item_stock")


        self.c_yingshi_lanjing_rawdata = get_collection(f"yingshi", "yingshi", f"{self.site.value}_item_stock")


    def create_index(self):
        self.c_md_item_buffer.create_index("item_id")
        self.c_md_item_buffer.create_index("date")

        self.c_md_item_buffer_bak.create_index("item_id")
        self.c_md_item_buffer_bak.create_index("date")

        self.c_midea_item.create_index("item_id", unique=True)
        self.c_midea_item.create_index("init_basic_info")  # bool
        self.c_midea_item.create_index("first_write_done")  # bool

        self.c_midea_every_day_item.create_index("sl")
        self.c_midea_every_day_item.create_index("dT")
        self.c_midea_every_day_item.create_index(["sl", "dT"], unique=True)


if __name__ == "__main__":
    import sys

    site = sys.argv[1]
    M(site).create_index()
