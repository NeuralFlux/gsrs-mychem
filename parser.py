import gzip
import itertools
import json
import os

from biothings import config
from biothings.utils.dataload import dict_convert, dict_sweep

logging = config.logger

process_key = lambda key: key.replace(" ", "_").lower()


def load_substances(data_folder: str):
    file_name = os.path.join(data_folder, "dump-public-2023-12-14.gsrs")

    with gzip.GzipFile(file_name) as fd:
        for raw_line in itertools.islice(fd, 10000):
            record = json.loads(raw_line.decode("utf-8").strip())
            record = dict_convert(record, keyfn=process_key)
            record = dict_sweep(record, vals=["", None], remove_invalid_list=True)
            record["_id"] = record["uuid"]

            yield record
