import gzip
import json
import os

from biothings import config
from biothings.utils.dataload import dict_convert, dict_sweep

# from keylookup import MyChemKeyLookup

logging = config.logger

process_key = lambda key: key.replace(" ", "_").lower()
# lookup = MyChemKeyLookup([("smiles", "pubchem.smiles")])


def load_substances(data_folder: str):
    file_name = os.path.join(data_folder, "dump-public-2023-12-14.gsrs")

    with open(file_name, "rb") as compressed_fd:
        with gzip.GzipFile(fileobj=compressed_fd) as fd:
            for line in fd:
                record = json.loads(line.strip())
                record = dict_convert(record, keyfn=process_key)
                # NOTE: do we need sweep?
                record["_id"] = record["uuid"]

                yield record
