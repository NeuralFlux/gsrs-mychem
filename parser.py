import gzip
import itertools
import json
import os
from datetime import datetime, timezone
from typing import List, Tuple

from biothings import config
from biothings.utils.dataload import dict_convert, dict_sweep

logging = config.logger

process_key = lambda key: key.replace(" ", "_").lower()
recognized_code_systems = ["cas", "chebi", "chembl", "drugbank", "fdaunii", "mesh", "pubchem"]


def timestamp_to_date(d: dict, keys: Tuple[str]):
    """"""
    for key in keys:
        if key in d.keys():
            date_obj = datetime.fromtimestamp(int(d[key]), tz=timezone.utc)
            d.update({key: date_obj.strftime("%Y-%m-%d")})
    return d


def parse_xrefs(codes: List[str]):
    xrefs = {}
    for code in codes:
        code_system = code["codeSystem"].replace(" ", "").lower()
        if code_system in recognized_code_systems:
            if code_system == "fdaunii":
                code_system = "unii"

            if code_system == "pubchem":
                if "url" not in code.keys():  # cannot determine cid or sid
                    continue
                if "compound" in code["url"]:
                    code_system += "_cid"
                elif "substance" in code["url"]:
                    code_system += "_sid"

            xrefs[code_system] = code["code"]
    return xrefs


def load_substances(data_folder: str):
    file_name = os.path.join(data_folder, "dump-public-2023-12-14.gsrs")

    with gzip.GzipFile(file_name) as fd:
        for raw_line in fd:
            record = json.loads(raw_line.decode("utf-8").strip())
            record = dict_convert(record, keyfn=process_key)
            if "codes" in record.keys():
                record["xrefs"] = parse_xrefs(record["codes"])
            # record = timestamp_to_date(record, ("documentDate", "deprecatedDate"))
            record = dict_sweep(record, vals=["", None], remove_invalid_list=True)

            _id = f"gsrs.uuid:{record['uuid']}"
            if "approvalid" in record.keys():
                _id = f"unii:{record['approvalid']}"
            yield {"_id": _id, "gsrs": record}
