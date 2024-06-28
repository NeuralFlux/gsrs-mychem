import gzip
import json
import os
from datetime import datetime, timezone
from typing import Any, List

from biothings import config
from biothings.utils.dataload import dict_convert, dict_sweep, dict_traverse

logging = config.logger

process_key = lambda key: key.replace(" ", "_").lower()
recognized_code_systems = ["cas", "chebi", "chembl", "drugbank", "fdaunii", "mesh", "pubchem"]
date_cols = ("documentDate", "deprecatedDate")


def timestamp_to_date(k: str, v: Any):
    """check if a key-value pair needs date formatting and do so"""
    if k in date_cols:
        date_obj = datetime.fromtimestamp(int(v), tz=timezone.utc)
        v = date_obj.strftime("%Y-%m-%d")
    return k, v


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
            dict_traverse(record, timestamp_to_date)  # parse dates in `date_cols` only
            record = dict_sweep(record, vals=["", None], remove_invalid_list=True)

            _id = f"gsrs.uuid:{record['uuid']}"
            if "approvalid" in record.keys():
                _id = f"unii:{record['approvalid']}"
            yield {"_id": _id, "gsrs": record}
