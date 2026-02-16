import os
import re
import json
import logging
import argparse
import datetime as dt

import requests
from arcgis.gis import GIS

from collections import defaultdict
import time

# Change Log - 02-12-2026
""" - Created a GIT repository for version control and collaboration.
    - Added change log.
"""


# ==== LOGGING CONFIGURATION ====
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("qb_arc_sync")

# ==== QUICKBASE FDH TABLE REFERENCE AND SETUP ====
QB_TABLE_ID = "bts3c49e9"
LAYER_ITEM_ID = "577f024964b844b7836402bf1f84b01f"
MATCH_FIELD = "FDH_ID"

QB_FIELDS = {
    3: "QB_Record_ID",
    12: "FDH Friendly Name",
    13: "FDH_ID_QB",
    14: "FDH Status",
    23: "City ID",
    24: "OFS Date",
    254: "CX Start Date",
    262: "Project Number",
    221: "CX Vendor",
    325: "PM"
}

 # ---- MAPPING QUICKBASE FIELDS TO ARCGIS ATTRIBUTE TABLE FIELDS ----
FIELD_MAPPING = {
    "OFS Date": "OFS_Date",
    "CX Start Date": "CX_Date",
    "Project Number": "ProjectNum",
    "City ID": "City_Code",
    "CX Vendor": "Const_Ven",
    "FDH Status": "projectpha",
    "PM": "Market_Lead"
}

QB_TO_ARC_FIELDS = list(FIELD_MAPPING.keys())

# ==== QUICKBASE MDU TABLE REFERENCE AND SETUP ====
MDU_TABLE_ID = "bva6wfne6"
MDU_LAYER_ITEM_ID = "54ec733402cc40c3b95415cdf5005a8a"
MDU_MATCH_FIELD = "MDU_id"

MDU_QB_FIELDS = {
    3: "QB_Record_ID",
    6: "MDU ID",
    13: "Property Name",
    14: "Status",
    37: "Management Company",
    24: "ROE Date",
    23: "ROE?",
    38: "Base MAK"
}

# ---- MAPPING QUICKBASE FIELDS TO ARCGIS ATTRIBUTE TABLE FIELDS ----
MDU_FIELD_MAPPING = {
    "MDU ID": "MDU_id",
    "Property Name": "PropertyNam",
    "Base MAK": "BaseMAK",
    "ROE Date": "ROEDate",
    "ROE?": "ROESigned",          # expects Y/N in your old tool
    "Management Company": "MgmtCompany",
    "Status": "projectpha"
}

# ===== QUCIKBASE PERMIT TABLE REFERENCE AND SETUP =====
PERMIT_TABLE_ID = "bts3c49gt"
FDH_LAYER_ITEM_ID = "577f024964b844b7836402bf1f84b01f"
PERMIT_MATCH_FIELD = "FDH_ID"

PERMIT_QB_FIELDS = {
    3: "Record ID#", # recordid data type
    9: "Permit Type", # text / multiple choice
    11: "Permit Applied Date", # date
    13: "Permit Issued Date", # date
    16: "Status", # text / multiple choice
    30: "FDH Engineering ID", # text
    35: "Permit #" # text
}

PERMIT_FIELD_MAPPING = {

}

# -----------------------------------------------
# Global Metrics and Helpers for logs to Teams
# ------------------------------------------------
METRICS = defaultdict(int)
RUN_INFO = {"started_utc": None, "ended_utc": None, "duration_sec": None}


def metrics_start():
    RUN_INFO["started_utc"] = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    RUN_INFO["_t0"] = time.time()


def metrics_end():
    RUN_INFO["ended_utc"] = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    RUN_INFO["duration_sec"] = int(round(time.time() - RUN_INFO["_t0"]))


def emit_pad_summary():
    # one-line JSON is easiest for PAD to parse and send
    payload = {
        "status": "OK" if METRICS["errors"] == 0 else "WARN",
        "started_utc": RUN_INFO["started_utc"],
        "ended_utc": RUN_INFO["ended_utc"],
        "duration_sec": RUN_INFO["duration_sec"],

        "fdh": {
            "qb_rows": METRICS["fdh_qb_rows"],
            "arc_features": METRICS["fdh_arc_features"],
            "updates_prepared": METRICS["fdh_updates_prepared"],
            "updated_ok": METRICS["fdh_updated_ok"],
            "updated_failed": METRICS["fdh_updated_failed"],
            "skipped_no_id": METRICS["fdh_skipped_no_id"],
            "skipped_no_qb_match": METRICS["fdh_skipped_no_qb_match"],
            "skipped_no_oid": METRICS["fdh_skipped_no_oid"],
            "cx_set": METRICS["fdh_cx_set"],
            "cx_cleared": METRICS["fdh_cx_cleared"],
            "cx_skipped_unparseable": METRICS["fdh_cx_unparseable"],
            "ofs_set": METRICS["fdh_ofs_set"],
            "ofs_cleared": METRICS["fdh_ofs_cleared"],
            "ofs_skipped_unparseable": METRICS["fdh_ofs_unparseable"],
        },

        "mdu": {
            "qb_rows": METRICS["mdu_qb_rows"],
            "arc_features": METRICS["mdu_arc_features"],
            "updates_prepared": METRICS["mdu_updates_prepared"],
            "updated_ok": METRICS["mdu_updated_ok"],
            "updated_failed": METRICS["mdu_updated_failed"],
        },

        "errors": METRICS["errors"],
    }

    log.info("PAD_SUMMARY=%s", json.dumps(payload, separators=(",", ":")))
    # Optional: also print so PAD can capture stdout easily
    print("PAD_SUMMARY=" + json.dumps(payload, separators=(",", ":")))

def get_gis(args):
    """
    Server-safe authentication:
    - reads from CLI first
    - falls back to environment variables
    """
    portal_url = args.portal_url or os.getenv("ARCGIS_PORTAL_URL")
    username = args.gis_user or os.getenv("OMNI_GIS_USER")
    password = args.gis_pass or os.getenv("OMNI_GIS_PASS")

    missing = [name for name, val in {
        "ARCGIS_PORTAL_URL/--portal-url": portal_url,
        "OMNI_GIS_USER/--gis-user": username,
        "OMNI_GIS_PASS/--gis-pass": password
    }.items() if not val]

    if missing:
        raise SystemExit(f"Missing ArcGIS credentials: {', '.join(missing)}")

    gis = GIS(portal_url, username, password)

    me = gis.users.me
    log.info("ArcGIS login OK as: %s", getattr(me, "username", None))
    return gis


def qb_headers(token):
    return {
        "QB-Realm-Hostname": "omnifiber.quickbase.com",
        "Authorization": f"QB-USER-TOKEN {token}",
        "Content-Type": "application/json"
    }


def fetch_quickbase_records(token, table_id, qb_fields_dict):
    log.info("Fetching Quickbase records from %s...", table_id)

    url = "https://api.quickbase.com/v1/records/query"
    payload = {"from": table_id, "select": list(qb_fields_dict.keys())}

    r = requests.post(url, headers=qb_headers(token), data=json.dumps(payload))
    r.raise_for_status()

    records = r.json().get("data", [])
    cleaned = []

    for rec in records:
        row = {}
        for fid, label in qb_fields_dict.items():
            val = rec.get(str(fid), {}).get("value")

            # list -> scalar/string
            if isinstance(val, list):
                if len(val) == 1:
                    val = val[0]
                else:
                    val = ", ".join([str(x) for x in val if x not in (None, "")])

            # normalize QB null sentinels
            if isinstance(val, str):
                s = val.strip()
                if s in {"<Null>", "<NULL>", "<null>"} or s.lower() in {"null", "none", "n/a"}:
                    val = None
                elif s.startswith("<") and s.endswith(">"):
                    val = None
                else:
                    val = s

            row[label] = val

        cleaned.append(row)

    log.info("Quickbase records cleaned: %s", len(cleaned))
    return cleaned


UNPARSEABLE_DATE = object()


def parse_qb_date(val):
    if val is None:
        return None

    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return None
        if s.lower() in {"<null>", "null", "none"}:
            return None

        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return dt.datetime(y, mo, d)
            except Exception as e:
                # log.warning("parse_qb_date datetime ctor failed: s=%r err=%r", s, e)
                return UNPARSEABLE_DATE

        try:
            x = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return x.replace(tzinfo=None)
        except Exception:
            pass

        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y", "%Y/%m/%d"):
            try:
                return dt.datetime.strptime(s, fmt)
            except Exception:
                continue

        return UNPARSEABLE_DATE

    try:
        if isinstance(val, (int, float)):
            ts = float(val)
            if ts > 1e12:
                ts /= 1000.0
            return dt.datetime.utcfromtimestamp(ts)
    except Exception:
        pass

    return UNPARSEABLE_DATE


def update_arcgis_from_qb(layer, qb_data, features, batch_size=200):
    """
    Quickbase -> ArcGIS updates using MINIMAL payloads (OBJECTID + mapped fields only).
    This prevents unrelated fields (like MR_Ven) from being resent and causing errors.

    - features: list of ArcGIS Feature objects already queried
    - batch_size: edit_features batch size
    """

    # ---- Build QB lookup by FDH_ID ----
    qb_lookup = {}
    for r in qb_data:
        k = r.get("FDH_ID_QB")
        if k is None:
            continue
        k = str(k).strip()
        if k:
            qb_lookup[k] = r

    # ---- Arc field type map (helps sanitize values) ----
    arc_field_types = {f["name"]: f.get("type") for f in layer.properties.fields}

    def normalize_qb_value(v):
        if v is None or v == "":
            return None
        if isinstance(v, list):
            if len(v) == 0:
                return None
            if len(v) == 1:
                return v[0]
            return ", ".join([str(x) for x in v if x not in (None, "")])
        return v

    def sanitize_arc_text(v):
        v = normalize_qb_value(v)
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if s.lower() in {"<null>", "null", "none", "n/a"}:
                return None
            if s.startswith("<") and s.endswith(">"):
                return None
            return s
        return v

    def coerce_number(v):
        v = normalize_qb_value(v)
        if v is None or v == "":
            return None
        try:
            return float(v)
        except Exception:
            return None

    log.info("Arc features retrieved: %s", len(features))
    METRICS["fdh_arc_features"] = len(features)

    # ---- Prepare updates (MINIMAL payloads) ----
    updates = []
    skipped_no_id = 0
    skipped_no_qb_match = 0
    skipped_no_oid = 0

    for feat in features:
        rec_id = feat.attributes.get(MATCH_FIELD)
        if not rec_id:
            skipped_no_id += 1
            METRICS["fdh_skipped_no_id"] += 1
            continue

        rec_id = str(rec_id).strip()
        record = qb_lookup.get(rec_id)
        if not record:
            skipped_no_qb_match += 1
            METRICS["fdh_skipped_no_qb_match"] += 1

            continue

        oid = feat.attributes.get("OBJECTID")
        if oid is None:
            skipped_no_oid += 1
            METRICS["fdh_skipped_no_oid"] += 1
            continue

        out_attrs = {"OBJECTID": oid, MATCH_FIELD: rec_id}  # include MATCH_FIELD for logging/debug

        for qb_label in QB_TO_ARC_FIELDS:
            arc_field = FIELD_MAPPING[qb_label]
            raw = sanitize_arc_text(record.get(qb_label))

            # Dates
            if arc_field in ("CX_Date", "OFS_Date"):
                raw_date = record.get(qb_label)
                parsed_dt = parse_qb_date(raw_date)

                if parsed_dt is None:
                    arc_before = feat.attributes.get(arc_field)

                    # Only clear if Arc currently has something
                    if arc_before not in (None, "", 0):
                        out_attrs[arc_field] = None

                        if arc_field == "CX_Date":
                            METRICS["fdh_cx_cleared"] += 1
                        else:
                            METRICS["fdh_ofs_cleared"] += 1

                        log.info(
                            "DATE CLEAR | FDH_ID=%s field=%s qb_raw=%r arc_before=%r",
                            rec_id, arc_field, raw_date, arc_before
                        )
                    else:
                        # Arc already empty -> no-op (optional metric)
                        if arc_field == "CX_Date":
                            METRICS["fdh_cx_clear_noop_already_null"] += 1
                        else:
                            METRICS["fdh_ofs_clear_noop_already_null"] += 1


                elif parsed_dt is UNPARSEABLE_DATE:
                    if arc_field == "CX_Date":
                        METRICS["fdh_cx_unparseable"] += 1
                    else:
                        METRICS["fdh_ofs_unparseable"] += 1
                    # leave Arc unchanged: do not set field in out_attrs

                else:
                    out_attrs[arc_field] = parsed_dt
                    if arc_field == "CX_Date":
                        METRICS["fdh_cx_set"] += 1
                    else:
                        METRICS["fdh_ofs_set"] += 1

                continue

            arc_type = arc_field_types.get(arc_field)

            # Integers
            if arc_type in ("esriFieldTypeInteger", "esriFieldTypeSmallInteger"):
                num = coerce_number(raw)
                if num is None:
                    continue
                out_attrs[arc_field] = int(round(num))
                continue

            # Doubles/Floats
            if arc_type in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
                num = coerce_number(raw)
                if num is None:
                    continue
                out_attrs[arc_field] = float(num)
                continue

            # Strings/other: only set if meaningful
            if raw is None or raw == "":
                continue
            out_attrs[arc_field] = raw

        # if "CX_Date" in out_attrs:
        #     log.info(
        #         "ARC UPDATE PAYLOAD | FDH_ID=%s CX_Date=%r",
        #          rec_id, out_attrs["CX_Date"]
        #     )

        # If we only have OBJECTID + MATCH_FIELD, nothing to update
        if len(out_attrs) > 2:
            updates.append({"attributes": out_attrs})
        else:
            METRICS["fdh_no_changes"] += 1

    METRICS["fdh_updates_prepared"] = len(updates)

    log.info(
        "Prepared %s updates | skipped missing %s=%s | skipped no QB match=%s | skipped no OBJECTID=%s",
        len(updates), MATCH_FIELD, skipped_no_id, skipped_no_qb_match, skipped_no_oid
    )

    if not updates:
        log.info("No updates to apply.")
        return

    # ---- Apply updates resiliently ----
    log.info("Applying %s updates (batch_size=%s)", len(updates), batch_size)

    for i in range(0, len(updates), batch_size):
        chunk = updates[i:i + batch_size]
        try:
            resp = layer.edit_features(updates=chunk) or {}
            results = resp.get("updateResults", []) or []
            ok = sum(1 for r in results if r.get("success"))
            bad = len(results) - ok

            METRICS["fdh_updated_ok"] += ok
            METRICS["fdh_updated_failed"] += bad
            if bad:
                METRICS["errors"] += bad

            log.info("Updated %s-%s | ok=%s failed=%s", i + 1, i + len(chunk), ok, bad)

        except Exception as e:
            METRICS["fdh_updated_failed"] += len(chunk)
            METRICS["errors"] += len(chunk)

            log.error("Batch %s-%s failed: %s", i + 1, i + len(chunk), e)

            # Fallback: one-by-one to identify offenders
            for upd in chunk:
                try:
                    layer.edit_features(updates=[upd])
                except Exception as e2:
                    attrs = upd.get("attributes", {})
                    oid = attrs.get("OBJECTID")
                    fdh = attrs.get(MATCH_FIELD)

                    # log only the mapped fields we attempted to send
                    outbound = {FIELD_MAPPING[qb]: attrs.get(FIELD_MAPPING[qb]) for qb in QB_TO_ARC_FIELDS}

                    METRICS["fdh_updated_failed"] += 1
                    METRICS["errors"] += 1

                    log.error(
                        "FAILED OBJECTID=%s %s=%s error=%s outbound=%r",
                        oid, MATCH_FIELD, fdh, e2, outbound
                    )
                    continue


def chunked_arc_query(layer, match_field, ids, chunk_size=250):
    features = []
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i+chunk_size]
        quoted = ",".join([f"'{x}'" for x in chunk])
        where = f"{match_field} IN ({quoted})"
        res = layer.query(where=where, return_geometry=False)
        feats = getattr(res, "features", [])
        features.extend(feats)
        log.info("Queried chunk %s-%s -> %s features", i+1, min(i+chunk_size, len(ids)), len(feats))
    return features


def run_fdh_sync(gis, qb_token, batch_size=200, dry_run=False, fdh_ids=None):
    log.info("==== FDH Sync starting ====")

    qb_rows = fetch_quickbase_records(qb_token, QB_TABLE_ID, QB_FIELDS)
    METRICS["fdh_qb_rows"] = len(qb_rows)

    item = gis.content.get(LAYER_ITEM_ID)
    if not item:
        raise SystemExit(f"Could not find FDH ArcGIS item {LAYER_ITEM_ID}")
    layer = item.layers[0]

    if fdh_ids:
        # debug override
        quoted = ",".join([f"'{x}'" for x in fdh_ids])
        where = f"{MATCH_FIELD} IN ({quoted})"
        features = layer.query(where=where, return_geometry=False).features
        log.info("FDH override: syncing %s FDHs from --fdh-ids", len(features))
    else:
        qb_ids = sorted(set(str(r.get("FDH_ID_QB")).strip() for r in qb_rows if r.get("FDH_ID_QB")))
        features = chunked_arc_query(layer, MATCH_FIELD, qb_ids, chunk_size=250)

    METRICS["fdh_arc_features"] = len(features)

    if dry_run:
        log.info("FDH dry-run: would process %s features", len(features))
        return

    update_arcgis_from_qb(layer, qb_rows, features, batch_size=batch_size)
    log.info("==== FDH Sync finished ====")


def run_mdu_sync(gis, qb_token, batch_size=200, dry_run=False):
    log.info("==== MDU Sync starting ====")

    qb_rows = fetch_quickbase_records(qb_token, MDU_TABLE_ID, MDU_QB_FIELDS)
    METRICS["mdu_qb_rows"] = len(qb_rows)

    item = gis.content.get(MDU_LAYER_ITEM_ID)
    if not item:
        raise SystemExit(f"Could not find MDU ArcGIS item {MDU_LAYER_ITEM_ID}")
    layer = item.layers[0]

    qb_ids = sorted(set(str(r.get("MDU ID")).strip() for r in qb_rows if r.get("MDU ID")))
    features = chunked_arc_query(layer, MDU_MATCH_FIELD, qb_ids, chunk_size=250)

    METRICS["mdu_arc_features"] = len(features)

    if dry_run:
        log.info("MDU dry-run: would process %s features", len(features))
        return

    update_mdu_arcgis_from_qb(layer, qb_rows, features, batch_size=batch_size)
    log.info("==== MDU Sync finished ====")


def qb_checkbox_value(val):
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "y", "checked")


def update_mdu_arcgis_from_qb(layer, qb_rows, features, batch_size=200):
    """
    Quickbase -> ArcGIS (MDU) updates using MINIMAL payloads.
    - Only sends OBJECTID + mapped fields.
    - Logs applyEdits success/fail counts.
    """

    # ---- QB lookup by MDU ID ----
    qb_lookup = {}
    for r in qb_rows:
        k = r.get("MDU ID")
        if k is None:
            continue
        k = str(k).strip()
        if k:
            qb_lookup[k] = r

    # ---- Arc field type map (helps sanitize values) ----
    arc_field_types = {f["name"]: f.get("type") for f in layer.properties.fields}

    def normalize_qb_value(v):
        """Convert QB shapes into simple scalars suitable for ArcGIS."""
        if v is None or v == "":
            return None
        if isinstance(v, list):
            if len(v) == 0:
                return None
            if len(v) == 1:
                return v[0]
            return ", ".join([str(x) for x in v if x not in (None, "")])
        return v

    def sanitize_arc_text(v):
        """Remove QB null sentinels and angle-bracket tokens like <Null>."""
        v = normalize_qb_value(v)
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if s.lower() in {"<null>", "null", "none", "n/a"}:
                return None
            if s.startswith("<") and s.endswith(">"):
                return None
            return s
        return v

    def coerce_number(v):
        """Try to coerce QB value into float; return None if not possible."""
        v = normalize_qb_value(v)
        if v is None or v == "":
            return None
        try:
            return float(v)
        except Exception:
            return None

    def apply_updates(layer, updates, label="MDU"):
        resp = layer.edit_features(updates=updates) or {}
        results = resp.get("updateResults", []) or []

        ok = sum(1 for r in results if r.get("success"))
        failed_results = [r for r in results if not r.get("success")]
        bad = len(failed_results)

        METRICS["mdu_updated_ok"] += ok
        METRICS["mdu_updated_failed"] += bad
        if bad:
            METRICS["errors"] += bad

        log.info("%s applyEdits: %s ok, %s failed", label, ok, bad)

        for r in failed_results[:5]:
            log.error("%s applyEdits failure: oid=%s error=%s", label, r.get("objectId"), r.get("error"))

        return resp

    log.info("Arc MDU features retrieved: %s", len(features))

    # ---- Build minimal update payloads ----
    updates = []
    skipped_no_oid = 0
    skipped_no_match = 0

    for feat in features:
        attrs = feat.attributes or {}
        oid = attrs.get("OBJECTID")
        if oid is None:
            skipped_no_oid += 1
            continue

        mdu_id = attrs.get(MDU_MATCH_FIELD)
        if not mdu_id:
            skipped_no_match += 1
            continue
        mdu_id = str(mdu_id).strip()

        rec = qb_lookup.get(mdu_id)
        if not rec:
            skipped_no_match += 1
            continue

        out_attrs = {"OBJECTID": oid}

        for qb_label, arc_field in MDU_FIELD_MAPPING.items():
            raw = sanitize_arc_text(rec.get(qb_label))

            # ROE checkbox -> Y/N
            if qb_label == "ROE?":
                out_attrs[arc_field] = "Y" if qb_checkbox_value(raw) else "N"
                continue

            # Date
            if qb_label == "ROE Date":
                parsed_dt = parse_qb_date(raw)
                if parsed_dt is None:
                    out_attrs[arc_field] = None
                elif parsed_dt is UNPARSEABLE_DATE:
                    METRICS["errors"] += 1
                    log.warning("MDU DATE SKIP (unparseable) | MDU_ID=%s raw=%r", mdu_id, raw)
                    # leave unchanged (don’t include field)
                else:
                    out_attrs[arc_field] = parsed_dt
                continue

            # Numeric coercion based on Arc field type
            arc_type = arc_field_types.get(arc_field)

            if arc_type in ("esriFieldTypeInteger", "esriFieldTypeSmallInteger"):
                num = coerce_number(raw)
                if num is None:
                    continue
                out_attrs[arc_field] = int(round(num))
                continue

            if arc_type in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
                num = coerce_number(raw)
                if num is None:
                    continue
                out_attrs[arc_field] = float(num)
                continue

            # Strings and other types
            if raw is not None and raw != "":
                out_attrs[arc_field] = raw

        # Only send if we have something besides OBJECTID
        if len(out_attrs) > 1:
            updates.append({"attributes": out_attrs})
        else:
            METRICS["mdu_no_changes"] += 1

    METRICS["mdu_updates_prepared"] = len(updates)

    log.info("MDU updates prepared: %s | skipped missing OBJECTID=%s | skipped no match=%s",
             len(updates), skipped_no_oid, skipped_no_match)

    if not updates:
        log.info("No MDU updates to apply.")
        return

    # ---- Apply edits in batches, fallback to single-item to find offenders ----
    for i in range(0, len(updates), batch_size):
        chunk = updates[i:i + batch_size]
        try:
            apply_updates(layer, chunk, label=f"MDU {i+1}-{i+len(chunk)}")
        except Exception as e:
            log.error("MDU batch %s-%s threw exception: %s", i+1, i+len(chunk), e)

            # one-by-one fallback
            for u in chunk:
                try:
                    apply_updates(layer, [u], label="MDU single")
                except Exception as e2:
                    oid = u.get("attributes", {}).get("OBJECTID")
                    log.error("MDU FAILED OBJECTID=%s error=%s outbound=%r", oid, e2, u.get("attributes"))


def main():
    metrics_start()
    parser = argparse.ArgumentParser()

    # Debug override (optional)
    parser.add_argument("--fdh-ids", nargs="*", help="Optional: limit FDH sync to these FDH_IDs")

    # ArcGIS auth (server-safe)
    parser.add_argument("--portal-url", help="ArcGIS Portal URL (or env ARCGIS_PORTAL_URL)")
    parser.add_argument("--gis-user", help="ArcGIS username (or env OMNI_GIS_USER)")
    parser.add_argument("--gis-pass", help="ArcGIS password (or env OMNI_GIS_PASS)")

    # Quickbase Auth
    parser.add_argument("--qb-token")

    # Tuning / safety
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    qb_token = args.qb_token or os.getenv("QB_TOKEN")
    if not qb_token:
        raise SystemExit("QB token missing (set QB_TOKEN or pass --qb-token).")

    gis = get_gis(args)

    # 1) FDH sync
    try:
        run_fdh_sync(gis, qb_token, batch_size=args.batch_size, dry_run=args.dry_run, fdh_ids=args.fdh_ids)
    except Exception:
        log.exception("FDH sync crashed; continuing to MDU sync.")

    # 2) MDU sync
    try:
        run_mdu_sync(gis, qb_token, batch_size=args.batch_size, dry_run=args.dry_run)
    except Exception:
        log.exception("MDU sync crashed.")
        raise

    metrics_end()
    emit_pad_summary()

    # print("PAD_SUMMARY=" + json.dumps(payload, separators=(",", ":")))

    log.info("✅ All syncs complete")


if __name__ == "__main__":
    main()
