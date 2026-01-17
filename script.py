#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iceland Phase 2 (PUBLIC-SAFE): product enrichment -> normalized CSV (TEST IDS ONLY).

What this script does
- Reads a Phase 1 CSV (product_code + category path metadata + product_url)
- Calls Iceland's public JSON endpoint (https://www.iceland.co.uk/algolia/products) in batches.
- Normalizes fields into a wide CSV schema (price, nutrition, pack size, flags, category levels, promos, etc.).
- Runs ONLY against the TEST_PRODUCT_IDS list (for demo purposes)
- Writes output CSV + logs into repo-local folders (no hardcoded /Users/... paths).

Public repo safety
- No API keys.
- No user-specific absolute paths.
- No large-scale full-catalog mode enabled by default.
- You should still respect the retailer’s terms, robots policies, and rate limits.

Repo layout (recommended)
  your-repo/
    scripts/
      iceland_phase2_public.py   <-- this file
    data/
      input/
        p1_iceland.csv           <-- Phase 1 output (or a small sample)
      output/
        (generated)
    logs/
      (generated)

Usage
  python3 scripts/iceland_phase2_public.py \
    --input data/input/p1_iceland.csv \
    --outdir data/output

If you omit args, defaults are used.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------- TEST IDS (as requested) -----------------
TEST_PRODUCT_IDS = [
    "4277","68905","71507","19701","62943","104615","65014","79718","103913","92502","14120","41508",
    "91437","7977","88147","72929","10706","7057","7056","78241","53629","40085","75776","86177",
    "65783","18004","94100","101520","101519","35824","101625","101615","101499","55258","62054","68684"
]

# ----------------- HTTP -----------------
ALGOLIA_PRODUCTS_URL = "https://www.iceland.co.uk/algolia/products"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36"
)

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://www.iceland.co.uk",
    "referer": "https://www.iceland.co.uk/",
    "user-agent": UA,
    "accept-language": "en-GB,en;q=0.9",
}

# ----------------- Defaults -----------------
DEFAULT_CONCURRENCY = 6
DEFAULT_BATCH_WRITE = 500
DEFAULT_CHUNK_SIZE = 120
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_ATTEMPTS = 3

# ----------------- Helpers (text & parsing) -----------------
_HTML_TAGS = re.compile(r"<[^>]+>")
_NUM_RE = re.compile(r"(\d+(?:\.\d+)?)")

# Money patterns
_POUNDS_RE = re.compile(r"£\s*([0-9]+(?:\.[0-9]{1,2})?)", re.I)
_PENCE_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*p\b", re.I)

# weight/volume patterns (includes centilitre / cl)
_WEIGHT_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*("
    r"g|grams?|kg|kilograms?|"
    r"ml|millilitres?|milliliters?|"
    r"l|litres?|liters?|"
    r"cl|centilitres?|centiliters?"
    r")\b",
    re.I,
)

# multi x patterns
_MULTI_TIGHT = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\b", re.I)
_MULTI_LOOSE = re.compile(r"(\d+)[^\d]{0,20}[x×]\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\b", re.I)


def _unit_to_base(u: str) -> Optional[str]:
    if not u:
        return None
    ul = u.strip().lower()
    if ul in ("g", "gram", "grams"):
        return "g"
    if ul in ("kg", "kilogram", "kilograms"):
        return "kg"
    if ul in ("ml", "millilitre", "millilitres", "milliliter", "milliliters"):
        return "ml"
    if ul in ("l", "litre", "litres", "liter", "liters"):
        return "l"
    if ul in ("cl", "centilitre", "centilitres", "centiliter", "centiliters"):
        return "cl"
    return None


def as_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (int, float)):
        return str(x)
    if isinstance(x, list):
        return " ".join(as_text(v) for v in x if v is not None)
    if isinstance(x, dict):
        parts = []
        for k, v in x.items():
            sv = as_text(v).strip()
            if sv:
                parts.append(f"{k}: {sv}")
        return " ".join(parts)
    return str(x)


def strip_html(s: str) -> str:
    return _HTML_TAGS.sub("", unescape(s or "")).strip()


def extract_bold_ingredients(ingredients_html: str) -> List[str]:
    if not ingredients_html:
        return []
    out: List[str] = []
    for m in re.finditer(
        r"<\s*(?:strong|b)\s*[^>]*>(.*?)<\s*/\s*(?:strong|b)\s*>",
        ingredients_html,
        re.I | re.S,
    ):
        t = strip_html(m.group(1))
        t = re.sub(r"\s+", " ", t).strip(",;: ").strip()
        if t:
            out.append(t)
    seen = set()
    uniq: List[str] = []
    for x in out:
        xl = x.lower()
        if xl in seen:
            continue
        seen.add(xl)
        uniq.append(x)
    return uniq


# ----------------- Packet quantity detection -----------------
_NOUNS = [
    r"eggs?",
    r"tablets?",
    r"tea\s*bags?",
    r"liners?",
    r"rashers?",
    r"rolls?",
    r"past(?:y|ies)",
    r"muffins?",
    r"bars?",
    r"wipes?",
    r"jambons?",
    r"cases?",
    r"churros?",
    r"slices?",
    r"subs?",
    r"servings?",
    r"portions?",
    r"quiches?",
    r"fillets?",
    r"fish\s*fingers?",
    r"fingers?",
    r"fries?",
    r"nuggets?",
    r"poppadoms?",
    # Bakery / countables:
    r"bagels?",
    r"cups?",
    r"pies?",
    r"rounds?",
    r"teacakes?|tea\s*cakes?",
    r"fancies",
    r"tarts?",
    r"cakes?",
    r"butteries?",
    r"danish(?:es)?",
    r"squares?",
    r"pack(?:s)?|pk",
]
_NOUNS_ALT = r"(?:%s)" % "|".join(_NOUNS)

_QTY_WITH_ADJ = re.compile(
    rf"(^|\b)(\d{{1,4}})\s+"
    rf"(?:\([^\)]*\)\s+)?"
    rf"(?:[A-Za-z&\'’\-\.]+(?:\s+|/|&|,|\.)+){{0,7}}?"
    rf"{_NOUNS_ALT}\b",
    re.I,
)

_PACK_BEFORE_NOUN = re.compile(
    rf"(^|\b)(\d{{1,4}})\s+(?:pack|packs|pk|box)\b.*?\b{_NOUNS_ALT}\b",
    re.I,
)

_PACK_ALONE = re.compile(r"(^|\b)(\d{1,4})\s*(?:x\s*)?(?:pack|packs|pk)\b", re.I)


def detect_packet_quantity(title: str) -> Optional[int]:
    """
    Detect packet quantity from titles, avoiding cases like "Omega 3" unless a target noun follows.
    Returns None unless confidently > 1.
    """
    if not title:
        return None
    s = title.strip()

    m = _MULTI_TIGHT.search(s) or _MULTI_LOOSE.search(s)
    if m:
        try:
            q = int(m.group(1))
            return q if q > 1 else None
        except Exception:
            pass

    m2 = _QTY_WITH_ADJ.search(s)
    if m2:
        try:
            qty = int(m2.group(2))
            return qty if qty > 1 else None
        except Exception:
            pass

    m3 = _PACK_BEFORE_NOUN.search(s)
    if m3:
        try:
            qty = int(m3.group(2))
            return qty if qty > 1 else None
        except Exception:
            pass

    m4 = _PACK_ALONE.search(s)
    if m4:
        try:
            qty = int(m4.group(2))
            return qty if qty > 1 else None
        except Exception:
            pass

    return None


# ----------------- Weight parsing -----------------
def parse_weight_from_title(title: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Returns (weight, base_unit) where base_unit is 'g' or 'ml' when possible.
    Supports cl in title.
    """
    if not title:
        return None, None
    s = title.strip()

    m = _MULTI_TIGHT.search(s) or _MULTI_LOOSE.search(s)
    if m:
        qty = int(m.group(1))
        unit_w = float(m.group(2))
        unit = _unit_to_base(m.group(3))
        if unit in ("kg", "l", "cl"):
            if unit == "kg":
                unit_w *= 1000.0
                unit = "g"
            elif unit == "l":
                unit_w *= 1000.0
                unit = "ml"
            elif unit == "cl":
                unit_w *= 10.0
                unit = "ml"
        if unit in ("g", "ml"):
            return qty * unit_w, unit

    m2 = _WEIGHT_RE.search(s)
    if m2:
        w = float(m2.group(1))
        unit = _unit_to_base(m2.group(2))
        if unit in ("kg", "l", "cl"):
            if unit == "kg":
                w *= 1000.0
                unit = "g"
            elif unit == "l":
                w *= 1000.0
                unit = "ml"
            elif unit == "cl":
                w *= 10.0
                unit = "ml"
        if unit in ("g", "ml"):
            return w, unit

    return None, None


# ----------------- PerUnit parsing -----------------
_PERUNIT_QTY = re.compile(
    r"\bper\s*(\d+(?:\.\d+)?)\s*("
    r"centilitres?|centiliters?|cl|"
    r"millilitres?|milliliters?|ml|"
    r"litres?|liters?|l|"
    r"grams?|g|kilograms?|kg|"
    r"units?|each|item|piece|egg"
    r")\b",
    re.I,
)


def parse_perunit_quantity(per_unit: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    if not per_unit:
        return None, None
    t = per_unit.strip().lower()
    m = _PERUNIT_QTY.search(t)
    if not m:
        return None, None
    qty = float(m.group(1))
    unit = _unit_to_base(m.group(2)) or m.group(2).lower()
    if unit in ("kg", "l", "cl", "ml", "g"):
        return qty, unit
    if unit in ("unit", "units", "each", "item", "piece", "egg"):
        return qty, "each"
    return None, None


def _extract_money_from_str(s: str) -> List[float]:
    out: List[float] = []
    for m in _POUNDS_RE.finditer(s):
        try:
            out.append(float(m.group(1)))
        except Exception:
            pass
    for m in _PENCE_RE.finditer(s):
        try:
            p = float(m.group(1))
            out.append(round(p / 100.0, 4))
        except Exception:
            pass
    return out


def extract_money_values_from_dict(d: Dict[str, Any]) -> List[float]:
    vals: List[float] = []
    for v in d.values():
        if isinstance(v, dict):
            vals.extend(extract_money_values_from_dict(v))
        elif isinstance(v, list):
            for x in v:
                if isinstance(x, dict):
                    vals.extend(extract_money_values_from_dict(x))
                elif isinstance(x, str):
                    vals.extend(_extract_money_from_str(x))
        elif isinstance(v, str):
            vals.extend(_extract_money_from_str(v))

    seen = set()
    out: List[float] = []
    for n in vals:
        if n is None or n <= 0:
            continue
        if abs(n) not in seen:
            seen.add(abs(n))
            out.append(n)
    return out


def infer_weight_from_priceinfo(price_info: Dict[str, Any], price_gbp: Optional[float]) -> Tuple[Optional[float], Optional[str]]:
    """
    Infer total pack size when priceInfo contains a per-100g or per-100ml style price.
    Returns (weight, base_unit) where base_unit is 'g' or 'ml'.
    """
    if not isinstance(price_info, dict) or not price_gbp:
        return None, None
    per_unit = as_text(price_info.get("perUnit") or price_info.get("per_unit"))
    if not per_unit:
        return None, None

    qty, unit = parse_perunit_quantity(per_unit)
    if not qty or not unit or unit not in ("g", "ml") or int(round(qty)) != 100:
        return None, None

    money_vals = extract_money_values_from_dict(price_info)
    if not money_vals:
        return None, None

    # choose smallest price that's less than item price as candidate per-100 price
    candidates = [v for v in money_vals if v < price_gbp]
    per100_price = min(candidates) if candidates else None
    if not per100_price or per100_price <= 0:
        return None, None

    weight = (price_gbp / per100_price) * 100.0
    if 0 < weight < 100000:
        return round(weight, 1), unit
    return None, None


# ----------------- Nutrition helpers -----------------
def choose_per100_index(heads: List[Any]) -> Optional[int]:
    if not heads:
        return None
    candidates: List[Tuple[int, str]] = []
    for i, h in enumerate(heads):
        t = as_text(h).strip().lower()
        if "per 100" in t:
            candidates.append((i, t))
    if not candidates:
        return None
    for i, t in candidates:
        if ("% " not in t) and (" ri" not in t) and ("reference" not in t) and ("%" not in t):
            return i
    return candidates[0][0]


def nutrition_is_cooked(title: Optional[str]) -> bool:
    if not title:
        return False
    tl = title.lower()
    cook_words = [
        "cooked","fried","baked","boiled","grilled","roasted","steamed","poached","braised",
        "sauteed","sautéed","stewed","broiled","barbecued","microwaved","toasted","prepared",
        "heated","pan fried","pan-fried",
    ]
    neg = ["uncooked", "raw"]
    if any(w in tl for w in cook_words) and not any(n in tl for n in neg):
        return True
    if any(n in tl for n in neg):
        return False
    return False


def _extract_unit_value(text: Any, unit_keyword: str) -> Optional[float]:
    s = as_text(text)
    if not s:
        return None
    m = re.search(rf"(\d+(?:\.\d+)?)\s*{unit_keyword}\b", s, re.I)
    return float(m.group(1)) if m else None


def parse_nutrition_iceland(nut: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract per-100 values + optional second heading values.
    Converts between kJ and kcal ONLY if one is missing.
    Safety swap: if calories > kJ for same basis, swap them.
    """
    res: Dict[str, Any] = {
        "first_nutrition_title_string": None,
        "has_second_nutritional_value_title": False,
        "second_nutrition_title_string": None,
        "second_title_numeric": None,
        "kj_per_100_uom": None,
        "calories_per_100_uom": None,
        "fat_per_100_uom": None,
        "saturates_per_100_uom": None,
        "carbs_per_100_uom": None,
        "sugars_per_100_uom": None,
        "fibre_per_100_uom": None,
        "protein_per_100_uom": None,
        "salt_per_100_uom": None,
        "second_kj": None,
        "second_calories": None,
        "second_fat": None,
        "second_saturates": None,
        "second_carbs": None,
        "second_sugars": None,
        "second_fibre": None,
        "second_protein": None,
        "second_salt": None,
    }
    if not nut or not isinstance(nut, dict):
        return res

    heads = nut.get("headings") or []
    per_idx = choose_per100_index(heads)

    sec_idx = None
    for i, h in enumerate(heads):
        if i == per_idx:
            continue
        if as_text(h).strip():
            sec_idx = i
            break

    res["first_nutrition_title_string"] = heads[per_idx] if per_idx is not None else None
    if sec_idx is not None:
        res["has_second_nutritional_value_title"] = True
        res["second_nutrition_title_string"] = heads[sec_idx]
        m = re.search(r"(\d+(?:\.\d+)?)", as_text(heads[sec_idx]))
        if m:
            res["second_title_numeric"] = float(m.group(1))

    rows = nut.get("nutrients") or []

    def num(s: Any) -> Optional[float]:
        if s is None:
            return None
        m = _NUM_RE.search(as_text(s))
        return float(m.group(1)) if m else None

    def set_if_none(key: str, value: Optional[float]):
        if value is None:
            return
        if res.get(key) is None:
            res[key] = value

    for r in rows:
        name = (r.get("name") or "").strip().lower()
        vals = r.get("values") or []
        v_per = vals[per_idx] if (per_idx is not None and per_idx < len(vals)) else None
        v_sec = vals[sec_idx] if (sec_idx is not None and sec_idx < len(vals)) else None

        # ENERGY / KJ / KCAL rows (robust to blank name rows)
        if ("energy" in name) or ("kj" in name) or ("kcal" in name) or (name == ""):
            kj_per_val = _extract_unit_value(v_per, "kj")
            kj_sec_val = _extract_unit_value(v_sec, "kj")
            set_if_none("kj_per_100_uom", kj_per_val)
            set_if_none("second_kj", kj_sec_val)

            kcal_per_val = _extract_unit_value(v_per, "kcal")
            kcal_sec_val = _extract_unit_value(v_sec, "kcal")
            set_if_none("calories_per_100_uom", kcal_per_val)
            set_if_none("second_calories", kcal_sec_val)

            if "energy" in name:
                if res.get("kj_per_100_uom") is None and v_per:
                    kjm = re.search(r"(\d+(?:\.\d+)?)\s*kj", as_text(v_per), re.I)
                    if kjm:
                        res["kj_per_100_uom"] = float(kjm.group(1))
                if res.get("calories_per_100_uom") is None and v_per:
                    kcalm = re.search(r"(\d+(?:\.\d+)?)\s*kcal", as_text(v_per), re.I)
                    if kcalm:
                        res["calories_per_100_uom"] = float(kcalm.group(1))
                if res.get("second_kj") is None and v_sec:
                    kj2 = re.search(r"(\d+(?:\.\d+)?)\s*kj", as_text(v_sec), re.I)
                    if kj2:
                        res["second_kj"] = float(kj2.group(1))
                if res.get("second_calories") is None and v_sec:
                    kc2 = re.search(r"(\d+(?:\.\d+)?)\s*kcal", as_text(v_sec), re.I)
                    if kc2:
                        res["second_calories"] = float(kc2.group(1))

        # Other macros & nutrients
        def set_pair(key100: str, key2: str):
            if v_per is not None:
                set_if_none(key100, num(v_per))
            if v_sec is not None:
                set_if_none(key2, num(v_sec))

        if "fat" in name and "saturate" not in name:
            set_pair("fat_per_100_uom", "second_fat")
        elif "saturate" in name:
            set_pair("saturates_per_100_uom", "second_saturates")
        elif "carbohydrate" in name or name.startswith("carbo"):
            set_pair("carbs_per_100_uom", "second_carbs")
        elif "sugar" in name:
            set_pair("sugars_per_100_uom", "second_sugars")
        elif "fibre" in name or "fiber" in name:
            set_pair("fibre_per_100_uom", "second_fibre")
        elif "protein" in name:
            set_pair("protein_per_100_uom", "second_protein")
        elif "salt" in name:
            set_pair("salt_per_100_uom", "second_salt")

    # Safety swap: if calories > kJ, assume swapped
    if res["kj_per_100_uom"] is not None and res["calories_per_100_uom"] is not None:
        if res["calories_per_100_uom"] > res["kj_per_100_uom"]:
            res["kj_per_100_uom"], res["calories_per_100_uom"] = res["calories_per_100_uom"], res["kj_per_100_uom"]

    if res["second_kj"] is not None and res["second_calories"] is not None:
        if res["second_calories"] > res["second_kj"]:
            res["second_kj"], res["second_calories"] = res["second_calories"], res["second_kj"]

    # Only convert if missing
    if res["kj_per_100_uom"] is not None and res["calories_per_100_uom"] is None:
        res["calories_per_100_uom"] = round(res["kj_per_100_uom"] / 4.184, 1)
    if res["calories_per_100_uom"] is not None and res["kj_per_100_uom"] is None:
        res["kj_per_100_uom"] = round(res["calories_per_100_uom"] * 4.184, 1)

    if res["second_kj"] is not None and res["second_calories"] is None:
        res["second_calories"] = round(res["second_kj"] / 4.184, 1)
    if res["second_calories"] is not None and res["second_kj"] is None:
        res["second_kj"] = round(res["second_calories"] * 4.184, 1)

    return res


# ----------------- Bread slice detection helpers -----------------
_SLICES_CONTAINS_RE = re.compile(
    r"(?:contain(?:s)?|include(?:s)?)\s*(?:about|approx\.?|typically)?\s*(\d{1,3})\s*slices?\b",
    re.I,
)
_SLICES_LOOSE_RE = re.compile(r"\b(\d{1,3})\s*slices?\b", re.I)


def detect_bread_slices_from_text(*texts: Optional[str]) -> Optional[int]:
    blob = " ".join([as_text(t) for t in texts if t]).lower()
    m = _SLICES_CONTAINS_RE.search(blob)
    if m:
        try:
            n = int(m.group(1))
            return n if n > 1 else None
        except Exception:
            pass
    for m in _SLICES_LOOSE_RE.finditer(blob):
        try:
            n = int(m.group(1))
            span_start = max(0, m.start() - 40)
            context = blob[span_start : m.start()]
            if re.search(r"(pack|loaf|bread|this\s+pack|per\s+pack|including\s+crusts)", context):
                return n if n > 1 else None
        except Exception:
            continue
    return None


# ----------------- Other helpers -----------------
EGG_WEIGHTS = {"very large": 73, "large": 68, "big": 68, "medium": 58, "small": 48, "mixed": 59}


def calculate_egg_weight(product_name: str, quantity: Optional[int]) -> Optional[float]:
    if not quantity or quantity <= 1:
        return None
    name_lower = (product_name or "").lower()
    for size, w in EGG_WEIGHTS.items():
        if size in name_lower:
            return float(w * quantity)
    if "egg" in name_lower:
        return float(EGG_WEIGHTS["medium"] * quantity)
    return None


def calculate_bakery_item_weight(
    nut: Dict[str, Any],
    category_level_1: Optional[str],
    uom: Optional[str],
    product_name: str,
    packet_quantity: Optional[int],
) -> Optional[float]:
    if (uom or "").lower() != "each":
        return None
    if not category_level_1 or category_level_1.strip().lower() != "bakery":
        return None
    if not packet_quantity or packet_quantity <= 1:
        return None

    st = nut.get("second_nutrition_title_string") or ""
    m = re.search(r"\((\d+(?:\.\d+)?)\s*g\)", st, re.I)
    if m:
        return round(float(m.group(1)) * packet_quantity, 0)

    e100 = nut.get("kj_per_100_uom") or (
        nut.get("calories_per_100_uom") * 4.184 if nut.get("calories_per_100_uom") else None
    )
    eitem = nut.get("second_kj") or (
        nut.get("second_calories") * 4.184 if nut.get("second_calories") else None
    )
    if e100 and eitem and e100 > 0:
        indiv = (eitem / e100) * 100.0
        return round(indiv * packet_quantity, 0)

    return None


def brand_normalize(brand_original: Optional[str], title: Optional[str]) -> Tuple[Optional[str], Optional[bool]]:
    if brand_original:
        bo = brand_original.strip()
        bol = bo.lower()
        if bol.startswith("iceland luxury") or bol.startswith("iceland takeaway"):
            brand = "Iceland"
        else:
            brand = bo
    else:
        brand = None

    own_brand = None
    base = ((brand or "") + " " + (title or "")).lower()
    if "iceland" in base:
        own_brand = True
    return brand, own_brand


def detect_claims_text(
    dietary: Optional[str],
    desc: Optional[str],
    ingredients_raw: Optional[str],
    title: Optional[str],
) -> Dict[str, Optional[bool]]:
    blob = " ".join([as_text(x).lower() for x in [dietary, desc, ingredients_raw, title] if x])
    return {
        "is_probiotic": True if re.search(r"\bprobiotic(s)?\b|live\s+cultures|bio\s*cultures", blob) else None,
        "is_caffeinated": True if re.search(r"\bcaffeine\b|\bcaffeinated\b|contains\s+caffeine", blob) else None,
        "is_fairtrade": True if re.search(r"\bfair\s*trade\b|\bfairtrade\b", blob) else None,
        "no_added_sugar": True if re.search(r"\bno\s+added\s+sugar\b", blob) else None,
        "lactose_free": True if re.search(r"\blactose[-\s]?free\b", blob) else None,
        "is_dairy_free": True if re.search(r"\bdairy[-\s]?free\b|\bmilk[-\s]?free\b", blob) else None,
        "contains_alcohol": True if re.search(r"\b(\d+(?:\.\d+)?)\s*%?\s*abv\b|\balcoholic?\b|\bcontains\s+alcohol\b", blob) else None,
    }


# ----------------- API fetch with degradation -----------------
def fetch_products_batch(ids: List[str], timeout: int = DEFAULT_TIMEOUT) -> List[Dict[str, Any]]:
    payload = {"ids": ",".join(ids), "day": 1, "store": "0", "ignoreRange": True}
    resp = requests.post(ALGOLIA_PRODUCTS_URL, headers=HEADERS, json=payload, timeout=timeout)
    if resp.status_code != 200:
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}: {resp.text[:200]}")
    data = resp.json()
    if isinstance(data, list):
        return data
    return data.get("hits") or data.get("results") or []


def fetch_with_degradation(ids: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Try batch; if fail, recursively split.
    Returns (products, failed_ids)
    """
    if not ids:
        return [], []
    try:
        prods = fetch_products_batch(ids)
        return (prods or []), []
    except Exception:
        if len(ids) == 1:
            for attempt in range(1, DEFAULT_MAX_ATTEMPTS + 1):
                try:
                    time.sleep(0.4 * attempt + random.uniform(0, 0.4))
                    prods = fetch_products_batch(ids)
                    return (prods or []), []
                except Exception:
                    continue
            return [], ids
        mid = max(1, len(ids) // 2)
        left, lfail = fetch_with_degradation(ids[:mid])
        right, rfail = fetch_with_degradation(ids[mid:])
        return left + right, lfail + rfail


# ----------------- Output schema -----------------
COLUMNS = [
    "retailer","product_name","product_item_name","product_code","url","brand_original","brand","own_brand",
    "item_weight_or_volume","item_uom","packet_quantity","has_serving_info","listed_servings","calculated_item_weight",
    "has_was_price","was_price_as_listed","item_price_no_discounts_applied","price_source",
    "price_per_100_uom_no_discounts","serving_price_no_discounts","packet_quantity_unit_price_no_discounts",
    "has_loyalty_discount","loyalty_discounted_price","loyalty_discounted_percentage",
    "first_nutrition_title_string","nutrition_basis_is_cooked","nutrition_basis_conversion_percentage",
    "kj_per_100_uom","calories_per_100_uom","fat_per_100_uom","saturates_per_100_uom","carbs_per_100_uom",
    "sugars_per_100_uom","fibre_per_100_uom","protein_per_100_uom","salt_per_100_uom",
    "protein_price_for_100g_protein_no_discounts","price_per_100_calories_no_discounts","protein_per_100_calories",
    "has_second_nutritional_value_title","second_nutrition_title_string","second_title_numeric","second_kj","second_calories",
    "second_fat","second_saturates","second_carbs","second_sugars","second_fibre","second_protein","second_salt",
    "is_new","is_bread","bread_slices_count","is_egg","is_free_range","is_organic","is_frozen","is_halal","is_gluten_free",
    "is_kosher","is_vegan","is_vegetarian","is_probiotic","is_caffeinated","is_fairtrade","no_added_sugar","lactose_free",
    "is_dairy_free","contains_alcohol","is_certified_nut_free","is_bundle_of_products","has_product_life_info",
    "product_life_info_original_text","product_life_info_converted","product_life","suitable_for_freezing",
    "country_of_origin_original_text","country_of_origin_converted","allergens_original_text",
    "allergens_parsed_from_allergens_original_text","allergens_bolded_from_ingredients","ingredients_original_text",
    "ingredients_converted","storage_type_original_text","storage_type_converted","cooking_instructions_original_text",
    "microwave_safe","usage_info_original_text","usage_info_converted","nutriscore","ecoscore","max_quantity",
    "no_of_reviews","average_review","availability_status","category_path","category_level_1","category_level_2",
    "category_level_3","category_level_4","category_level_5","category_level_6","has_multibuy_option","multibuy_id1",
    "multibuy_id1_promo_start_date","multibuy_id1_promo_end_date","multibuy_bundle_price1","multibuy_quantity1",
    "multibuy_unit_price1","multibuy_id2","multibuy_id2_promo_start_date","multibuy_id2_promo_end_date",
    "multibuy_bundle_price2","multibuy_quantity2","multibuy_unit_price2","has_buy_x_get_y","buy_x_get_y_promo_id",
    "buy_x_get_y_promo_start_date","buy_x_get_y_promo_end_date","x_quantity_requirement","y_quantity","total_requirement",
    "is_meal_deal","meal_deal_item_type","meal_deal_loyalty_price1","meal_deal_non_loyalty_price1","meal_deal_id1",
    "meal_deal_id1_promo_start_date","meal_deal_id1_promo_end_date","meal_deal_id2","meal_deal_loyalty_price2",
    "meal_deal_non_loyalty_price2","meal_deal_id2_promo_start_date","meal_deal_id2_promo_end_date","last_scraped",
    "description_original_text","dietary_information_original_text","tpnb","tpnc","gtin","ean"
]


# ----------------- Row builder -----------------
def build_row(prod: Dict[str, Any], src_meta: Dict[str, Any]) -> Dict[str, Any]:
    title = as_text(prod.get("name")) or None
    product_id = str(prod.get("id")) if prod.get("id") is not None else src_meta.get("product_code")
    brand_original = as_text(prod.get("brand")) or None

    brand, own_brand_flag = brand_normalize(brand_original, title)

    # Prices
    price_gbp = None
    price = prod.get("price") or {}
    if isinstance(price, dict):
        raw = price.get("GBP")
        try:
            price_gbp = float(raw) if raw is not None else None
        except Exception:
            price_gbp = None

    price_info = prod.get("priceInfo") or {}

    # Weight & packet quantity
    item_weight, item_uom = parse_weight_from_title(title or "")
    packet_quantity = detect_packet_quantity(title or "")

    # If no uom from title, try perUnit
    per_unit_str = as_text(price_info.get("perUnit") or price_info.get("per_unit"))
    qty_u, unit_u = parse_perunit_quantity(per_unit_str)
    if not item_uom and unit_u:
        if unit_u == "each":
            item_uom = "each"
        elif unit_u in ("g", "ml", "l", "kg", "cl") and (qty_u is not None) and int(round(qty_u)) != 100:
            if unit_u == "kg":
                item_weight = (item_weight or 0) or (qty_u * 1000.0)
                item_uom = "g"
            elif unit_u == "l":
                item_weight = (item_weight or 0) or (qty_u * 1000.0)
                item_uom = "ml"
            elif unit_u == "cl":
                item_weight = (item_weight or 0) or (qty_u * 10.0)
                item_uom = "ml"
            elif unit_u in ("g", "ml"):
                item_weight = (item_weight or 0) or qty_u
                item_uom = unit_u

    # Infer size from per-100 price if needed
    if item_weight is None and price_gbp:
        inferred_w, inferred_u = infer_weight_from_priceinfo(price_info, price_gbp)
        if inferred_w and inferred_u:
            item_weight, item_uom = inferred_w, inferred_u

    # Nutrition (skip if category L1 is Pets)
    level_1 = src_meta.get("category_level_1") or ""
    nut_raw = prod.get("nutritionInformation") or {}
    if level_1.strip().lower() == "pets":
        nut = {
            "first_nutrition_title_string": None,
            "has_second_nutritional_value_title": False,
            "second_nutrition_title_string": None,
            "second_title_numeric": None,
            "kj_per_100_uom": None,
            "calories_per_100_uom": None,
            "fat_per_100_uom": None,
            "saturates_per_100_uom": None,
            "carbs_per_100_uom": None,
            "sugars_per_100_uom": None,
            "fibre_per_100_uom": None,
            "protein_per_100_uom": None,
            "salt_per_100_uom": None,
            "second_kj": None,
            "second_calories": None,
            "second_fat": None,
            "second_saturates": None,
            "second_carbs": None,
            "second_sugars": None,
            "second_fibre": None,
            "second_protein": None,
            "second_salt": None,
        }
        first_title = None
        is_cooked = False
    else:
        nut = parse_nutrition_iceland(nut_raw)
        first_title = nut["first_nutrition_title_string"]
        is_cooked = nutrition_is_cooked(first_title)

    # is_egg only if level_3 == "Eggs"
    level_3 = (src_meta.get("category_level_3") or "").strip().lower()
    is_egg = True if level_3 == "eggs" else None

    # is_bread only if level_2 == "Bread"
    level_2 = (src_meta.get("category_level_2") or "").strip().lower()
    is_bread = True if level_2 == "bread" else None

    # If eggs and uom missing => 'each'
    if is_egg and not item_uom:
        item_uom = "each"

    # Calculated weights
    calc_weight = None
    if is_egg:
        calc_weight = calculate_egg_weight(title or "", packet_quantity)
    if calc_weight is None and (item_uom or "").lower() == "each":
        w = calculate_bakery_item_weight(nut, level_1, item_uom, title or "", packet_quantity)
        if w:
            calc_weight = w

    # Serving inference
    has_serving_info = False
    listed_servings = None
    per_item_grams = nut.get("second_title_numeric")
    total_pack_g = item_weight or calc_weight
    if total_pack_g and per_item_grams and per_item_grams > 0:
        est_serv = int(round(float(total_pack_g) / float(per_item_grams)))
        if est_serv >= 1:
            has_serving_info = True
            listed_servings = est_serv

    # Bread slice detection
    desc = as_text(prod.get("longDescription") or prod.get("shortDescription")) or None
    dietary_info = as_text(prod.get("dietaryInformation")) or None
    bread_slices = detect_bread_slices_from_text(nut_raw, desc, dietary_info, title)

    # grams per slice from second title
    if bread_slices is None:
        st = (nut.get("second_nutrition_title_string") or "").lower()
        if "slice" in st and nut.get("second_title_numeric") and total_pack_g:
            grams_per_slice = float(nut["second_title_numeric"])
            if grams_per_slice > 0:
                bread_slices = int(round(total_pack_g / grams_per_slice))

    # energy ratio fallback for bread slices
    if bread_slices is None:
        second_head = (nut.get("second_nutrition_title_string") or "").lower()
        if "slice" in second_head and total_pack_g:
            e100_kj = nut.get("kj_per_100_uom")
            e100_kcal = nut.get("calories_per_100_uom")
            eslice_kj = nut.get("second_kj")
            eslice_kcal = nut.get("second_calories")
            grams_per_slice = None
            if e100_kj and eslice_kj and e100_kj > 0 and eslice_kj > 0:
                grams_per_slice = (eslice_kj / e100_kj) * 100.0
            elif e100_kcal and eslice_kcal and e100_kcal > 0 and eslice_kcal > 0:
                grams_per_slice = (eslice_kcal / e100_kcal) * 100.0
            if grams_per_slice and grams_per_slice > 0:
                bread_slices = int(round(float(total_pack_g) / grams_per_slice))

    # Pricing & derived metrics
    price_per100 = None
    protein_price_for_100g_protein = None
    price_per_100_kcal = None
    if price_gbp and total_pack_g and total_pack_g > 0:
        price_per100 = round((price_gbp / float(total_pack_g)) * 100.0, 4)
        cals_100 = nut.get("calories_per_100_uom")
        if cals_100 and cals_100 > 0:
            total_kcal = float(total_pack_g) * (float(cals_100) / 100.0)
            if total_kcal > 0:
                price_per_100_kcal = round((price_gbp / total_kcal) * 100.0, 4)
        prot_100 = nut.get("protein_per_100_uom")
        if prot_100 and prot_100 > 0:
            total_prot_g = float(total_pack_g) * (float(prot_100) / 100.0)
            if total_prot_g > 0:
                protein_price_for_100g_protein = round((price_gbp / total_prot_g) * 100.0, 4)

    serving_price = None
    if price_gbp and listed_servings and listed_servings > 0:
        serving_price = round(price_gbp / float(listed_servings), 4)

    # Availability
    in_stock = prod.get("in_stock")
    availability_status = ("Available" if in_stock else "Out of Stock") if isinstance(in_stock, bool) else None

    # Promos (simplified)
    promos = ((prod.get("productPromotions") or {}).get("nearPromos")) or []
    if isinstance(promos, dict):
        promos = [promos]

    has_multibuy = False
    mb1 = {"id": None, "start": None, "end": None, "bundle": None, "qty": None, "unit": None}
    mb2 = {"id": None, "start": None, "end": None, "bundle": None, "qty": None, "unit": None}
    buyxgety = {"has": False, "id": None, "start": None, "end": None, "x": None, "y": None, "total": None}

    mb_count = 0
    for p in promos:
        t = p.get("t") or p.get("type")
        pid = p.get("ID") or p.get("id")
        msg = as_text(p.get("msg"))
        start = p.get("sd") or p.get("startDate")
        end = p.get("ed") or p.get("endDate")

        if (t == "BuyXForTotal") and msg:
            mb_count += 1
            m = re.search(r"(\d+)\s+for\s+£\s*([0-9]+(?:\.[0-9]{1,2})?)", msg, re.I)
            qty = int(m.group(1)) if m else None
            bundle = float(m.group(2)) if m else None
            unit = round(bundle / qty, 3) if (bundle and qty) else None
            if mb_count == 1:
                has_multibuy = True
                mb1.update({"id": pid, "start": start, "end": end, "bundle": bundle, "qty": qty, "unit": unit})
            elif mb_count == 2:
                mb2.update({"id": pid, "start": start, "end": end, "bundle": bundle, "qty": qty, "unit": unit})

        if t == "BuyXGetY":
            buyxgety.update({
                "has": True,
                "id": pid,
                "start": start,
                "end": end,
                "x": p.get("buyXReq"),
                "y": p.get("getYQuantity"),
                "total": p.get("req"),
            })

    # Allergens & ingredients
    ingredients_list = prod.get("ingredients") or []
    ingredients_raw = "; ".join([as_text(i) for i in ingredients_list]) if ingredients_list else None
    ingredients_bold = extract_bold_ingredients(ingredients_raw or "") if ingredients_raw else []
    allergens_contains = prod.get("allergenContains") or []
    allergens_may = prod.get("allergenMayContain") or []
    allergens_original = None
    if allergens_contains or allergens_may:
        parts = []
        if allergens_contains:
            parts.append(", ".join(allergens_contains))
        if allergens_may:
            parts.append("may contain: " + ", ".join(allergens_may))
        allergens_original = "; ".join(parts)

    bag: List[str] = []
    for ptxt in allergens_contains:
        bag.extend([t.strip() for t in re.split(r"\s*,\s*", ptxt) if t.strip()])
    for ptxt in allergens_may:
        bag.extend([t.strip() for t in re.split(r"\s*,\s*", ptxt) if t.strip()])
    bag.extend(ingredients_bold or [])
    seen = set()
    allergens_parsed_list: List[str] = []
    for tkn in bag:
        tl = tkn.lower()
        if tl in seen:
            continue
        seen.add(tl)
        allergens_parsed_list.append(tkn)
    allergens_parsed = ", ".join(allergens_parsed_list) if allergens_parsed_list else None

    # Claims
    claims = detect_claims_text(dietary_info, desc, ingredients_raw, title)
    base_blob = " ".join([as_text(x).lower() for x in [dietary_info, desc, ingredients_raw, title] if x])
    is_halal = True if "halal" in base_blob else None
    is_kosher = True if "kosher" in base_blob else None
    is_vegan = True if re.search(r"\bvegan(s)?\b|suitable\s+for\s+vegans", base_blob) else None
    is_vegetarian = True if re.search(r"\bvegetarian(s)?\b|suitable\s+for\s+vegetarians", base_blob) else None
    is_gluten_free = True if re.search(r"\bgluten[-\s]?free\b", base_blob) else None
    is_frozen = True if (level_1 or "").strip().lower() == "frozen" else None
    is_free_range = True if re.search(r"\bfree[-\s]?range\b", base_blob) else None
    is_organic = True if re.search(r"\borganic\b", base_blob) else None

    gtin = prod.get("gtin") or prod.get("ean") or None
    no_reviews = prod.get("productReviewCount")
    avg_review = prod.get("productRating")

    # If bread and slices detected, force packet_quantity
    if is_bread and bread_slices and bread_slices > 0:
        packet_quantity = bread_slices

    row: Dict[str, Any] = {
        "retailer": "Iceland",
        "product_name": title,
        "product_item_name": title,
        "product_code": product_id,
        "url": src_meta.get("product_url"),
        "brand_original": brand_original,
        "brand": brand,
        "own_brand": own_brand_flag,
        "item_weight_or_volume": float(item_weight) if item_weight is not None else None,
        "item_uom": (item_uom or None),
        "packet_quantity": int(packet_quantity) if packet_quantity else None,
        "has_serving_info": has_serving_info,
        "listed_servings": listed_servings,
        "calculated_item_weight": float(calc_weight) if calc_weight is not None else None,
        "has_was_price": True if (prod.get("priceInfo") or {}).get("wasPrice") is not None else False,
        "was_price_as_listed": (prod.get("priceInfo") or {}).get("wasPrice"),
        "item_price_no_discounts_applied": price_gbp,
        "price_source": "API",
        "price_per_100_uom_no_discounts": price_per100,
        "serving_price_no_discounts": serving_price,
        "packet_quantity_unit_price_no_discounts": (
            round(price_gbp / packet_quantity, 4) if (price_gbp and packet_quantity and packet_quantity > 1) else None
        ),
        "has_loyalty_discount": None,
        "loyalty_discounted_price": None,
        "loyalty_discounted_percentage": None,
        "first_nutrition_title_string": first_title,
        "nutrition_basis_is_cooked": is_cooked,
        "nutrition_basis_conversion_percentage": None,
        "kj_per_100_uom": nut.get("kj_per_100_uom"),
        "calories_per_100_uom": nut.get("calories_per_100_uom"),
        "fat_per_100_uom": nut.get("fat_per_100_uom"),
        "saturates_per_100_uom": nut.get("saturates_per_100_uom"),
        "carbs_per_100_uom": nut.get("carbs_per_100_uom"),
        "sugars_per_100_uom": nut.get("sugars_per_100_uom"),
        "fibre_per_100_uom": nut.get("fibre_per_100_uom"),
        "protein_per_100_uom": nut.get("protein_per_100_uom"),
        "salt_per_100_uom": nut.get("salt_per_100_uom"),
        "protein_price_for_100g_protein_no_discounts": protein_price_for_100g_protein,
        "price_per_100_calories_no_discounts": price_per_100_kcal,
        "protein_per_100_calories": (
            round((nut["protein_per_100_uom"] / nut["calories_per_100_uom"]) * 100.0, 4)
            if (nut.get("protein_per_100_uom") and nut.get("calories_per_100_uom") and nut["calories_per_100_uom"] > 0)
            else None
        ),
        "has_second_nutritional_value_title": nut.get("has_second_nutritional_value_title"),
        "second_nutrition_title_string": nut.get("second_nutrition_title_string"),
        "second_title_numeric": nut.get("second_title_numeric"),
        "second_kj": nut.get("second_kj"),
        "second_calories": nut.get("second_calories"),
        "second_fat": nut.get("second_fat"),
        "second_saturates": nut.get("second_saturates"),
        "second_carbs": nut.get("second_carbs"),
        "second_sugars": nut.get("second_sugars"),
        "second_fibre": nut.get("second_fibre"),
        "second_protein": nut.get("second_protein"),
        "second_salt": nut.get("second_salt"),
        "is_new": prod.get("isNew"),
        "is_bread": is_bread,
        "bread_slices_count": bread_slices,
        "is_egg": is_egg,
        "is_free_range": True if re.search(r"\bfree[-\s]?range\b", (title or "").lower()) else None,
        "is_organic": is_organic,
        "is_frozen": is_frozen,
        "is_halal": is_halal,
        "is_gluten_free": is_gluten_free,
        "is_kosher": is_kosher,
        "is_vegan": is_vegan,
        "is_vegetarian": is_vegetarian,
        "is_probiotic": claims["is_probiotic"],
        "is_caffeinated": claims["is_caffeinated"],
        "is_fairtrade": claims["is_fairtrade"],
        "no_added_sugar": claims["no_added_sugar"],
        "lactose_free": claims["lactose_free"],
        "is_dairy_free": claims["is_dairy_free"],
        "contains_alcohol": claims["contains_alcohol"],
        "is_certified_nut_free": None,
        "is_bundle_of_products": None,
        "has_product_life_info": True if prod.get("shelfLifeMessage") else None,
        "product_life_info_original_text": prod.get("shelfLifeMessage"),
        "product_life_info_converted": prod.get("shelfLifeMessage"),
        "product_life": None,
        "suitable_for_freezing": None,
        "country_of_origin_original_text": None,
        "country_of_origin_converted": None,
        "allergens_original_text": allergens_original,
        "allergens_parsed_from_allergens_original_text": allergens_parsed,
        "allergens_bolded_from_ingredients": "; ".join(ingredients_bold) if ingredients_bold else None,
        "ingredients_original_text": ingredients_raw,
        "ingredients_converted": ingredients_raw,
        "storage_type_original_text": None,
        "storage_type_converted": None,
        "cooking_instructions_original_text": None,
        "microwave_safe": None,
        "usage_info_original_text": None,
        "usage_info_converted": None,
        "nutriscore": None,
        "ecoscore": None,
        "max_quantity": prod.get("maxOrderQuantity"),
        "no_of_reviews": no_reviews,
        "average_review": avg_review,
        "availability_status": availability_status,
        "category_path": src_meta.get("category_path"),
        "category_level_1": src_meta.get("category_level_1"),
        "category_level_2": src_meta.get("category_level_2"),
        "category_level_3": src_meta.get("category_level_3"),
        "category_level_4": src_meta.get("category_level_4"),
        "category_level_5": src_meta.get("category_level_5"),
        "category_level_6": src_meta.get("category_level_6"),
        "has_multibuy_option": has_multibuy,
        "multibuy_id1": mb1["id"],
        "multibuy_id1_promo_start_date": mb1["start"],
        "multibuy_id1_promo_end_date": mb1["end"],
        "multibuy_bundle_price1": mb1["bundle"],
        "multibuy_quantity1": mb1["qty"],
        "multibuy_unit_price1": mb1["unit"],
        "multibuy_id2": mb2["id"],
        "multibuy_id2_promo_start_date": mb2["start"],
        "multibuy_id2_promo_end_date": mb2["end"],
        "multibuy_bundle_price2": mb2["bundle"],
        "multibuy_quantity2": mb2["qty"],
        "multibuy_unit_price2": mb2["unit"],
        "has_buy_x_get_y": buyxgety["has"],
        "buy_x_get_y_promo_id": buyxgety["id"],
        "buy_x_get_y_promo_start_date": buyxgety["start"],
        "buy_x_get_y_promo_end_date": buyxgety["end"],
        "x_quantity_requirement": buyxgety["x"],
        "y_quantity": buyxgety["y"],
        "total_requirement": buyxgety["total"],
        "is_meal_deal": None,
        "meal_deal_item_type": None,
        "meal_deal_loyalty_price1": None,
        "meal_deal_non_loyalty_price1": None,
        "meal_deal_id1": None,
        "meal_deal_id1_promo_start_date": None,
        "meal_deal_id1_promo_end_date": None,
        "meal_deal_id2": None,
        "meal_deal_loyalty_price2": None,
        "meal_deal_non_loyalty_price2": None,
        "meal_deal_id2_promo_start_date": None,
        "meal_deal_id2_promo_end_date": None,
        "last_scraped": datetime.now().isoformat(),
        "description_original_text": desc,
        "dietary_information_original_text": dietary_info,
        "tpnb": None,
        "tpnc": None,
        "gtin": gtin,
        "ean": gtin,
    }

    # Ensure all columns exist
    for k in COLUMNS:
        if k not in row:
            row[k] = None

    return row


# ----------------- CSV helpers -----------------
def append_rows(out_path: Path, rows: List[Dict[str, Any]], header_written: bool) -> bool:
    if not rows:
        return header_written
    df = pd.DataFrame(rows)
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[COLUMNS]
    df.to_csv(out_path, mode="a", index=False, header=(not header_written))
    return True


def append_errors(errs_path: Path, errs: List[Dict[str, Any]]):
    if not errs:
        return
    df = pd.DataFrame(errs, columns=["date_iso", "product_code", "url", "stage", "message"])
    header = not errs_path.exists()
    df.to_csv(errs_path, mode="a", index=False, header=header)


def chunked(seq: List[Any], n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


# ----------------- CLI / main flow -----------------
@dataclass
class Paths:
    input_csv: Path
    out_dir: Path
    log_dir: Path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Iceland Phase 2 (public-safe, test IDs only).")
    p.add_argument("--input", dest="input_csv", default="data/input/p1_iceland.csv",
                   help="Phase 1 CSV path (default: data/input/p1_iceland.csv)")
    p.add_argument("--outdir", dest="out_dir", default="data/output",
                   help="Output directory (default: data/output)")
    p.add_argument("--logdir", dest="log_dir", default="logs",
                   help="Log directory (default: logs)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    p.add_argument("--batch-write", type=int, default=DEFAULT_BATCH_WRITE)
    return p


def configure_logging(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("ICELAND_P2_PUBLIC")


def run(paths: Paths, concurrency: int, chunk_size: int, batch_write: int) -> int:
    ts_short = datetime.now().strftime("%Y%m%d_%H%M")
    paths.out_dir.mkdir(parents=True, exist_ok=True)
    paths.log_dir.mkdir(parents=True, exist_ok=True)

    out_csv = paths.out_dir / f"p2_iceland_test_{ts_short}.csv"
    errs_csv = paths.log_dir / "iceland_enrich_errors.csv"
    log_file = paths.log_dir / f"iceland_phase2_test_{ts_short}.log"
    log = configure_logging(log_file)

    # Load Phase 1
    try:
        usecols = ["product_code", "product_name", "product_url", "category_path",
                   "level_1", "level_2", "level_3", "level_4", "level_5", "level_6"]
        df = pd.read_csv(paths.input_csv, usecols=usecols)
        df = df.dropna(subset=["product_code"])
        df["product_code"] = df["product_code"].astype(str)
    except Exception as e:
        log.error(f"Failed to read input CSV '{paths.input_csv}': {e}")
        return 1

    # TEST MODE ONLY (filter to your provided IDs)
    target = set(TEST_PRODUCT_IDS)
    df = df[df["product_code"].isin(target)].copy()
    if df.empty:
        log.error("None of the TEST_PRODUCT_IDS were found in the input CSV. "
                  "Make sure Phase 1 includes those product_code values.")
        return 1

    # shuffle to spread load
    seed = random.randint(1, 10**9)
    df = df.sample(frac=1.0, random_state=seed).reset_index(drop=True)

    items: List[Tuple[str, Dict[str, Any]]] = []
    for _, r in df.iterrows():
        meta = {
            "product_code": str(r["product_code"]),
            "product_url": r.get("product_url"),
            "category_path": r.get("category_path"),
            "category_level_1": r.get("level_1"),
            "category_level_2": r.get("level_2"),
            "category_level_3": r.get("level_3"),
            "category_level_4": r.get("level_4"),
            "category_level_5": r.get("level_5"),
            "category_level_6": r.get("level_6"),
        }
        items.append((meta["product_code"], meta))

    log.info(f"Starting Iceland Phase 2 TEST: {len(items)} products -> {out_csv}")
    header_written = out_csv.exists()
    pending_rows: List[Dict[str, Any]] = []
    pending_errs: List[Dict[str, Any]] = []

    def process_ids(id_meta_list: List[Tuple[str, Dict[str, Any]]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        rows: List[Dict[str, Any]] = []
        errs: List[Dict[str, Any]] = []

        ids = [pid for (pid, _) in id_meta_list]
        random.shuffle(ids)

        prods, failed = fetch_with_degradation(ids)

        by_id: Dict[str, Dict[str, Any]] = {}
        for p in prods:
            pid = str(p.get("id"))
            if pid:
                by_id[pid] = p

        for pid, meta in id_meta_list:
            prod = by_id.get(pid)
            if prod:
                try:
                    row = build_row(prod, meta)
                    rows.append(row)
                except Exception as e:
                    errs.append({
                        "date_iso": datetime.now().isoformat(),
                        "product_code": pid,
                        "url": meta.get("product_url"),
                        "stage": "parse",
                        "message": f"{type(e).__name__}: {e}",
                    })
            else:
                if pid in failed:
                    errs.append({
                        "date_iso": datetime.now().isoformat(),
                        "product_code": pid,
                        "url": meta.get("product_url"),
                        "stage": "fetch",
                        "message": "failed after degradation",
                    })
                else:
                    errs.append({
                        "date_iso": datetime.now().isoformat(),
                        "product_code": pid,
                        "url": meta.get("product_url"),
                        "stage": "fetch",
                        "message": "missing in batch response",
                    })

        return rows, errs

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = []
        for group in chunked(items, chunk_size):
            time.sleep(random.uniform(0.05, 0.2))  # small jitter
            futures.append(ex.submit(process_ids, group))

        for fut in as_completed(futures):
            try:
                rows, errs = fut.result()
                if rows:
                    pending_rows.extend(rows)
                    if len(pending_rows) >= batch_write:
                        header_written = append_rows(out_csv, pending_rows, header_written) or header_written
                        pending_rows.clear()
                if errs:
                    pending_errs.extend(errs)
                    if len(pending_errs) >= 100:
                        append_errors(errs_csv, pending_errs)
                        pending_errs.clear()
            except Exception as e:
                log.warning(f"Worker failure: {e}")

    if pending_rows:
        header_written = append_rows(out_csv, pending_rows, header_written) or header_written
    if pending_errs:
        append_errors(errs_csv, pending_errs)

    # final dedupe + sort
    try:
        df_out = pd.read_csv(out_csv)
        before = len(df_out)
        df_out = df_out.drop_duplicates(subset=["product_code"], keep="last")
        df_out["category_path"] = df_out["category_path"].replace("", pd.NA)
        df_out = df_out.sort_values(by="category_path", ascending=True, na_position="last")
        df_out.to_csv(out_csv, index=False)
        after = len(df_out)
        log.info(f"Final de-dupe: {before} -> {after}; sorted by category_path.")
    except Exception as e:
        log.warning(f"Final de-dupe/sort failed: {e}")

    log.info(f"Done. Output -> {out_csv}")
    log.info(f"Logs  -> {log_file}")
    if errs_csv.exists():
        log.info(f"Errors -> {errs_csv}")

    return 0


def main() -> int:
    args = build_parser().parse_args()
    paths = Paths(
        input_csv=Path(args.input_csv),
        out_dir=Path(args.out_dir),
        log_dir=Path(args.log_dir),
    )
    return run(paths, concurrency=args.concurrency, chunk_size=args.chunk_size, batch_write=args.batch_write)


if __name__ == "__main__":
    raise SystemExit(main())