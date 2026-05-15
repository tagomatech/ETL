#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import plotly
import requests
from jinja2 import Template


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AUTH_DIR = PROJECT_ROOT / "auth"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REFERENCE_DIR = PROJECT_ROOT / "data" / "reference"
VIEWS_DIR = PROJECT_ROOT / "views"
LOGS_DIR = PROJECT_ROOT / "logs"

PROCESSED_CSV = PROCESSED_DIR / "ukraine_weekly_crop_progress.csv"
TRANSLATION_CSV = REFERENCE_DIR / "translation_lookup.csv"
DATA_DICTIONARY_CSV = REFERENCE_DIR / "data_dictionary.csv"
PUBLIC_SEED_CSV = REFERENCE_DIR / "public_mirror_seed.csv"
LATEST_LOG_FILE = LOGS_DIR / "latest.log"
UKRAINE_MAP_SVG = REFERENCE_DIR / "ukraine_oblast_map.svg"
EXECUTIVE_TEMPLATE_FILE = PROJECT_ROOT / "src" / "executive_dashboard_template.html"

DEFAULT_AUTH_FILE = AUTH_DIR / "minagro_request.txt"
DEFAULT_AUTH_FILE_ARG = str(Path("auth") / "minagro_request.txt")
DEFAULT_PROCESSED_CSV_ARG = str(Path("data") / "processed" / "ukraine_weekly_crop_progress.csv")
DEFAULT_WEEKDAY = "FRI"
BROWSER_PROFILE_DIR = AUTH_DIR / "browser_profile"
DEFAULT_BROWSER_AUTH_TIMEOUT_SECONDS = 420
DEFAULT_HISTORY_START = date(2024, 1, 5)
DEFAULT_HISTORY_START_ARG = DEFAULT_HISTORY_START.isoformat()

BASE_URL = "https://minagro.gov.ua"
API_URL = f"{BASE_URL}/api/minagro"
BROWSER_CDP_HOST = "127.0.0.1"

LEGACY_CATEGORY_ALIASES: dict[str, tuple[str, str, str]] = {
    "hid-zbirannya-urozhayu": ("hid-polovih-robit", "Хід збирання", "Harvest progress"),
}


@dataclass(frozen=True)
class Category:
    slug: str
    label_uk: str
    label_en: str
    season_mode: str


@dataclass(frozen=True)
class SeasonRule:
    start_month: int
    label_style: str


CATEGORIES: tuple[Category, ...] = (
    Category("hid-sivbi-yarih-kultur", "Хід сівби ярих культур", "Spring sowing progress", "spring"),
    Category("hid-sivbi-ozimih-kultur", "Хід сівби озимих культур", "Winter sowing progress", "winter"),
    # The live Minagro homepage currently exposes this harvest slug in its category selector.
    Category("hid-polovih-robit", "Хід збирання", "Harvest progress", "harvest"),
)

CATEGORY_BY_SLUG = {item.slug: item for item in CATEGORIES}

OBLASTS: tuple[dict[str, str], ...] = (
    {"slug": "vinnytska", "uk": "Вінницька область", "en": "Vinnytsia"},
    {"slug": "volynska", "uk": "Волинська область", "en": "Volyn"},
    {"slug": "dnipropetrovska", "uk": "Дніпропетровська область", "en": "Dnipropetrovsk"},
    {"slug": "donetska", "uk": "Донецька область", "en": "Donetsk"},
    {"slug": "zhytomyrska", "uk": "Житомирська область", "en": "Zhytomyr"},
    {"slug": "zakarpatska", "uk": "Закарпатська область", "en": "Zakarpattia"},
    {"slug": "zaporizka", "uk": "Запорізька область", "en": "Zaporizhzhia"},
    {"slug": "ivano-frankivska", "uk": "Івано-Франківська область", "en": "Ivano-Frankivsk"},
    {"slug": "kyivska", "uk": "Київська область", "en": "Kyiv"},
    {"slug": "kirovohradska", "uk": "Кіровоградська область", "en": "Kirovohrad"},
    {"slug": "luhanska", "uk": "Луганська область", "en": "Luhansk"},
    {"slug": "lvivska", "uk": "Львівська область", "en": "Lviv"},
    {"slug": "mykolaivska", "uk": "Миколаївська область", "en": "Mykolaiv"},
    {"slug": "odeska", "uk": "Одеська область", "en": "Odesa"},
    {"slug": "poltavska", "uk": "Полтавська область", "en": "Poltava"},
    {"slug": "rivnenska", "uk": "Рівненська область", "en": "Rivne"},
    {"slug": "sumska", "uk": "Сумська область", "en": "Sumy"},
    {"slug": "ternopilska", "uk": "Тернопільська область", "en": "Ternopil"},
    {"slug": "kharkivska", "uk": "Харківська область", "en": "Kharkiv"},
    {"slug": "khersonska", "uk": "Херсонська область", "en": "Kherson"},
    {"slug": "khmelnytska", "uk": "Хмельницька область", "en": "Khmelnytskyi"},
    {"slug": "cherkaska", "uk": "Черкаська область", "en": "Cherkasy"},
    {"slug": "chernivetska", "uk": "Чернівецька область", "en": "Chernivtsi"},
    {"slug": "chernihivska", "uk": "Чернігівська область", "en": "Chernihiv"},
)

OBLAST_BY_SLUG = {item["slug"]: item for item in OBLASTS}

MAP_SLUG_BY_OBLAST_SLUG = {
    "vinnytska": "vinnitska",
    "volynska": "volinska",
    "dnipropetrovska": "dnipropetrovska",
    "donetska": "donetska",
    "zhytomyrska": "zhitomirska",
    "zakarpatska": "zakarpatska",
    "zaporizka": "zaporizka",
    "ivano-frankivska": "ivano-frankivska",
    "kyivska": "kiivska",
    "kirovohradska": "kirovogradska",
    "luhanska": "luganska",
    "lvivska": "lvivska",
    "mykolaivska": "mikolaivska",
    "odeska": "odeska",
    "poltavska": "poltavska",
    "rivnenska": "rivnenska",
    "sumska": "sumska",
    "ternopilska": "ternopilska",
    "kharkivska": "kharkivska",
    "khersonska": "khersonska",
    "khmelnytska": "khmelnitska",
    "cherkaska": "cherkaska",
    "chernivetska": "chernivetska",
    "chernihivska": "chernigivska",
}

CROP_TRANSLATIONS = {
    "Пшениця": "Wheat",
    "Пшениця озима": "Winter wheat",
    "Пшениця яра": "Spring wheat",
    "Ячмінь": "Barley",
    "Ячмінь озимий": "Winter barley",
    "Ячмінь ярий": "Spring barley",
    "Жито": "Rye",
    "Овес": "Oats",
    "Кукурудза": "Corn",
    "Кукурудза на зерно": "Corn (grain)",
    "Горох": "Peas",
    "Гречка": "Buckwheat",
    "Просо": "Millet",
    "Ріпак": "Rapeseed",
    "Ріпак озимий": "Winter rapeseed",
    "Соняшник": "Sunflower",
    "Соя": "Soybean",
    "Цукрові буряки": "Sugar beet",
    "цукровий буряк": "Sugar beet",
    "Картопля": "Potatoes",
    "Овочі": "Vegetables",
    "зернові та зернобобові": "Cereals and pulses",
    "озимі на зерно": "Winter grains",
}

UK_TO_LATIN = {
    "а": "a", "б": "b", "в": "v", "г": "h", "ґ": "g", "д": "d", "е": "e",
    "є": "ie", "ж": "zh", "з": "z", "и": "y", "і": "i", "ї": "i", "й": "i",
    "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch",
    "ш": "sh", "щ": "shch", "ь": "", "ю": "iu", "я": "ia", "'": "", "’": "",
}

WORD_TRANSLATIONS = {
    "прогноз": "forecast",
    "фактично": "actual",
    "посіяно": "sown",
    "засіяно": "sown",
    "до": "to",
    "прогнозу": "plan",
    "зібрано": "harvested",
    "обмолочено": "harvested",
    "намолочено": "produced",
    "урожайність": "yield",
    "добрий": "good",
    "добрі": "good",
    "відмінний": "excellent",
    "відмінні": "excellent",
    "задовільний": "satisfactory",
    "задовільні": "satisfactory",
    "слабкі": "weak",
    "зріджені": "sparse",
    "загинуло": "lost",
    "не": "not",
    "отримано": "received",
    "сходів": "emergence",
}

METRIC_RULES: tuple[tuple[re.Pattern[str], str, str], ...] = (
    (re.compile(r"^прогноз$", re.I), "forecast_area", "Forecast area"),
    (re.compile(r"^(фактично|посіяно|засіяно)$", re.I), "actual_area", "Actual area"),
    (re.compile(r"^до прогнозу$", re.I), "progress_to_plan_pct", "Progress to plan"),
    (re.compile(r"^(обмолочено|зібрано)$", re.I), "harvested_area", "Harvested area"),
    (re.compile(r"^намолочено$", re.I), "production", "Production"),
    (re.compile(r"^урожайність$", re.I), "yield", "Yield"),
    (re.compile(r"(добр|відмінн)", re.I), "good_condition_share", "Good condition share"),
    (re.compile(r"задовільн", re.I), "satisfactory_condition_share", "Satisfactory condition share"),
    (re.compile(r"(слабк|зріджен)", re.I), "weak_sparse_condition_share", "Weak or sparse condition share"),
    (re.compile(r"загинул", re.I), "lost_condition_share", "Lost condition share"),
    (re.compile(r"не.*сход", re.I), "no_emergence_share", "No emergence share"),
)

UNIT_TRANSLATIONS = {
    "%": "Percent",
    "тис.га": "Thousand ha",
    "тис. га": "Thousand ha",
    "тис.тонн": "Thousand tonnes",
    "тис. тонн": "Thousand tonnes",
    "т/га": "Tonnes per ha",
    "ц/га": "Centners per ha",
    "га": "Ha",
}

CONDITION_WEIGHTS = {
    "good_condition_share": 1.0,
    "satisfactory_condition_share": 0.6,
    "weak_sparse_condition_share": 0.2,
    "lost_condition_share": 0.0,
    "no_emergence_share": 0.0,
}

# USDA FAS Crop Explorer uses a September-August winter crop season and an April-start spring crop
# season for Ukraine. For harvest rows, the crop-specific campaign starts below are anchored to USDA
# FAS marketing-year starts so late-winter updates stay attached to the same harvest campaign instead
# of resetting on January 1.
SEASON_RULES_BY_MODE = {
    "spring": SeasonRule(start_month=4, label_style="single"),
    "winter": SeasonRule(start_month=9, label_style="range"),
}

HARVEST_SEASON_RULES_BY_CROP = {
    "__all__": SeasonRule(start_month=7, label_style="single"),
    "gorokh": SeasonRule(start_month=7, label_style="single"),
    "grechka": SeasonRule(start_month=9, label_style="single"),
    "kukurudza": SeasonRule(start_month=10, label_style="single"),
    "kukurudza-na-zerno": SeasonRule(start_month=10, label_style="single"),
    "oves": SeasonRule(start_month=7, label_style="single"),
    "ozimi-na-zerno": SeasonRule(start_month=7, label_style="single"),
    "proso": SeasonRule(start_month=9, label_style="single"),
    "pshenitsya": SeasonRule(start_month=7, label_style="single"),
    "ripak": SeasonRule(start_month=7, label_style="single"),
    "sonyashnik": SeasonRule(start_month=9, label_style="single"),
    "soya": SeasonRule(start_month=9, label_style="single"),
    "tsukrovi-buryaki": SeasonRule(start_month=9, label_style="single"),
    "tsukroviy-buryak": SeasonRule(start_month=9, label_style="single"),
    "yachmin": SeasonRule(start_month=7, label_style="single"),
    "yari-zernovi-ta-zernobobovi": SeasonRule(start_month=7, label_style="single"),
    "zernovi-ta-zernobobovi": SeasonRule(start_month=7, label_style="single"),
    "zhito": SeasonRule(start_month=7, label_style="single"),
}

DEFAULT_HARVEST_SEASON_RULE = SeasonRule(start_month=7, label_style="single")

PUBLIC_MIRROR_SEED = [
    {
        "snapshot_date": "2024-07-12",
        "category_slug": "hid-polovih-robit",
        "category_en": "Harvest progress",
        "category_uk": "Хід збирання",
        "campaign_year": 2024,
        "campaign_week": 28,
        "season_label": "2024",
        "geography_level": "national",
        "oblast_slug": "",
        "oblast_en": "Ukraine",
        "oblast_uk": "Вся Україна",
        "crop_slug": "__all__",
        "crop_en": "All crops",
        "crop_uk": "Усі культури",
        "item_key": "production",
        "item_en": "Production",
        "item_uk": "Намолочено",
        "metric_key": "production",
        "metric_en": "Production",
        "metric_uk": "Намолочено",
        "value": 8.3,
        "unit_uk": "млн тонн",
        "unit_en": "Million tonnes",
        "source_kind": "public_mirror_seed",
        "source_path": "https://sky.zp.ua/?p=912716",
        "notes": "Public mirror article text only; no oblast breakdown available.",
    },
    {
        "snapshot_date": "2024-08-16",
        "category_slug": "hid-polovih-robit",
        "category_en": "Harvest progress",
        "category_uk": "Хід збирання",
        "campaign_year": 2024,
        "campaign_week": 33,
        "season_label": "2024",
        "geography_level": "national",
        "oblast_slug": "",
        "oblast_en": "Ukraine",
        "oblast_uk": "Вся Україна",
        "crop_slug": "__all__",
        "crop_en": "All crops",
        "crop_uk": "Усі культури",
        "item_key": "production",
        "item_en": "Production",
        "item_uk": "Намолочено",
        "metric_key": "production",
        "metric_en": "Production",
        "metric_uk": "Намолочено",
        "value": 31.8213,
        "unit_uk": "млн тонн",
        "unit_en": "Million tonnes",
        "source_kind": "public_mirror_seed",
        "source_path": "https://sky.zp.ua/?p=918277",
        "notes": "Public mirror article text only; no full oblast table available.",
    },
    {
        "snapshot_date": "2024-10-11",
        "category_slug": "hid-polovih-robit",
        "category_en": "Harvest progress",
        "category_uk": "Хід збирання",
        "campaign_year": 2024,
        "campaign_week": 41,
        "season_label": "2024",
        "geography_level": "national",
        "oblast_slug": "",
        "oblast_en": "Ukraine",
        "oblast_uk": "Вся Україна",
        "crop_slug": "__all__",
        "crop_en": "All crops",
        "crop_uk": "Усі культури",
        "item_key": "production",
        "item_en": "Production",
        "item_uk": "Намолочено",
        "metric_key": "production",
        "metric_en": "Production",
        "metric_uk": "Намолочено",
        "value": 62.2,
        "unit_uk": "млн тонн",
        "unit_en": "Million tonnes",
        "source_kind": "public_mirror_seed",
        "source_path": "https://sky.zp.ua/stanom-na-11-zhovtnya-v-ukra%D1%97ni-zibrali-ponad-62-miljoni-tonn-novogo-vrozhayu-minagro/",
        "notes": "Public mirror article text only; no full oblast table available.",
    },
    {
        "snapshot_date": "2025-05-09",
        "category_slug": "hid-sivbi-yarih-kultur",
        "category_en": "Spring sowing progress",
        "category_uk": "Хід сівби ярих культур",
        "campaign_year": 2025,
        "campaign_week": 19,
        "season_label": "2025",
        "geography_level": "national",
        "oblast_slug": "",
        "oblast_en": "Ukraine",
        "oblast_uk": "Вся Україна",
        "crop_slug": "__all__",
        "crop_en": "All spring grains and pulses",
        "crop_uk": "Усі ярі зернові та зернобобові",
        "item_key": "actual_area",
        "item_en": "Actual area",
        "item_uk": "Фактично",
        "metric_key": "actual_area",
        "metric_en": "Actual area",
        "metric_uk": "Фактично",
        "value": 4.32,
        "unit_uk": "млн га",
        "unit_en": "Million ha",
        "source_kind": "public_mirror_seed",
        "source_path": "https://uga.ua/ru/news/agraryy-zaseyaly-uzhe-76-yarovyh-zernovyh-y-zernobobovyh-kultur-ot-prognozyruemyh-ploshhadej/",
        "notes": "Public mirror article text only; no crop-by-oblast table available.",
    },
    {
        "snapshot_date": "2025-05-09",
        "category_slug": "hid-sivbi-yarih-kultur",
        "category_en": "Spring sowing progress",
        "category_uk": "Хід сівби ярих культур",
        "campaign_year": 2025,
        "campaign_week": 19,
        "season_label": "2025",
        "geography_level": "national",
        "oblast_slug": "",
        "oblast_en": "Ukraine",
        "oblast_uk": "Вся Україна",
        "crop_slug": "__all__",
        "crop_en": "All spring grains and pulses",
        "crop_uk": "Усі ярі зернові та зернобобові",
        "item_key": "progress_to_plan_pct",
        "item_en": "Progress to plan",
        "item_uk": "До прогнозу",
        "metric_key": "progress_to_plan_pct",
        "metric_en": "Progress to plan",
        "metric_uk": "До прогнозу",
        "value": 76.0,
        "unit_uk": "%",
        "unit_en": "Percent",
        "source_kind": "public_mirror_seed",
        "source_path": "https://uga.ua/ru/news/agraryy-zaseyaly-uzhe-76-yarovyh-zernovyh-y-zernobobovyh-kultur-ot-prognozyruemyh-ploshhadej/",
        "notes": "Public mirror article text only; no crop-by-oblast table available.",
    },
]

DATA_DICTIONARY = [
    ("snapshot_date", "Date of the weekly snapshot requested from the ministry API."),
    ("category_slug", "Official ministry category slug."),
    ("category_en", "English display label for the category."),
    ("category_uk", "Original Ukrainian category label."),
    ("campaign_year", "Derived seasonal campaign year used for overlays."),
    ("campaign_week", "1-based week index inside the derived seasonal campaign."),
    ("season_label", "Display label for the derived seasonal campaign."),
    ("geography_level", "`national` or `oblast`."),
    ("oblast_slug", "Official oblast slug used by the site."),
    ("oblast_en", "English oblast label."),
    ("oblast_uk", "Original Ukrainian oblast label."),
    ("crop_slug", "Official crop slug from the API."),
    ("crop_en", "English crop label."),
    ("crop_uk", "Original Ukrainian crop label."),
    ("item_key", "Normalized metric key used by dashboard slicers."),
    ("item_en", "English item label used in the dashboards."),
    ("item_uk", "Original or derived Ukrainian item label."),
    ("metric_key", "Normalized raw metric key."),
    ("metric_en", "English metric label."),
    ("metric_uk", "Original Ukrainian metric label."),
    ("value", "Numeric value returned by the site or derived from it."),
    ("unit_uk", "Original Ukrainian unit."),
    ("unit_en", "English unit label."),
    ("source_kind", "Either `minagro_api` or `public_mirror_seed`."),
    ("source_path", "Raw cache file path or public mirror source URL."),
    ("notes", "Free-text notes for transparency."),
]

EXECUTIVE_HTML = VIEWS_DIR / "executive_dashboard.html"


class WorkflowError(RuntimeError):
    pass


class AuthRefreshRequired(WorkflowError):
    pass


@dataclass(frozen=True)
class RequestSpec:
    snapshot_date: date
    category_slug: str
    oblast_slug: str
    cache_file: Path
    source_path: str


@dataclass(frozen=True)
class SyncPlan:
    start_date: date
    end_date: date
    expected_dates: list[date]
    missing_dates: list[date]
    missing_request_count: int
    expected_request_count: int
    historical_gap_dates: list[date]
    new_gap_dates: list[date]
    latest_api_date: Optional[date]
    cached_reprocess_request_count: int
    cached_empty_request_count: int


RUN_LOG_FILE: Optional[Path] = None
DISABLED_LOG_PATHS: set[str] = set()
LOG_WRITE_WARNING_SHOWN = False


def display_path(value: Any) -> str:
    path = Path(str(value))
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def log(message: str) -> None:
    global LOG_WRITE_WARNING_SHOWN

    print(message)
    if RUN_LOG_FILE is None:
        return

    timestamped = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
    for path in [RUN_LOG_FILE, LATEST_LOG_FILE]:
        path_key = str(path)
        if path_key in DISABLED_LOG_PATHS:
            continue

        for attempt in range(3):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(timestamped)
                break
            except PermissionError:
                if attempt < 2:
                    time.sleep(0.15 * (attempt + 1))
                    continue
                DISABLED_LOG_PATHS.add(path_key)
                if not LOG_WRITE_WARNING_SHOWN:
                    print(
                        f"Warning: file logging became unavailable for {display_path(path)}. "
                        "The workflow will continue and keep printing to the console.",
                        file=sys.stderr,
                    )
                    LOG_WRITE_WARNING_SHOWN = True
            except Exception:
                DISABLED_LOG_PATHS.add(path_key)
                if not LOG_WRITE_WARNING_SHOWN:
                    print(
                        f"Warning: could not keep writing the log file at {display_path(path)}. "
                        "The workflow will continue and keep printing to the console.",
                        file=sys.stderr,
                    )
                    LOG_WRITE_WARNING_SHOWN = True


def init_run_logging() -> None:
    global RUN_LOG_FILE

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RUN_LOG_FILE = LOGS_DIR / f"minagro_weekly_workflow_{timestamp}.log"

    header = (
        f"Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        "Workflow: src/minagro_weekly_workflow.py\n"
        "----\n"
    )
    for path in [RUN_LOG_FILE, LATEST_LOG_FILE]:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(header, encoding="utf-8")
        except Exception:
            DISABLED_LOG_PATHS.add(str(path))
    log(f"Run log: {display_path(RUN_LOG_FILE)}")


def ensure_directories() -> None:
    for path in (AUTH_DIR, RAW_DIR, PROCESSED_DIR, REFERENCE_DIR, VIEWS_DIR, LOGS_DIR):
        path.mkdir(parents=True, exist_ok=True)


def transliterate_uk(text: str) -> str:
    pieces: list[str] = []
    for char in text.lower():
        if char in UK_TO_LATIN:
            pieces.append(UK_TO_LATIN[char])
        elif char.isascii() and char.isalnum():
            pieces.append(char)
        else:
            pieces.append("-")
    slug = re.sub(r"-+", "-", "".join(pieces)).strip("-")
    return slug or "unknown"


def phrase_to_english(text: str) -> str:
    parts = re.findall(r"[A-Za-zА-Яа-яІіЇїЄєҐґ0-9']+", text)
    translated = [WORD_TRANSLATIONS.get(part.lower(), part) for part in parts]
    result = " ".join(translated).strip()
    if not result:
        return "Unknown metric"
    return result[0].upper() + result[1:]


def normalize_metric(metric_uk: str) -> tuple[str, str]:
    for pattern, key, label_en in METRIC_RULES:
        if pattern.search(metric_uk):
            return key, label_en
    return transliterate_uk(metric_uk), phrase_to_english(metric_uk)


def normalize_crop(crop_uk: str, crop_slug: str) -> tuple[str, str]:
    if crop_uk in CROP_TRANSLATIONS:
        return crop_slug or transliterate_uk(crop_uk), CROP_TRANSLATIONS[crop_uk]
    return crop_slug or transliterate_uk(crop_uk), phrase_to_english(crop_uk)


def normalize_unit(unit_uk: str) -> str:
    if unit_uk in UNIT_TRANSLATIONS:
        return UNIT_TRANSLATIONS[unit_uk]
    if unit_uk in {"млн га", "млн. га"}:
        return "Million ha"
    if unit_uk in {"млн тонн", "млн. тонн", "млн т"}:
        return "Million tonnes"
    return unit_uk or "Unit not provided"


def category_for_slug(slug: str) -> Category:
    if slug not in CATEGORY_BY_SLUG:
        raise WorkflowError(f"Unknown category slug: {slug}")
    return CATEGORY_BY_SLUG[slug]


def season_rule_for_row(category_slug: str, crop_slug: str = "") -> SeasonRule:
    category = category_for_slug(category_slug)
    if category.season_mode == "harvest":
        return HARVEST_SEASON_RULES_BY_CROP.get(crop_slug, DEFAULT_HARVEST_SEASON_RULE)
    if category.season_mode in SEASON_RULES_BY_MODE:
        return SEASON_RULES_BY_MODE[category.season_mode]
    return SeasonRule(start_month=1, label_style="single")


def compute_campaign_start_year(snapshot_date: date, category_slug: str, crop_slug: str = "") -> int:
    rule = season_rule_for_row(category_slug, crop_slug)
    return snapshot_date.year if snapshot_date.month >= rule.start_month else snapshot_date.year - 1


def compute_campaign_year(snapshot_date: date, category_slug: str, crop_slug: str = "") -> int:
    start_year = compute_campaign_start_year(snapshot_date, category_slug, crop_slug)
    rule = season_rule_for_row(category_slug, crop_slug)
    if rule.label_style == "range":
        return start_year + 1
    return start_year


def compute_campaign_week(snapshot_date: date, category_slug: str, crop_slug: str = "") -> int:
    start_year = compute_campaign_start_year(snapshot_date, category_slug, crop_slug)
    rule = season_rule_for_row(category_slug, crop_slug)
    start_date = date(start_year, rule.start_month, 1)
    return ((snapshot_date - start_date).days // 7) + 1


def compute_season_label(snapshot_date: date, category_slug: str, crop_slug: str = "") -> str:
    start_year = compute_campaign_start_year(snapshot_date, category_slug, crop_slug)
    rule = season_rule_for_row(category_slug, crop_slug)
    if rule.label_style == "range":
        return f"{start_year}/{start_year + 1}"
    return str(start_year)


def raw_cache_path(snapshot_date: date, category_slug: str, oblast_slug: str) -> Path:
    safe_oblast = oblast_slug or "country"
    return RAW_DIR / snapshot_date.isoformat() / category_slug / f"{safe_oblast}.json"


def parse_weekday(value: str) -> str:
    value = value.strip().upper()
    valid = {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}
    if value not in valid:
        raise WorkflowError(f"Weekday must be one of {sorted(valid)}.")
    return value


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_weekly_dates(start_date: date, end_date: date, weekday_code: str) -> list[date]:
    if end_date < start_date:
        raise WorkflowError("`--to` date must be on or after `--from` date.")
    rng = pd.date_range(start_date, end_date, freq=f"W-{weekday_code}")
    dates = [item.date() for item in rng]
    if not dates or dates[0] != start_date:
        dates = [start_date] + dates
    if dates[-1] != end_date:
        dates.append(end_date)
    return sorted(set(dates))


def build_anchor_weekly_dates(start_date: date, end_date: date, weekday_code: str) -> list[date]:
    if end_date < start_date:
        raise WorkflowError("`--to` date must be on or after `--from` date.")
    return [item.date() for item in pd.date_range(start_date, end_date, freq=f"W-{weekday_code}")]


def normalize_source_path_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if re.match(r"^[a-z]+://", text, re.I):
        return text
    return Path(text).as_posix()


def scoped_oblasts(limit_oblasts: Optional[int]) -> list[dict[str, str]]:
    if limit_oblasts is None:
        return list(OBLASTS)
    return list(OBLASTS[: max(0, limit_oblasts)])


def build_request_specs(snapshot_dates: Iterable[date], limit_oblasts: Optional[int]) -> list[RequestSpec]:
    oblasts = scoped_oblasts(limit_oblasts)
    specs: list[RequestSpec] = []

    for snapshot_date in sorted(set(snapshot_dates)):
        for category in CATEGORIES:
            for oblast_slug in [""] + [item["slug"] for item in oblasts]:
                cache_file = raw_cache_path(snapshot_date, category.slug, oblast_slug)
                specs.append(
                    RequestSpec(
                        snapshot_date=snapshot_date,
                        category_slug=category.slug,
                        oblast_slug=oblast_slug,
                        cache_file=cache_file,
                        source_path=normalize_source_path_text(cache_file.relative_to(PROJECT_ROOT)),
                    )
                )

    return specs


def build_translation_lookup_rows(observed_rows: Optional[pd.DataFrame] = None) -> list[dict[str, str]]:
    lookup: dict[tuple[str, str, str], dict[str, str]] = {}

    def add(label_type: str, source_uk: str, display_en: str, normalized_key: str) -> None:
        source_uk = source_uk or ""
        display_en = display_en or ""
        normalized_key = normalized_key or ""
        key = (label_type, source_uk, display_en)
        lookup[key] = {
            "label_type": label_type,
            "source_uk": source_uk,
            "display_en": display_en,
            "normalized_key": normalized_key,
        }

    for category in CATEGORIES:
        add("category", category.label_uk, category.label_en, category.slug)
    add("oblast", "Вся Україна", "Ukraine", "country")
    for oblast in OBLASTS:
        add("oblast", oblast["uk"], oblast["en"], oblast["slug"])
    for crop_uk, crop_en in CROP_TRANSLATIONS.items():
        add("crop", crop_uk, crop_en, transliterate_uk(crop_uk))
    for _, key, metric_en in METRIC_RULES:
        add("metric", key, metric_en, key)
    add("metric", "Індекс стану посівів (розрахунковий)", "Crop condition index (derived)", "condition_index_100")

    if observed_rows is not None and not observed_rows.empty:
        for row in observed_rows.to_dict("records"):
            add("category", str(row["category_uk"]), str(row["category_en"]), str(row["category_slug"]))
            add("oblast", str(row["oblast_uk"]), str(row["oblast_en"]), str(row["oblast_slug"] or "country"))
            add("crop", str(row["crop_uk"]), str(row["crop_en"]), str(row["crop_slug"]))
            add("metric", str(row["metric_uk"]), str(row["metric_en"]), str(row["metric_key"]))
            add("item", str(row["item_uk"]), str(row["item_en"]), str(row["item_key"]))

    rows = list(lookup.values())
    rows.sort(key=lambda item: (item["label_type"], item["display_en"], item["source_uk"]))
    return rows


def write_translation_lookup(observed_rows: Optional[pd.DataFrame] = None) -> None:
    if observed_rows is not None and not observed_rows.empty:
        observed_rows = normalize_processed_dataframe(observed_rows)
    rows = build_translation_lookup_rows(observed_rows)
    with TRANSLATION_CSV.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=["label_type", "source_uk", "display_en", "normalized_key"])
        writer.writeheader()
        writer.writerows(rows)


def write_data_dictionary() -> None:
    with DATA_DICTIONARY_CSV.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["column_name", "description"])
        writer.writerows(DATA_DICTIONARY)


def write_public_seed_file() -> None:
    pd.DataFrame(PUBLIC_MIRROR_SEED).to_csv(PUBLIC_SEED_CSV, index=False, encoding="utf-8-sig")


def load_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def parse_env_style_auth(text: str) -> dict[str, str]:
    env_map: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_map[key.strip()] = value.strip().strip("'").strip('"')
    return env_map


def parse_curl_like_auth(text: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    cookie_header = ""

    compact = re.sub(r"\^\s*\r?\n", " ", text)
    compact = re.sub(r"\\\s*\r?\n", " ", compact)

    header_values = re.findall(r"""(?:-H|--header)\s+(?:"([^"]+)"|'([^']+)')""", compact)
    for double_value, single_value in header_values:
        raw = double_value or single_value
        if ":" not in raw:
            continue
        name, value = raw.split(":", 1)
        name = name.strip()
        value = value.strip()
        headers[name] = value
        if name.lower() == "cookie":
            cookie_header = value

    cookie_values = re.findall(r"""(?:-b|--cookie)\s+(?:"([^"]+)"|'([^']+)')""", compact)
    for double_value, single_value in cookie_values:
        cookie_header = double_value or single_value

    return {
        "USER_AGENT": headers.get("User-Agent", ""),
        "COOKIE_HEADER": cookie_header,
        "X_CSRF_TOKEN": headers.get("X-CSRF-TOKEN", headers.get("x-csrf-token", "")),
        "REFERER": headers.get("Referer", headers.get("referer", BASE_URL)),
        "ACCEPT_LANGUAGE": headers.get("Accept-Language", headers.get("accept-language", "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7")),
    }


def load_auth_bundle(auth_file: Path) -> dict[str, str]:
    if not auth_file.exists():
        raise WorkflowError(
            f"Auth file not found: {auth_file}\n"
            "Paste either a browser `Copy as cURL` request or an env-style block into that file."
        )

    text = load_text_file(auth_file)
    if not text:
        raise WorkflowError(f"Auth file is empty: {auth_file}")

    env_map = parse_env_style_auth(text)
    if env_map.get("COOKIE_HEADER") or env_map.get("USER_AGENT"):
        bundle = env_map
    else:
        bundle = parse_curl_like_auth(text)

    if not bundle.get("USER_AGENT"):
        raise WorkflowError("Could not find a `User-Agent` in the auth file.")
    if not bundle.get("COOKIE_HEADER"):
        raise WorkflowError("Could not find a `Cookie` header in the auth file.")
    return bundle


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for chunk in cookie_header.split(";"):
        piece = chunk.strip()
        if not piece or "=" not in piece:
            continue
        name, value = piece.split("=", 1)
        cookies[name.strip()] = value.strip()
    return cookies


def build_cookie_header(cookie_map: dict[str, str]) -> str:
    priority = ["cf_clearance", "minagro_session", "XSRF-TOKEN"]
    pieces: list[str] = []
    seen: set[str] = set()

    for name in priority:
        if name in cookie_map and cookie_map[name]:
            pieces.append(f"{name}={cookie_map[name]}")
            seen.add(name)

    for name in sorted(cookie_map):
        if name in seen or not cookie_map[name]:
            continue
        pieces.append(f"{name}={cookie_map[name]}")

    return "; ".join(pieces)


def write_auth_bundle(auth_file: Path, auth_bundle: dict[str, str], provenance: str) -> None:
    auth_file.parent.mkdir(parents=True, exist_ok=True)
    captured_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# Auto-generated auth bundle for the Ukraine weekly crop workflow.",
        f"# Captured: {captured_at}",
        f"# Provenance: {provenance}",
        f"USER_AGENT={auth_bundle.get('USER_AGENT', '')}",
        f"COOKIE_HEADER={auth_bundle.get('COOKIE_HEADER', '')}",
        f"X_CSRF_TOKEN={auth_bundle.get('X_CSRF_TOKEN', '')}",
        f"REFERER={auth_bundle.get('REFERER', BASE_URL)}",
        f"ACCEPT_LANGUAGE={auth_bundle.get('ACCEPT_LANGUAGE', 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7')}",
    ]
    auth_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def probe_auth_bundle(auth_bundle: dict[str, str], timeout_seconds: int = 30) -> tuple[bool, str]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": auth_bundle["USER_AGENT"],
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": auth_bundle.get("ACCEPT_LANGUAGE", "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7"),
            "Origin": BASE_URL,
            "Referer": auth_bundle.get("REFERER", BASE_URL),
            "X-Requested-With": "XMLHttpRequest",
            "Cookie": auth_bundle["COOKIE_HEADER"],
        }
    )
    if auth_bundle.get("X_CSRF_TOKEN"):
        session.headers["X-CSRF-TOKEN"] = auth_bundle["X_CSRF_TOKEN"]
        session.headers["X-XSRF-TOKEN"] = auth_bundle["X_CSRF_TOKEN"]

    try:
        response = session.get(
            API_URL,
            params={
                "category": "hid-sivbi-yarih-kultur",
                "to": date.today().strftime("%d.%m.%Y"),
            },
            timeout=timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"Probe request failed: {exc}"

    if response.status_code != 200:
        snippet = response.text[:160].replace("\n", " ")
        return False, f"Probe returned HTTP {response.status_code} at {response.url}. Snippet: {snippet}"

    try:
        payload = response.json()
    except Exception:  # noqa: BLE001
        snippet = response.text[:120].replace("\n", " ")
        return False, f"Probe returned non-JSON content from {response.url}: {snippet}"

    if not isinstance(payload, list):
        return False, "Probe returned JSON, but not the expected list payload."

    return True, f"Probe OK. API returned {len(payload)} crop groups."


def build_browser_auth_bundle(
    user_agent: str,
    page_url: str,
    cookie_items: list[dict[str, Any]],
    csrf_token: str,
) -> dict[str, str]:
    cookie_map = {
        str(cookie["name"]): str(cookie["value"])
        for cookie in cookie_items
        if "minagro.gov.ua" in str(cookie.get("domain", ""))
    }
    cookie_header = build_cookie_header(cookie_map)
    return {
        "USER_AGENT": user_agent,
        "COOKIE_HEADER": cookie_header,
        "X_CSRF_TOKEN": csrf_token or cookie_map.get("XSRF-TOKEN", ""),
        "REFERER": page_url if page_url.startswith(BASE_URL) else BASE_URL,
        "ACCEPT_LANGUAGE": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    }


def summarize_cookie_names(cookie_map: dict[str, str]) -> str:
    names = sorted(name for name, value in cookie_map.items() if value)
    return ", ".join(names) if names else "none"


def candidate_browser_paths() -> list[Path]:
    paths: list[Path] = []

    local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
    program_files = Path(os.environ.get("PROGRAMFILES", ""))
    program_files_x86 = Path(os.environ.get("PROGRAMFILES(X86)", ""))

    candidates = [
        local_app_data / "Google" / "Chrome" / "Application" / "chrome.exe",
        program_files / "Google" / "Chrome" / "Application" / "chrome.exe",
        program_files_x86 / "Google" / "Chrome" / "Application" / "chrome.exe",
        local_app_data / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        program_files / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        program_files_x86 / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ]

    for item in candidates:
        if item and item.exists():
            paths.append(item)

    for executable_name in ("chrome.exe", "msedge.exe", "chrome", "msedge"):
        resolved = shutil.which(executable_name)
        if resolved:
            paths.append(Path(resolved))

    unique_paths: list[Path] = []
    seen: set[str] = set()
    for item in paths:
        key = str(item).lower()
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(item)

    return unique_paths


def find_browser_executable() -> Path:
    candidates = candidate_browser_paths()
    if not candidates:
        raise WorkflowError(
            "Could not find a local Chrome or Edge executable. "
            "Install one of them or provide a manual auth bundle in auth/minagro_request.txt."
        )
    return candidates[0]


def reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((BROWSER_CDP_HOST, 0))
        return int(sock.getsockname()[1])


def wait_for_cdp_endpoint(port: int, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((BROWSER_CDP_HOST, port), timeout=1.5):
                return
        except OSError:
            time.sleep(0.5)
    raise WorkflowError(f"Timed out waiting for the browser debugging port {port} to open.")


def launch_attachable_browser(target_url: str) -> tuple[subprocess.Popen[Any], str]:
    browser_executable = find_browser_executable()
    port = reserve_local_port()
    BROWSER_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    command = [
        str(browser_executable),
        f"--remote-debugging-port={port}",
        f"--user-data-dir={BROWSER_PROFILE_DIR}",
        "--no-first-run",
        "--no-default-browser-check",
        "--new-window",
        target_url,
    ]

    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_cdp_endpoint(port=port, timeout_seconds=30)
    endpoint = f"http://{BROWSER_CDP_HOST}:{port}"
    return process, endpoint


def capture_auth_interactively(auth_file: Path, timeout_seconds: int) -> dict[str, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        raise WorkflowError(f"Playwright is required for automatic auth capture: {exc}") from exc

    BROWSER_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + timeout_seconds
    last_status = ""
    last_probe_signature = ""
    last_probe_at = 0.0
    provisional_bundle: Optional[dict[str, str]] = None
    redirected_from_404 = False

    with sync_playwright() as playwright:
        browser_process: Optional[subprocess.Popen[Any]] = None
        browser = None
        try:
            browser_process, cdp_endpoint = launch_attachable_browser(BASE_URL)
            browser = playwright.chromium.connect_over_cdp(cdp_endpoint)
            if not browser.contexts:
                raise WorkflowError("Connected to the browser, but no default context was available.")
            context = browser.contexts[0]
        except Exception as exc:
            if browser is not None:
                browser.close()
            if browser_process is not None and browser_process.poll() is None:
                browser_process.terminate()
            raise WorkflowError(f"Could not launch a normal browser window for auth capture: {exc}") from exc

        try:
            page = context.pages[0] if context.pages else context.new_page()
            if not page.url.startswith(BASE_URL):
                page.goto(BASE_URL, wait_until="domcontentloaded", timeout=120000)
            log("Browser opened for auth capture.")
            log(
                "If a Cloudflare checkbox appears, solve it in that browser window. "
                "This browser should look more normal now, and I will watch for the cookies automatically."
            )

            while time.time() < deadline:
                page.wait_for_timeout(3000)
                cookie_items = context.cookies()
                cookie_map = {
                    str(cookie["name"]): str(cookie["value"])
                    for cookie in cookie_items
                    if "minagro.gov.ua" in str(cookie.get("domain", ""))
                }
                page_url = page.url
                page_title = page.title()

                csrf_token = ""
                try:
                    csrf_token = page.locator("meta[name='csrf-token']").get_attribute("content") or ""
                except Exception:
                    csrf_token = ""

                bundle = build_browser_auth_bundle(
                    user_agent=page.evaluate("() => navigator.userAgent"),
                    page_url=page_url,
                    cookie_items=cookie_items,
                    csrf_token=csrf_token,
                )
                provisional_bundle = bundle if bundle.get("COOKIE_HEADER") else provisional_bundle

                status = (
                    f"url='{page_url}' | title='{page_title}' | "
                    f"cookie_names={summarize_cookie_names(cookie_map)} | "
                    f"cf_clearance={'yes' if cookie_map.get('cf_clearance') else 'no'}"
                )
                if status != last_status:
                    log(f"Auth capture status: {status}")
                    last_status = status

                if page_url.rstrip("/") == f"{BASE_URL}/statistics" and page_title.strip().startswith("404") and not redirected_from_404:
                    log("The `/statistics` page resolved to 404 after Cloudflare. Redirecting the browser to the site root and continuing.")
                    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=120000)
                    redirected_from_404 = True
                    continue

                if not bundle.get("COOKIE_HEADER"):
                    continue

                ready_for_probe = bool(cookie_map.get("cf_clearance")) or "just a moment" not in page_title.lower()
                if not ready_for_probe:
                    continue

                probe_signature = f"{page_url}|{bundle['COOKIE_HEADER']}|csrf={'yes' if csrf_token else 'no'}"
                if probe_signature == last_probe_signature and (time.time() - last_probe_at) < 15:
                    continue
                last_probe_signature = probe_signature
                last_probe_at = time.time()

                log(f"Auth probe attempt with cookie names: {summarize_cookie_names(cookie_map)}")
                ok, probe_message = probe_auth_bundle(bundle)
                log(f"Auth probe result: {probe_message}")
                if ok:
                    write_auth_bundle(auth_file, bundle, provenance="browser profile auto-capture")
                    log(f"Saved fresh auth bundle to {auth_file}")
                    return bundle

            if provisional_bundle and provisional_bundle.get("COOKIE_HEADER"):
                write_auth_bundle(auth_file, provisional_bundle, provenance="browser profile provisional capture")
                raise WorkflowError(
                    "I saved the latest detected cookies, but the probe still did not pass. "
                    "Please complete any remaining browser action on the Minagro page, then rerun the same command."
                )

            raise WorkflowError(
                f"Timed out after {timeout_seconds} seconds without capturing the required Minagro cookies."
            )
        finally:
            try:
                if browser is not None:
                    browser.close()
            finally:
                if browser_process is not None and browser_process.poll() is None:
                    browser_process.terminate()


def ensure_auth_bundle(
    auth_file: Path,
    allow_browser_capture: bool,
    timeout_seconds: int,
) -> dict[str, str]:
    auth_file = auth_file if auth_file.is_absolute() else PROJECT_ROOT / auth_file

    if auth_file.exists():
        try:
            bundle = load_auth_bundle(auth_file)
            ok, message = probe_auth_bundle(bundle)
            log(f"Saved auth check: {message}")
            if ok:
                return bundle
        except Exception as exc:  # noqa: BLE001
            log(f"Saved auth could not be used yet: {exc}")

    if not allow_browser_capture:
        raise WorkflowError(
            "No working auth bundle is available. Enable browser capture or refresh the auth file manually."
        )

    return capture_auth_interactively(auth_file=auth_file, timeout_seconds=timeout_seconds)


class PoliteMinagroFetcher:
    def __init__(self, auth_bundle: dict[str, str], sleep_seconds: float, retries: int) -> None:
        self.sleep_seconds = sleep_seconds
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": auth_bundle["USER_AGENT"],
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": auth_bundle.get("ACCEPT_LANGUAGE", "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7"),
                "Origin": BASE_URL,
                "Referer": auth_bundle.get("REFERER", BASE_URL),
                "X-Requested-With": "XMLHttpRequest",
                "Cookie": auth_bundle["COOKIE_HEADER"],
            }
        )
        if auth_bundle.get("X_CSRF_TOKEN"):
            self.session.headers["X-CSRF-TOKEN"] = auth_bundle["X_CSRF_TOKEN"]
            self.session.headers["X-XSRF-TOKEN"] = auth_bundle["X_CSRF_TOKEN"]

    def fetch_payload(self, category_slug: str, snapshot_date: date, oblast_slug: str) -> list[dict[str, Any]]:
        params = {"category": category_slug, "to": snapshot_date.strftime("%d.%m.%Y")}
        if oblast_slug:
            params["oblast"] = oblast_slug

        delay = self.sleep_seconds
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(API_URL, params=params, timeout=45)
                content_type = response.headers.get("Content-Type", "")
                body_head = response.text[:500]
                if response.status_code in {403, 429, 503} and (
                    "Just a moment..." in body_head
                    or "cf-mitigated" in content_type.lower()
                    or "challenge-platform" in body_head
                ):
                    snippet = response.text[:160].replace("\n", " ")
                    raise AuthRefreshRequired(
                        f"Cloudflare challenged the API for {snapshot_date.isoformat()} | "
                        f"{category_slug} | {oblast_slug or 'country'}: HTTP {response.status_code} at "
                        f"{response.url}. Snippet: {snippet}"
                    )
                response.raise_for_status()
                payload = response.json()
                time.sleep(delay + random.uniform(0.05, 0.35))
                return payload
            except AuthRefreshRequired:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(delay)
                delay *= 2
        raise WorkflowError(
            f"Minagro request failed for {snapshot_date.isoformat()} | {category_slug} | {oblast_slug or 'country'}: {last_error}"
        )


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_api_payload(
    payload: list[dict[str, Any]],
    snapshot_date: date,
    category_slug: str,
    oblast_slug: str,
    source_path: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    category = category_for_slug(category_slug)
    oblast_info = OBLAST_BY_SLUG.get(oblast_slug, {"slug": "", "uk": "Вся Україна", "en": "Ukraine"})

    for crop_blob in payload:
        crop_slug = str(crop_blob.get("slug", "") or "")
        crop_uk = str(crop_blob.get("title", "") or "")
        crop_slug, crop_en = normalize_crop(crop_uk, crop_slug)

        for metric_blob in crop_blob.get("data", []):
            metric_uk = str(metric_blob.get("name", "") or "")
            metric_key, metric_en = normalize_metric(metric_uk)
            unit_uk = str(metric_blob.get("unit", "") or "")
            unit_en = normalize_unit(unit_uk)
            value = float(metric_blob.get("value", 0))

            rows.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "category_slug": category.slug,
                    "category_en": category.label_en,
                    "category_uk": category.label_uk,
                    "campaign_year": compute_campaign_year(snapshot_date, category.slug, crop_slug),
                    "campaign_week": compute_campaign_week(snapshot_date, category.slug, crop_slug),
                    "season_label": compute_season_label(snapshot_date, category.slug, crop_slug),
                    "geography_level": "national" if not oblast_slug else "oblast",
                    "oblast_slug": oblast_info["slug"],
                    "oblast_en": oblast_info["en"],
                    "oblast_uk": oblast_info["uk"],
                    "crop_slug": crop_slug,
                    "crop_en": crop_en,
                    "crop_uk": crop_uk,
                    "item_key": metric_key,
                    "item_en": metric_en,
                    "item_uk": metric_uk,
                    "metric_key": metric_key,
                    "metric_en": metric_en,
                    "metric_uk": metric_uk,
                    "value": value,
                    "unit_uk": unit_uk,
                    "unit_en": unit_en,
                    "source_kind": "minagro_api",
                    "source_path": source_path,
                    "notes": "",
                }
            )
    return rows


def append_derived_condition_index(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base_rows = list(rows)
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for row in rows:
        if row["category_slug"] != "stan-posiviv-ozimih":
            continue
        metric_key = row["metric_key"]
        if metric_key not in CONDITION_WEIGHTS:
            continue

        key = (
            row["snapshot_date"],
            row["oblast_slug"],
            row["crop_slug"],
            row["category_slug"],
        )
        bucket = groups.setdefault(
            key,
            {
                "row": row,
                "weighted_sum": 0.0,
                "observed_share": 0.0,
            },
        )
        weight = CONDITION_WEIGHTS[metric_key]
        bucket["weighted_sum"] += float(row["value"]) * weight
        bucket["observed_share"] += float(row["value"])

    for bucket in groups.values():
        if bucket["observed_share"] <= 0:
            continue
        template_row = dict(bucket["row"])
        template_row["item_key"] = "condition_index_100"
        template_row["item_en"] = "Crop condition index (derived)"
        template_row["item_uk"] = "Індекс стану посівів (розрахунковий)"
        template_row["metric_key"] = "condition_index_100"
        template_row["metric_en"] = "Crop condition index (derived)"
        template_row["metric_uk"] = "Індекс стану посівів (розрахунковий)"
        template_row["value"] = round((bucket["weighted_sum"] / bucket["observed_share"]) * 100, 4)
        template_row["unit_uk"] = "індекс"
        template_row["unit_en"] = "Index points"
        template_row["notes"] = (
            "Derived from condition-share rows using weights: good/excellent=1.0, "
            "satisfactory=0.6, weak or sparse=0.2, lost/no emergence=0.0."
        )
        base_rows.append(template_row)

    return base_rows


def load_processed_dataframe() -> pd.DataFrame:
    if not PROCESSED_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(PROCESSED_CSV, low_memory=False, dtype={"season_label": "string"})
    if df.empty:
        return df
    return normalize_processed_dataframe(df)


def normalize_processed_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    normalized = df.copy()

    text_columns = [
        "snapshot_date",
        "category_slug",
        "category_en",
        "category_uk",
        "geography_level",
        "oblast_slug",
        "oblast_en",
        "oblast_uk",
        "crop_slug",
        "crop_en",
        "crop_uk",
        "item_key",
        "item_en",
        "item_uk",
        "metric_key",
        "metric_en",
        "metric_uk",
        "unit_uk",
        "unit_en",
        "source_kind",
        "source_path",
        "notes",
        "season_label",
    ]
    for column in text_columns:
        if column in normalized.columns:
            normalized[column] = normalized[column].fillna("").astype(str)

    if "category_slug" in normalized.columns:
        for old_slug, (new_slug, new_uk, new_en) in LEGACY_CATEGORY_ALIASES.items():
            mask = normalized["category_slug"].eq(old_slug)
            if not mask.any():
                continue
            normalized.loc[mask, "category_slug"] = new_slug
            if "category_uk" in normalized.columns:
                normalized.loc[mask, "category_uk"] = new_uk
            if "category_en" in normalized.columns:
                normalized.loc[mask, "category_en"] = new_en

    if {"crop_uk", "crop_en", "crop_slug"}.issubset(normalized.columns):
        needs_crop_translation = normalized["crop_en"].str.contains(r"[А-Яа-яІіЇїЄєҐґ]", na=False) | normalized["crop_en"].eq("")
        if needs_crop_translation.any():
            translated = normalized.loc[needs_crop_translation, ["crop_uk", "crop_slug"]].apply(
                lambda row: normalize_crop(str(row["crop_uk"]), str(row["crop_slug"])),
                axis=1,
                result_type="expand",
            )
            translated.columns = ["crop_slug", "crop_en"]
            normalized.loc[needs_crop_translation, "crop_slug"] = translated["crop_slug"].values
            normalized.loc[needs_crop_translation, "crop_en"] = translated["crop_en"].values

    if {"snapshot_date", "category_slug", "crop_slug"}.issubset(normalized.columns):
        parsed_snapshot_dates = pd.to_datetime(normalized["snapshot_date"], errors="coerce")
        campaign_years: list[int | None] = []
        campaign_weeks: list[int | None] = []
        season_labels: list[str] = []

        for snapshot_value, category_slug, crop_slug in zip(
            parsed_snapshot_dates,
            normalized["category_slug"].astype(str),
            normalized["crop_slug"].astype(str),
        ):
            if pd.isna(snapshot_value) or not category_slug:
                campaign_years.append(None)
                campaign_weeks.append(None)
                season_labels.append("")
                continue
            snapshot_day = snapshot_value.date()
            campaign_years.append(compute_campaign_year(snapshot_day, category_slug, crop_slug))
            campaign_weeks.append(compute_campaign_week(snapshot_day, category_slug, crop_slug))
            season_labels.append(compute_season_label(snapshot_day, category_slug, crop_slug))

        normalized["campaign_year"] = pd.Series(campaign_years, index=normalized.index, dtype="Int64")
        normalized["campaign_week"] = pd.Series(campaign_weeks, index=normalized.index, dtype="Int64")
        normalized["season_label"] = season_labels

    return normalized


def persist_processed_rows(new_rows: list[dict[str, Any]]) -> pd.DataFrame:
    new_df = normalize_processed_dataframe(pd.DataFrame(new_rows)) if new_rows else pd.DataFrame()
    current_df = load_processed_dataframe()
    if current_df.empty:
        combined = new_df
    elif new_df.empty:
        combined = current_df
    else:
        combined = pd.concat([current_df, new_df], ignore_index=True)

    if not combined.empty:
        combined["_source_priority"] = combined["source_kind"].map({"minagro_api": 0, "public_mirror_seed": 1}).fillna(9)
        combined = combined.sort_values(
            by=[
                "snapshot_date",
                "category_slug",
                "oblast_slug",
                "crop_slug",
                "metric_key",
                "_source_priority",
            ],
            ascending=[True, True, True, True, True, True],
        )
        combined = combined.drop_duplicates(
            subset=[
                "snapshot_date",
                "category_slug",
                "oblast_slug",
                "crop_slug",
                "metric_key",
            ]
        ).sort_values(
            by=["snapshot_date", "category_slug", "oblast_slug", "crop_en", "item_en"],
            ascending=[True, True, True, True, True],
        )
        combined = combined.drop(columns=["_source_priority"])
    combined.to_csv(PROCESSED_CSV, index=False, encoding="utf-8-sig")
    return combined


def processed_api_request_paths(df: pd.DataFrame) -> set[str]:
    if df.empty or "source_kind" not in df.columns or "source_path" not in df.columns:
        return set()

    mask = df["source_kind"].fillna("").eq("minagro_api") & df["source_path"].notna()
    return {
        normalize_source_path_text(source_path)
        for source_path in df.loc[mask, "source_path"].astype(str).tolist()
        if str(source_path).strip()
    }


def processed_api_dates(df: pd.DataFrame) -> list[date]:
    if df.empty or "source_kind" not in df.columns or "snapshot_date" not in df.columns:
        return []

    mask = df["source_kind"].fillna("").eq("minagro_api") & df["snapshot_date"].notna()
    raw_dates = sorted({str(value).strip() for value in df.loc[mask, "snapshot_date"].tolist() if str(value).strip()})
    return [parse_date(value) for value in raw_dates]


def cache_payload_has_rows(cache_file: Path) -> bool:
    payload = read_json(cache_file)
    if isinstance(payload, list):
        return bool(payload)
    return bool(payload)


def analyze_sync_plan(
    current_df: pd.DataFrame,
    start_date: date,
    end_date: date,
    weekday_code: str,
    limit_oblasts: Optional[int],
) -> SyncPlan:
    expected_dates = build_anchor_weekly_dates(start_date, end_date, weekday_code)
    request_specs = build_request_specs(expected_dates, limit_oblasts)
    api_paths = processed_api_request_paths(current_df)
    api_dates = processed_api_dates(current_df)
    latest_api_date = max(api_dates) if api_dates else None

    missing_dates: set[date] = set()
    missing_request_count = 0
    cached_reprocess_request_count = 0
    cached_empty_request_count = 0

    for spec in request_specs:
        if spec.source_path in api_paths:
            continue

        if spec.cache_file.exists():
            try:
                if not cache_payload_has_rows(spec.cache_file):
                    cached_empty_request_count += 1
                    continue
                cached_reprocess_request_count += 1
            except Exception:
                pass

        missing_dates.add(spec.snapshot_date)
        missing_request_count += 1

    ordered_missing_dates = sorted(missing_dates)
    historical_gap_dates = [item for item in ordered_missing_dates if latest_api_date is not None and item < latest_api_date]
    new_gap_dates = [item for item in ordered_missing_dates if latest_api_date is None or item >= latest_api_date]

    return SyncPlan(
        start_date=start_date,
        end_date=end_date,
        expected_dates=expected_dates,
        missing_dates=ordered_missing_dates,
        missing_request_count=missing_request_count,
        expected_request_count=len(request_specs),
        historical_gap_dates=historical_gap_dates,
        new_gap_dates=new_gap_dates,
        latest_api_date=latest_api_date,
        cached_reprocess_request_count=cached_reprocess_request_count,
        cached_empty_request_count=cached_empty_request_count,
    )


def finalize_outputs(current_df: pd.DataFrame, include_public_seed: bool) -> pd.DataFrame:
    combined = current_df
    if include_public_seed:
        combined = persist_processed_rows(PUBLIC_MIRROR_SEED)
    write_translation_lookup(combined if not combined.empty else None)
    write_data_dictionary()
    write_public_seed_file()
    return combined


def fetch_selected_dates(
    auth_bundle: dict[str, str],
    dates: Iterable[date],
    sleep_seconds: float,
    retries: int,
    include_public_seed: bool,
    limit_oblasts: Optional[int],
    auth_file: Optional[Path] = None,
    allow_browser_capture: bool = True,
    auth_timeout_seconds: int = DEFAULT_BROWSER_AUTH_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    ordered_dates = sorted(set(dates))
    if not ordered_dates:
        return finalize_outputs(load_processed_dataframe(), include_public_seed=include_public_seed)

    current_auth_bundle = auth_bundle
    fetcher = PoliteMinagroFetcher(current_auth_bundle, sleep_seconds=sleep_seconds, retries=retries)
    oblasts = scoped_oblasts(limit_oblasts)

    current_df = load_processed_dataframe()
    max_calls = len(ordered_dates) * len(CATEGORIES) * (len(oblasts) + 1)
    completed = 0
    log(
        f"Maximum planned requests: {max_calls} "
        f"(weekly dates={len(ordered_dates)}, categories={len(CATEGORIES)}, oblasts={len(oblasts)})."
    )
    log("That number is an upper bound. The actual web-request count drops whenever the national slice is empty, because oblast calls are skipped for that date/category.")
    log("The workflow now requests the national slice first and skips all oblast calls for that date/category when the national payload is empty.")
    log("Request pacing is intentionally conservative to stay gentle with the ministry server.")

    def refresh_fetcher() -> None:
        nonlocal current_auth_bundle, fetcher

        log("Saved auth stopped working during the fetch. Refreshing auth and retrying the blocked request.")
        current_auth_bundle = ensure_auth_bundle(
            auth_file=auth_file or DEFAULT_AUTH_FILE,
            allow_browser_capture=allow_browser_capture,
            timeout_seconds=auth_timeout_seconds,
        )
        fetcher = PoliteMinagroFetcher(current_auth_bundle, sleep_seconds=sleep_seconds, retries=retries)

    def load_or_fetch_payload(snapshot_date: date, category_slug: str, oblast_slug: str) -> tuple[list[dict[str, Any]], str]:
        nonlocal completed

        cache_file = raw_cache_path(snapshot_date, category_slug, oblast_slug)
        source_path = normalize_source_path_text(cache_file.relative_to(PROJECT_ROOT))
        completed += 1

        if cache_file.exists():
            try:
                payload = read_json(cache_file)
                if not isinstance(payload, list):
                    raise WorkflowError("Cached payload is not a JSON list.")
                log(f"[{completed}] cache hit  {snapshot_date} | {category_slug} | {oblast_slug or 'country'}")
                return payload, source_path
            except Exception as exc:  # noqa: BLE001
                log(
                    f"[{completed}] refetching {snapshot_date} | {category_slug} | "
                    f"{oblast_slug or 'country'} because the cache could not be reused ({exc})"
                )

        for auth_attempt in range(2):
            log(f"[{completed}] fetching   {snapshot_date} | {category_slug} | {oblast_slug or 'country'}")
            try:
                payload = fetcher.fetch_payload(category_slug, snapshot_date, oblast_slug)
                write_json(cache_file, payload)
                return payload, source_path
            except AuthRefreshRequired as exc:
                if auth_attempt == 0:
                    log(str(exc))
                    refresh_fetcher()
                    continue
                raise WorkflowError(str(exc)) from exc

        raise WorkflowError(
            f"Could not fetch {snapshot_date.isoformat()} | {category_slug} | {oblast_slug or 'country'} "
            "after refreshing auth."
        )

    for snapshot_date in ordered_dates:
        date_rows: list[dict[str, Any]] = []
        for category in CATEGORIES:
            country_payload, country_source_path = load_or_fetch_payload(snapshot_date, category.slug, "")
            date_rows.extend(
                normalize_api_payload(
                    payload=country_payload,
                    snapshot_date=snapshot_date,
                    category_slug=category.slug,
                    oblast_slug="",
                    source_path=country_source_path,
                )
            )

            if not country_payload:
                log(
                    f"National payload was empty for {snapshot_date} | {category.slug}. "
                    f"Skipping {len(oblasts)} oblast requests for that slice."
                )
                continue

            for oblast in oblasts:
                oblast_slug = oblast["slug"]
                payload, source_path = load_or_fetch_payload(snapshot_date, category.slug, oblast_slug)
                date_rows.extend(
                    normalize_api_payload(
                        payload=payload,
                        snapshot_date=snapshot_date,
                        category_slug=category.slug,
                        oblast_slug=oblast_slug,
                        source_path=source_path,
                    )
                )

        if date_rows:
            current_df = persist_processed_rows(append_derived_condition_index(date_rows))
            log(f"Persisted processed rows for {snapshot_date}. Current CSV row count: {len(current_df):,}")

    return finalize_outputs(current_df, include_public_seed=include_public_seed)


def fetch_backfill(
    auth_bundle: dict[str, str],
    start_date: date,
    end_date: date,
    weekday_code: str,
    sleep_seconds: float,
    retries: int,
    include_public_seed: bool,
    limit_oblasts: Optional[int],
    auth_file: Optional[Path] = None,
    allow_browser_capture: bool = True,
    auth_timeout_seconds: int = DEFAULT_BROWSER_AUTH_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    dates = build_weekly_dates(start_date, end_date, weekday_code)
    return fetch_selected_dates(
        auth_bundle=auth_bundle,
        dates=dates,
        sleep_seconds=sleep_seconds,
        retries=retries,
        include_public_seed=include_public_seed,
        limit_oblasts=limit_oblasts,
        auth_file=auth_file,
        allow_browser_capture=allow_browser_capture,
        auth_timeout_seconds=auth_timeout_seconds,
    )


def run_update(
    auth_bundle: dict[str, str],
    until_date: date,
    weekday_code: str,
    sleep_seconds: float,
    retries: int,
    limit_oblasts: Optional[int],
    auth_file: Optional[Path] = None,
    allow_browser_capture: bool = True,
    auth_timeout_seconds: int = DEFAULT_BROWSER_AUTH_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    current_df = load_processed_dataframe()
    if current_df.empty:
        raise WorkflowError("No existing processed CSV found. Run `fetch` first.")

    last_snapshot = pd.to_datetime(current_df["snapshot_date"]).max().date()
    next_start = last_snapshot + pd.Timedelta(days=7)
    next_start = next_start.date() if isinstance(next_start, pd.Timestamp) else next_start
    if next_start > until_date:
        log(f"No update needed. Latest snapshot in CSV is already {last_snapshot.isoformat()}.")
        write_translation_lookup(current_df)
        write_data_dictionary()
        return current_df

    return fetch_backfill(
        auth_bundle=auth_bundle,
        start_date=next_start,
        end_date=until_date,
        weekday_code=weekday_code,
        sleep_seconds=sleep_seconds,
        retries=retries,
        include_public_seed=False,
        limit_oblasts=limit_oblasts,
        auth_file=auth_file,
        allow_browser_capture=allow_browser_capture,
        auth_timeout_seconds=auth_timeout_seconds,
    )


def run_sync_missing(
    auth_bundle: Optional[dict[str, str]],
    start_date: date,
    end_date: date,
    weekday_code: str,
    sleep_seconds: float,
    retries: int,
    include_public_seed: bool,
    limit_oblasts: Optional[int],
    auth_file: Optional[Path] = None,
    allow_browser_capture: bool = True,
    auth_timeout_seconds: int = DEFAULT_BROWSER_AUTH_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    current_df = load_processed_dataframe()
    plan = analyze_sync_plan(
        current_df=current_df,
        start_date=start_date,
        end_date=end_date,
        weekday_code=weekday_code,
        limit_oblasts=limit_oblasts,
    )

    log(
        f"Gap scan checked {len(plan.expected_dates)} weekly dates from "
        f"{plan.start_date.isoformat()} to {plan.end_date.isoformat()}."
    )
    if not plan.expected_dates:
        log("No scheduled weekly snapshot dates fall inside that window yet.")
        return finalize_outputs(current_df, include_public_seed=include_public_seed)
    if plan.latest_api_date is None:
        log("No official Minagro API weeks are in the processed CSV yet.")
    else:
        log(f"Latest official Minagro week already in the CSV: {plan.latest_api_date.isoformat()}.")

    if not plan.missing_dates:
        log("No missing weekly gaps were found. The CSV already covers the requested history window.")
        return finalize_outputs(current_df, include_public_seed=include_public_seed)

    log(
        f"Missing weekly dates to fill: {len(plan.missing_dates)} "
        f"({len(plan.historical_gap_dates)} historical gaps, {len(plan.new_gap_dates)} current or recent weeks)."
    )
    log(
        f"Potential uncached request slots before runtime pruning: "
        f"{plan.missing_request_count} out of {plan.expected_request_count}."
    )
    if plan.cached_reprocess_request_count:
        log(
            f"{plan.cached_reprocess_request_count} missing request slots already have raw cache files "
            "and will be rebuilt from disk before any new web requests are needed."
        )
    if plan.cached_empty_request_count:
        log(
            f"{plan.cached_empty_request_count} request slots have cached empty official responses "
            "and will be left as complete."
        )

    preview = ", ".join(item.isoformat() for item in plan.missing_dates[:12])
    if preview:
        suffix = " ..." if len(plan.missing_dates) > 12 else ""
        log(f"Weekly dates queued: {preview}{suffix}")

    if auth_bundle is None:
        auth_bundle = ensure_auth_bundle(
            auth_file=auth_file or DEFAULT_AUTH_FILE,
            allow_browser_capture=allow_browser_capture,
            timeout_seconds=auth_timeout_seconds,
        )

    return fetch_selected_dates(
        auth_bundle=auth_bundle,
        dates=plan.missing_dates,
        sleep_seconds=sleep_seconds,
        retries=retries,
        include_public_seed=include_public_seed,
        limit_oblasts=limit_oblasts,
        auth_file=auth_file,
        allow_browser_capture=allow_browser_capture,
        auth_timeout_seconds=auth_timeout_seconds,
    )


def compact_dashboard_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    keep_columns = [
        "snapshot_date",
        "category_slug",
        "category_en",
        "category_uk",
        "campaign_year",
        "campaign_week",
        "season_label",
        "geography_level",
        "oblast_slug",
        "oblast_en",
        "oblast_uk",
        "crop_slug",
        "crop_en",
        "crop_uk",
        "item_key",
        "item_en",
        "item_uk",
        "metric_key",
        "metric_en",
        "metric_uk",
        "value",
        "unit_en",
        "unit_uk",
        "source_kind",
        "notes",
    ]
    prepared = df[keep_columns].copy()
    prepared["value"] = prepared["value"].astype(float)
    return prepared.to_dict("records")


def executive_dashboard_template() -> Template:
    if not EXECUTIVE_TEMPLATE_FILE.exists():
        raise WorkflowError(f"Executive dashboard template not found: {EXECUTIVE_TEMPLATE_FILE}")
    return Template(EXECUTIVE_TEMPLATE_FILE.read_text(encoding="utf-8"))


def load_dashboard_map_svg() -> str:
    if UKRAINE_MAP_SVG.exists():
        return UKRAINE_MAP_SVG.read_text(encoding="utf-8")
    return (
        '<svg viewBox="0 0 800 520" aria-label="Ukraine oblast map placeholder">'
        '<rect x="12" y="12" width="776" height="496" rx="24" fill="#f4eee3" stroke="#d5c8b3" stroke-dasharray="10 10"/>'
        '<text x="400" y="245" text-anchor="middle" font-size="28" fill="#6d675d" font-family="Aptos, Segoe UI, sans-serif">'
        'Ukraine oblast map asset is not available yet'
        '</text>'
        '<text x="400" y="285" text-anchor="middle" font-size="16" fill="#8a8378" font-family="Aptos, Segoe UI, sans-serif">'
        'Place data/reference/ukraine_oblast_map.svg in the project to enable the interactive map'
        "</text>"
        "</svg>"
    )


def write_dashboard_files(df: pd.DataFrame) -> None:
    df = normalize_processed_dataframe(df)
    if df.empty:
        raise WorkflowError("The processed CSV is empty. Fetch data before building the dashboards.")

    plotly_js = plotly.offline.get_plotlyjs()
    rows_json = json.dumps(compact_dashboard_rows(df), ensure_ascii=False)
    lookup_rows = build_translation_lookup_rows(df)
    lookup_json = json.dumps(lookup_rows, ensure_ascii=False)
    map_slug_json = json.dumps(MAP_SLUG_BY_OBLAST_SLUG, ensure_ascii=False)
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    map_svg_markup = load_dashboard_map_svg()

    executive_template = executive_dashboard_template()
    executive_html = executive_template.render(
        title="Ukraine Weekly Crop Dashboard",
        subtitle="Unified standalone dashboard for Ukraine weekly crop progress with a national-first view, clickable oblast drilldown, and season-over-season pace comparisons.",
        note="All on-screen labels remain English-first. The Ukrainian source labels stay available in the tooltips, detail panels, and translation reference whenever you need to validate terminology.",
        row_count=len(df),
        generated_at=generated_at,
        plotly_js=plotly_js,
        rows_json=rows_json,
        lookup_json=lookup_json,
        map_slug_json=map_slug_json,
        map_svg_markup=map_svg_markup,
    )
    EXECUTIVE_HTML.write_text(executive_html, encoding="utf-8")


def write_reference_assets() -> None:
    ensure_directories()
    current_df = load_processed_dataframe()
    write_translation_lookup(current_df if not current_df.empty else None)
    write_data_dictionary()
    write_public_seed_file()


def add_browser_auth_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--auth-file", default=DEFAULT_AUTH_FILE_ARG, help="Path to the saved auth bundle.")
    parser.add_argument(
        "--no-browser-auth",
        action="store_true",
        help="Do not open a browser automatically when the saved auth bundle is missing or expired.",
    )
    parser.add_argument(
        "--auth-timeout-seconds",
        type=int,
        default=DEFAULT_BROWSER_AUTH_TIMEOUT_SECONDS,
        help="How long to wait for manual Cloudflare/browser completion when capturing auth automatically.",
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Polite weekly crop progress workflow for Ukraine Minagro data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch", help="Backfill weekly dates and update the processed CSV.")
    add_browser_auth_args(fetch)
    fetch.add_argument("--from", dest="start_date", required=True, help="Start date in YYYY-MM-DD.")
    fetch.add_argument("--to", dest="end_date", required=True, help="End date in YYYY-MM-DD.")
    fetch.add_argument("--weekday", default=DEFAULT_WEEKDAY, help="Weekly anchor day, e.g. FRI.")
    fetch.add_argument("--sleep-seconds", type=float, default=1.5, help="Base delay between requests.")
    fetch.add_argument("--retries", type=int, default=3, help="Retry count per request.")
    fetch.add_argument("--no-public-seed", action="store_true", help="Do not append the small public mirror seed rows.")
    fetch.add_argument("--limit-oblasts", type=int, default=None, help="Optional test limit for oblast calls.")

    update = subparsers.add_parser("update", help="Fetch weekly dates after the latest snapshot already in the processed CSV.")
    add_browser_auth_args(update)
    update.add_argument("--to", dest="until_date", default=date.today().isoformat(), help="Update until YYYY-MM-DD.")
    update.add_argument("--weekday", default=DEFAULT_WEEKDAY, help="Weekly anchor day, e.g. FRI.")
    update.add_argument("--sleep-seconds", type=float, default=1.5, help="Base delay between requests.")
    update.add_argument("--retries", type=int, default=3, help="Retry count per request.")
    update.add_argument("--limit-oblasts", type=int, default=None, help="Optional test limit for oblast calls.")

    build = subparsers.add_parser("build-views", help="Generate standalone national and oblast HTML dashboards from the processed CSV.")
    build.add_argument("--csv", default=DEFAULT_PROCESSED_CSV_ARG, help="Processed CSV path.")

    refresh = subparsers.add_parser("refresh", help="Run `update` and then rebuild the HTML dashboards.")
    add_browser_auth_args(refresh)
    refresh.add_argument("--to", dest="until_date", default=date.today().isoformat(), help="Update until YYYY-MM-DD.")
    refresh.add_argument("--weekday", default=DEFAULT_WEEKDAY, help="Weekly anchor day, e.g. FRI.")
    refresh.add_argument("--sleep-seconds", type=float, default=1.5, help="Base delay between requests.")
    refresh.add_argument("--retries", type=int, default=3, help="Retry count per request.")
    refresh.add_argument("--limit-oblasts", type=int, default=None, help="Optional test limit for oblast calls.")

    capture = subparsers.add_parser("capture-auth", help="Open a browser, let the user solve Cloudflare if needed, then save the fresh auth cookies automatically.")
    add_browser_auth_args(capture)

    historical = subparsers.add_parser(
        "historical",
        help="One-command historical sync: inspect a date range, fill only missing weekly gaps, then rebuild dashboards.",
    )
    add_browser_auth_args(historical)
    historical.add_argument("--from", dest="start_date", required=True, help="Start date in YYYY-MM-DD.")
    historical.add_argument("--to", dest="end_date", required=True, help="End date in YYYY-MM-DD.")
    historical.add_argument("--weekday", default=DEFAULT_WEEKDAY, help="Weekly anchor day, e.g. FRI.")
    historical.add_argument("--sleep-seconds", type=float, default=1.5, help="Base delay between requests.")
    historical.add_argument("--retries", type=int, default=3, help="Retry count per request.")
    historical.add_argument("--no-public-seed", action="store_true", help="Do not append the small public mirror seed rows.")
    historical.add_argument("--limit-oblasts", type=int, default=None, help="Optional test limit for oblast calls.")

    weekly = subparsers.add_parser(
        "weekly-update",
        help="One-command smart sync: scan the history window, fill missing historical and recent weeks, then rebuild dashboards.",
    )
    add_browser_auth_args(weekly)
    weekly.add_argument(
        "--history-from",
        dest="history_start",
        default=DEFAULT_HISTORY_START_ARG,
        help=f"Start date for gap scanning in YYYY-MM-DD. Default: {DEFAULT_HISTORY_START_ARG}.",
    )
    weekly.add_argument("--to", dest="until_date", default=date.today().isoformat(), help="Update until YYYY-MM-DD.")
    weekly.add_argument("--weekday", default=DEFAULT_WEEKDAY, help="Weekly anchor day, e.g. FRI.")
    weekly.add_argument("--sleep-seconds", type=float, default=1.5, help="Base delay between requests.")
    weekly.add_argument("--retries", type=int, default=3, help="Retry count per request.")
    weekly.add_argument("--limit-oblasts", type=int, default=None, help="Optional test limit for oblast calls.")

    refs = subparsers.add_parser("write-reference", help="Write translation lookup, public seed, and data dictionary files.")
    subparsers.add_parser("seed-public", help="Write the small public-mirror seed into the processed CSV for bootstrap/testing.")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    ensure_directories()
    init_run_logging()
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        auth_bundle: Optional[dict[str, str]] = None
        if args.command in {"fetch", "update", "refresh", "capture-auth"}:
            auth_bundle = ensure_auth_bundle(
                auth_file=Path(args.auth_file),
                allow_browser_capture=not args.no_browser_auth,
                timeout_seconds=args.auth_timeout_seconds,
            )

        if args.command == "fetch":
            df = fetch_backfill(
                auth_bundle=auth_bundle,
                start_date=parse_date(args.start_date),
                end_date=parse_date(args.end_date),
                weekday_code=parse_weekday(args.weekday),
                sleep_seconds=args.sleep_seconds,
                retries=args.retries,
                include_public_seed=not args.no_public_seed,
                limit_oblasts=args.limit_oblasts,
                auth_file=Path(args.auth_file),
                allow_browser_capture=not args.no_browser_auth,
                auth_timeout_seconds=args.auth_timeout_seconds,
            )
            log(f"Wrote processed CSV: {display_path(PROCESSED_CSV)}")
            log(f"Current row count: {len(df):,}")
            return 0

        if args.command == "update":
            df = run_update(
                auth_bundle=auth_bundle,
                until_date=parse_date(args.until_date),
                weekday_code=parse_weekday(args.weekday),
                sleep_seconds=args.sleep_seconds,
                retries=args.retries,
                limit_oblasts=args.limit_oblasts,
                auth_file=Path(args.auth_file),
                allow_browser_capture=not args.no_browser_auth,
                auth_timeout_seconds=args.auth_timeout_seconds,
            )
            log(f"Wrote processed CSV: {display_path(PROCESSED_CSV)}")
            log(f"Current row count: {len(df):,}")
            return 0

        if args.command == "build-views":
            csv_path = Path(args.csv)
            if not csv_path.exists():
                raise WorkflowError(f"CSV not found: {csv_path}")
            df = normalize_processed_dataframe(pd.read_csv(csv_path, low_memory=False, dtype={"season_label": "string"}))
            write_dashboard_files(df)
            write_translation_lookup(df)
            write_data_dictionary()
            log(f"Wrote standalone HTML view: {EXECUTIVE_HTML.name}")
            return 0

        if args.command == "refresh":
            df = run_update(
                auth_bundle=auth_bundle,
                until_date=parse_date(args.until_date),
                weekday_code=parse_weekday(args.weekday),
                sleep_seconds=args.sleep_seconds,
                retries=args.retries,
                limit_oblasts=args.limit_oblasts,
                auth_file=Path(args.auth_file),
                allow_browser_capture=not args.no_browser_auth,
                auth_timeout_seconds=args.auth_timeout_seconds,
            )
            write_dashboard_files(df)
            write_translation_lookup(df)
            write_data_dictionary()
            log(f"Refreshed CSV and rebuilt HTML views in {display_path(VIEWS_DIR)}")
            return 0

        if args.command == "capture-auth":
            auth_path = Path(args.auth_file)
            if not auth_path.is_absolute():
                auth_path = PROJECT_ROOT / auth_path
            log(f"Auth is ready and saved at {display_path(auth_path)}")
            return 0

        if args.command == "historical":
            df = run_sync_missing(
                auth_bundle=None,
                start_date=parse_date(args.start_date),
                end_date=parse_date(args.end_date),
                weekday_code=parse_weekday(args.weekday),
                sleep_seconds=args.sleep_seconds,
                retries=args.retries,
                include_public_seed=not args.no_public_seed,
                limit_oblasts=args.limit_oblasts,
                auth_file=Path(args.auth_file),
                allow_browser_capture=not args.no_browser_auth,
                auth_timeout_seconds=args.auth_timeout_seconds,
            )
            write_dashboard_files(df)
            write_translation_lookup(df)
            write_data_dictionary()
            log(f"Historical gap sync finished. CSV: {display_path(PROCESSED_CSV)}")
            log(f"Standalone views rebuilt in {display_path(VIEWS_DIR)}")
            return 0

        if args.command == "weekly-update":
            df = run_sync_missing(
                auth_bundle=None,
                start_date=parse_date(args.history_start),
                end_date=parse_date(args.until_date),
                weekday_code=parse_weekday(args.weekday),
                sleep_seconds=args.sleep_seconds,
                retries=args.retries,
                include_public_seed=False,
                limit_oblasts=args.limit_oblasts,
                auth_file=Path(args.auth_file),
                allow_browser_capture=not args.no_browser_auth,
                auth_timeout_seconds=args.auth_timeout_seconds,
            )
            write_dashboard_files(df)
            write_translation_lookup(df)
            write_data_dictionary()
            log(f"Weekly smart sync finished. CSV: {display_path(PROCESSED_CSV)}")
            log(f"Standalone views rebuilt in {display_path(VIEWS_DIR)}")
            return 0

        if args.command == "write-reference":
            write_reference_assets()
            log(f"Wrote reference files to {display_path(REFERENCE_DIR)}")
            return 0

        if args.command == "seed-public":
            combined = persist_processed_rows(PUBLIC_MIRROR_SEED)
            write_translation_lookup(combined)
            write_data_dictionary()
            write_public_seed_file()
            log(f"Wrote public mirror seed rows into {display_path(PROCESSED_CSV)}")
            return 0
    except WorkflowError as exc:
        log(f"WorkflowError: {exc}")
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        log(f"Unexpected error: {exc}")
        log(traceback.format_exc())
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

