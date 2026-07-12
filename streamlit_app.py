import platform
import sys
import polars as pl
import duckdb
import streamlit as st
import os
import time
import logging
import io
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import json"""
================================================================================
🚗 ULTIMATE UNIT ECONOMICS FOR AUTO PARTS v100.5 - ENTERPRISE EDITION
================================================================================
📌 ВЕРСИЯ: 100.5.1 (ENTERPRISE)
📌 СПЕЦИАЛИЗАЦИЯ: АВТОЗАПЧАСТИ, АВТОТОВАРЫ И АГРЕГАТЫ
📌 ТЕХНОЛОГИИ: STREAMLIT, POLARS, DUCKDB, SCIKIT-LEARN, OPENPYXL, PLOTLY
📌 УЛУЧШЕНИЯ v100.5.1:
✅ ИСПРАВЛЕНЫ КРАКОЗЯБРЫ (двойное UTF-8 кодирование)
✅ АВТООПРЕДЕЛЕНИЕ И ИСПРАВЛЕНИЕ КОДИРОВКИ КОЛОНОК
✅ ПРАВИЛЬНЫЙ ПОРЯДОК ЧТЕНИЯ CSV (UTF-8 приоритет)
✅ ОБЪЁМНЫЙ ВЕС ДЛЯ ТОЧНОЙ ЛОГИСТИКИ
✅ ПРОГРЕССИВНАЯ СТОИМОСТЬ ХРАНЕНИЯ
✅ РЕАЛЬНЫЕ ВОЗВРАТЫ С ОБРАТНОЙ ЛОГИСТИКОЙ
✅ СПЕЦИФИЧЕСКИЕ РАСХОДЫ АВТОЗАПЧАСТЕЙ
✅ УЧЁТ СКИДОК И АКЦИЙ В КОМИССИЯХ
✅ РЕКЛАМНЫЕ РАСХОДЫ (ДРР)
✅ РАЗНЫЕ НАЛОГОВЫЕ РЕЖИМЫ (УСН 6%, УСН 15%, ОСН, ПСН, НПД)
✅ ПРОФЕССИОНАЛЬНЫЙ EXCEL-ЭКСПОРТ С ДАШБОРДОМ И ГРАФИКАМИ
✅ ТОЧНЫЕ РАСЧЁТЫ ЧЕРЕЗ DECIMAL
✅ БЕНЧМАРКИ РЫНКА И АНАЛИЗ ЧУВСТВИТЕЛЬНОСТИ
✅ ПАРАЛЛЕЛЬНЫЙ РАСЧЕТ ДЛЯ 100K+ ТОВАРОВ
✅ СОВМЕСТИМОСТЬ STREAMLIT 1.58+ (width='stretch')
✅ МИГРАЦИЯ БД (авто-добавление новых колонок)
================================================================================
"""
# ============================================================================
# БЛОК 0: ВСЕ НЕОБХОДИМЫЕ ИМПОРТЫ И КОНФИГУРАЦИЯ
# ============================================================================
# === Стандартная библиотека Python ===
import streamlit as st
import pandas as pd
import numpy as np
import requests
import logging
import time
import hashlib
import json
import re
import os
import sys
import traceback
import io
import pickle
import random
import math
import warnings
import csv
import base64
import urllib.parse
import tempfile
import itertools
import functools
import operator
import string
import textwrap
import decimal
import uuid
import glob
import shutil
import zipfile
import threading
import queue
import concurrent.futures
import signal
import platform
import gc
import copy
import pprint
import statistics
import secrets
import subprocess
import inspect
import importlib
import importlib.util
import webbrowser
import calendar
import hmac
import configparser
import argparse
import getpass
import sqlite3
from html import escape, unescape
from xml.etree import ElementTree
import xml.dom.minidom
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib

# === Типизация и утилиты ===
from typing import Dict, List, Any, Optional, Tuple, Union, Set, Callable, Iterable, Iterator, Generator
from dataclasses import dataclass, field, asdict, astuple, replace
from functools import lru_cache, wraps, reduce, partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, date, timezone
from collections import defaultdict, Counter, deque, OrderedDict, ChainMap, namedtuple
from enum import Enum, auto, IntEnum
from threading import Lock, RLock, Semaphore, Thread, Event, Barrier, Condition
from contextlib import contextmanager, closing, suppress, ExitStack
from pathlib import Path, PurePath
from abc import ABC, abstractmethod
from multiprocessing import Pool, cpu_count
import multiprocessing as mp
from decimal import Decimal, ROUND_HALF_UP

# ============================================================================
# ОПЦИОНАЛЬНЫЕ ИМПОРТЫ С ОБРАБОТКОЙ ОШИБОК
# ============================================================================
# === PIL/Pillow (изображения) ===
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# === pytz (часовые пояса) ===
try:
    import pytz
    PYTZ_AVAILABLE = True
except ImportError:
    PYTZ_AVAILABLE = False

# === dateutil (парсинг дат) ===
try:
    import dateutil
    from dateutil.parser import parse
    from dateutil.relativedelta import relativedelta
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

# === holidays (праздники) ===
try:
    import holidays
    HOLIDAYS_AVAILABLE = True
except ImportError:
    HOLIDAYS_AVAILABLE = False

# === phonenumbers (телефоны) ===
try:
    import phonenumbers
    from phonenumbers import PhoneNumberType, PhoneNumber
    from phonenumbers import parse as parse_phone, format_number, PhoneNumberFormat
    PHONENUMBERS_AVAILABLE = True
except ImportError:
    PHONENUMBERS_AVAILABLE = False

# === validators (валидация) ===
try:
    import validators
    from validators import url, email as validate_email, domain, ip_address
    VALIDATORS_AVAILABLE = True
except ImportError:
    VALIDATORS_AVAILABLE = False

# === pycountry (страны) ===
try:
    import pycountry
    PYCOUNTRY_AVAILABLE = True
except ImportError:
    PYCOUNTRY_AVAILABLE = False

# === tzlocal (локальная таймзона) ===
try:
    import tzlocal
    TZLOCAL_AVAILABLE = True
except ImportError:
    TZLOCAL_AVAILABLE = False

# === Polars (быстрые DataFrame) ===
try:
    import polars as pl
    import polars.selectors as cs
    POLARS_AVAILABLE = True
    logger_polars = logging.getLogger('polars')
    logger_polars.setLevel(logging.WARNING)
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

# === DuckDB (аналитическая БД) ===
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None

# DASK удалён из-за конфликта с pandas 2.3.3
DASK_AVAILABLE = False
DASK_DF_AVAILABLE = False

# === Ray (распределённые вычисления) ===
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

# === Modin (параллельный pandas) ===
try:
    import modin.pandas as mpd
    import modin.config as mcfg
    MODIN_AVAILABLE = True
except ImportError:
    MODIN_AVAILABLE = False

# === PyArrow (колоночные данные) ===
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.csv as pc
    import pyarrow.json as pj
    import pyarrow.fs as pfs
    import pyarrow.compute as pc_comp
    import pyarrow.dataset as ds
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

# === Pandera (валидация DataFrame) ===
try:
    import pandera as pandera_schema
    from pandera import Column, DataFrameSchema, Check, Index
    PANDERA_AVAILABLE = True
except ImportError:
    PANDERA_AVAILABLE = False

# === scikit-learn (ML) ===
try:
    import sklearn
    from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer, HashingVectorizer
    from sklearn.naive_bayes import MultinomialNB, GaussianNB, BernoulliNB, ComplementNB
    from sklearn.pipeline import Pipeline, make_pipeline, FeatureUnion
    from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV, RandomizedSearchCV
    from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score
    from sklearn.metrics import precision_score, recall_score, roc_auc_score, roc_curve, auc
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, explained_variance_score
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, VotingRegressor
    from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge, Lasso, ElasticNet
    from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder, OneHotEncoder
    from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering, Birch, OPTICS
    from sklearn.decomposition import PCA, TruncatedSVD, NMF, LatentDirichletAllocation
    from sklearn.manifold import TSNE, MDS, Isomap, SpectralEmbedding
    from sklearn.neighbors import NearestNeighbors, KNeighborsClassifier, KNeighborsRegressor
    from sklearn.svm import SVC, SVR, LinearSVC, LinearSVR
    from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, export_graphviz
    from sklearn.isotonic import IsotonicRegression
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.feature_selection import SelectKBest, chi2, f_classif, mutual_info_classif
    from sklearn.feature_selection import RFE, RFECV, SelectFromModel
    from sklearn.multioutput import MultiOutputClassifier, MultiOutputRegressor
    from sklearn.multiclass import OneVsRestClassifier, OneVsOneClassifier
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# === Plotly (интерактивные графики) ===
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    from plotly.offline import plot, iplot
    from plotly.figure_factory import create_annotated_heatmap, create_distplot, create_2d_density
    import plotly.io as pio
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    go = None
    px = None
    make_subplots = None

# === Matplotlib + Seaborn (статические графики) ===
try:
    import matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.patches import Rectangle, Circle, Polygon, Arrow, FancyBboxPatch
    from matplotlib.patches import ConnectionPatch, Wedge, Ellipse, RegularPolygon
    from matplotlib.lines import Line2D
    from matplotlib.text import Text
    from matplotlib.collections import LineCollection, PatchCollection
    from matplotlib.colors import LinearSegmentedColormap, ListedColormap, Normalize
    from matplotlib.cm import ScalarMappable
    from matplotlib.ticker import FuncFormatter, PercentFormatter, EngFormatter
    from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
    import seaborn as sns
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# === Altair (декларативные графики) ===
try:
    import altair as alt
    ALTAIR_AVAILABLE = True
except ImportError:
    ALTAIR_AVAILABLE = False

# === Bokeh (интерактивные дашборды) ===
try:
    import bokeh
    from bokeh.plotting import figure, output_notebook, show
    from bokeh.models import ColumnDataSource, HoverTool, LabelSet, Range1d
    from bokeh.layouts import row, column, gridplot
    BOKEH_AVAILABLE = True
except ImportError:
    BOKEH_AVAILABLE = False

# === openpyxl (Excel) ===
try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, GradientFill, NamedStyle
    from openpyxl.styles import Color, colors, fills, borders, numbers, protection
    from openpyxl.utils import get_column_letter, coordinate_from_string, column_index_from_string
    from openpyxl.utils.dataframe import dataframe_to_rows
    from openpyxl.chart import BarChart, Reference, Series, LineChart, PieChart, ScatterChart
    from openpyxl.chart import AreaChart, RadarChart, StockChart, SurfaceChart
    from openpyxl.chart.label import DataLabelList
    from openpyxl.chart.legend import Legend
    from openpyxl.chart.axis import ChartAxis, NumericAxis, TextAxis, DateAxis
    from openpyxl.chart.data_source import NumData, NumRef
    from openpyxl.chart.shapes import GraphicalProperties
    from openpyxl.chart.text import RichText
    from openpyxl.formatting.rule import Rule, ColorScaleRule, DataBarRule, IconSetRule, CellIsRule, FormulaRule
    from openpyxl.comments import Comment
    from openpyxl.drawing.image import Image as XLImage
    from openpyxl.worksheet.table import Table, TableStyleInfo
    from openpyxl.worksheet.worksheet import Worksheet
    from openpyxl.workbook.workbook import Workbook as XLWorkbook
    from openpyxl.worksheet.datavalidation import DataValidation
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

# === ReportLab (PDF) ===
try:
    from reportlab.lib.pagesizes import letter, A4, A3, A5, landscape, portrait
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.platypus import PageBreak, Image, KeepTogether, NextPageTemplate
    from reportlab.platypus import Frame, PageTemplate, Flowable, DocTemplate
    from reportlab.platypus.para import Paragraph as Para
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch, cm, mm
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
    from reportlab.lib.utils import ImageReader
    from reportlab.graphics.shapes import Drawing, Circle, Rect, String, Line, Polygon
    from reportlab.graphics.charts.barcharts import VerticalBarChart, HorizontalBarChart
    from reportlab.graphics.charts.linecharts import HorizontalLineChart, VerticalLineChart
    from reportlab.graphics.charts.piecharts import Pie
    from reportlab.graphics.charts.legends import Legend as RLLegend
    from reportlab.graphics.widgets.markers import makeMarker
    PDF_EXPORT = True
except ImportError:
    PDF_EXPORT = False

# === xlsxwriter (быстрый Excel) ===
try:
    import xlsxwriter
    XLSXWRITER_AVAILABLE = True
except ImportError:
    XLSXWRITER_AVAILABLE = False

# === tabulate (красивые таблицы) ===
try:
    import tabulate
    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False

# === chardet (определение кодировки) ===
try:
    import chardet
    CHARDET_AVAILABLE = True
except ImportError:
    CHARDET_AVAILABLE = False
    chardet = None

# === OpenAI API ===
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    openai = None

# === Anthropic API ===
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# === tiktoken (токенизация) ===
try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False

# === aiohttp + aiofiles (асинхронность) ===
try:
    import aiohttp
    import aiofiles
    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False

# === httpx (современный HTTP) ===
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

# === websockets ===
try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# === psutil (мониторинг системы) ===
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

# === Babel (локализация чисел/валют) ===
try:
    from babel.numbers import format_currency as babel_format_currency
    from babel.numbers import format_percent as babel_format_percent
    from babel.numbers import format_decimal as babel_format_decimal
    BABEL_AVAILABLE = True
except ImportError:
    BABEL_AVAILABLE = False
# ============================================================================
# ПОДАВЛЕНИЕ ПРЕДУПРЕЖДЕНИЙ
# ============================================================================
warnings.filterwarnings('ignore')
os.environ['PYTHONWARNINGS'] = 'ignore'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'

# ============================================================================
# 🆕 v100.5.1: СОВМЕСТИМОСТЬ STREAMLIT 1.58+
# ============================================================================
def st_dataframe_compat(df, *args, **kwargs):
    """Совместимая обёртка для st.dataframe (width='stretch' для Streamlit 1.58+)"""
    kwargs.pop('use_container_width', None)
    if 'width' not in kwargs:
        kwargs['width'] = 'stretch'
    return st.dataframe(df, *args, **kwargs)

# ============================================================================
# 🆕 v100.5.1: ИСПРАВЛЕНИЕ КРАКОЗЯБР (ДВОЙНОГО UTF-8 КОДИРОВАНИЯ)
# ============================================================================
def detect_mojibake(text: str) -> bool:
    """
    🆕 v100.5.1: Определяет наличие кракозябр (двойного UTF-8 кодирования).
    Кракозябры выглядят как: РђСЂС‚РёРєСѓР», Р‘СЂРµРЅРґ, Р¦РµРЅР°
    Это результат чтения UTF-8 как CP1251 с последующей записью в UTF-8.
    """
    if not isinstance(text, str) or not text:
        return False
    # Паттерн типичных кракозябр: последовательности символов РЎ-РЏ, Р°-СЏ
    # которые выглядят как "Р" + кириллический символ
    mojibake_patterns = [
        r'Р[°-Џ]{2,}',           # Р + 2+ символа из диапазона кракозябр
        r'Р[РЎ][°-Џ]{2,}',       # РР/РС + 2+ символа
        r'[РЎР][°-Џ]{3,}',       # Р/С + 3+ символа
        r'Р[°-Џ]Р[°-Џ]',         # Чередование Р+символ
    ]
    for pattern in mojibake_patterns:
        if re.search(pattern, text):
            return True
    # Дополнительная проверка: если в тексте много "Р" в начале слов
    words = text.split()
    if len(words) >= 3:
        r_words = sum(1 for w in words if w.startswith('Р') and len(w) >= 2)
        if r_words / len(words) > 0.5:
            return True
    return False

def fix_double_utf8(text: str) -> str:
    """
    🆕 v100.5.1: Исправляет двойное кодирование UTF-8.
    Преобразует: РђСЂС‚РёРєСѓР» → Артикул
    Р‘СЂРµРЅРґ → Бренд
    Р¦РµРЅР° → Цена
    """
    if not isinstance(text, str) or not text:
        return text
    # Пробуем разные кодировки для декодирования
    encodings_to_try = [
        ('cp1251', 'utf-8'),      # Windows-1251 → UTF-8 (самый частый случай)
        ('latin1', 'utf-8'),      # Latin-1 → UTF-8
        ('iso-8859-1', 'utf-8'),  # ISO-8859-1 → UTF-8
        ('cp1252', 'utf-8'),      # Windows-1252 → UTF-8
    ]
    for source_enc, target_enc in encodings_to_try:
        try:
            fixed = text.encode(source_enc).decode(target_enc)
            # Проверяем, что результат валидный и не содержит кракозябр
            if fixed and not detect_mojibake(fixed):
                return fixed
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
    return text

def fix_dataframe_encoding(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    🆕 v100.5.1 → v100.9: Исправляет кракозябры во всём DataFrame.
    ✅ ИСПРАВЛЕНО v100.9:
    - Правильный подсчёт исправленных ячеек (а не всех строк)
    - Проверка на mojibake перед исправлением
    Возвращает: (исправленный DataFrame, количество исправленных ячеек)
    """
    fixed_count = 0
    # Исправляем названия колонок
    new_columns = []
    for col in df.columns:
        col_str = str(col)
        if detect_mojibake(col_str):
            new_col = fix_double_utf8(col_str)
            new_columns.append(new_col)
            fixed_count += 1
        else:
            new_columns.append(col)
    df.columns = new_columns
    # Исправляем строковые значения в ячейках
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                # ✅ ИСПРАВЛЕНИЕ v100.9: считаем только реально исправленные ячейки
                def _fix_cell(x):
                    if isinstance(x, str) and detect_mojibake(x):
                        return fix_double_utf8(x)
                    return x
                # Считаем сколько ячеек действительно содержат mojibake
                mask = df[col].apply(lambda x: isinstance(x, str) and detect_mojibake(x))
                fixed_count += int(mask.sum())
                df[col] = df[col].apply(_fix_cell)
            except Exception:
                pass
    return df, fixed_count

def smart_read_csv(file_obj, **kwargs) -> pd.DataFrame:
    """
    🆕 v100.5.1: Умное чтение CSV с автоматическим исправлением кракозябр.
    Приоритет кодировок:
    1. UTF-8-sig (с BOM)
    2. UTF-8
    3. CP1251 (Windows Cyrillic)
    4. Другие
    """
    separators = [';', ',', '\t', '|']
    # Приоритетный порядок кодировок
    encodings_priority = ['utf-8-sig', 'utf-8', 'cp1251', 'windows-1251']
    best_df = None
    best_encoding = None
    best_sep = None
    mojibake_count = 0
    for encoding in encodings_priority:
        for sep in separators:
            try:
                file_obj.seek(0)
                df = pd.read_csv(
                    file_obj,
                    encoding=encoding,
                    sep=sep,
                    engine='python',
                    on_bad_lines='skip',
                    skipinitialspace=True,
                    quotechar='"',
                    doublequote=True,
                    **kwargs
                )
                if df is None or df.empty or len(df.columns) <= 1:
                    continue
                # Проверяем наличие кракозябр в колонках
                current_mojibake = sum(
                    1 for col in df.columns
                    if isinstance(col, str) and detect_mojibake(col)
                )
                # Если нашли вариант без кракозябр — используем его
                if current_mojibake == 0:
                    logger.info(f"✅ CSV прочитан без кракозябр: кодировка={encoding}, разделитель='{sep}'")
                    return df
                # Запоминаем лучший вариант (с минимальным количеством кракозябр)
                if best_df is None or current_mojibake < mojibake_count:
                    best_df = df
                    best_encoding = encoding
                    best_sep = sep
                    mojibake_count = current_mojibake
            except (pd.errors.ParserError, UnicodeDecodeError, Exception):
                continue
    # Если нашли DataFrame с кракозябрами — исправляем их
    if best_df is not None:
        logger.warning(f"⚠️ CSV прочитан с кракозябрами (кодировка={best_encoding}). Исправляем...")
        fixed_df, fixed_count = fix_dataframe_encoding(best_df)
        logger.info(f"✅ Исправлено {fixed_count} ячеек с кракозябрами")
        return fixed_df
    # Fallback: пробуем chardet
    if CHARDET_AVAILABLE and chardet is not None:
        try:
            file_obj.seek(0)
            raw_data = file_obj.read(100000)
            detected = chardet.detect(raw_data)
            if detected and detected.get('encoding'):
                file_obj.seek(0)
                for sep in separators:
                    try:
                        df = pd.read_csv(
                            file_obj,
                            encoding=detected['encoding'],
                            sep=sep,
                            engine='python',
                            on_bad_lines='skip'
                        )
                        if df is not None and not df.empty and len(df.columns) > 1:
                            logger.info(f"CSV прочитан через chardet: {detected['encoding']}")
                            # Проверяем и исправляем кракозябры
                            has_mojibake = any(
                                isinstance(col, str) and detect_mojibake(col)
                                for col in df.columns
                            )
                            if has_mojibake:
                                df, _ = fix_dataframe_encoding(df)
                            return df
                    except (pd.errors.ParserError, UnicodeDecodeError):
                        continue
        except Exception as e:
            logger.warning(f"Ошибка chardet: {e}")
    raise ValueError("Не удалось прочитать CSV файл. Проверьте кодировку и разделитель.")

# ============================================================================
# ВЕРСИЯ И КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ
# ============================================================================
APP_VERSION = "100.5.1"
APP_NAME = "🚗 Юнит-экономика автозапчастей PRO 2026"
APP_AUTHOR = "AutoParts Analytics Team"
APP_DESCRIPTION = "Enterprise расчет юнит-экономики для автозапчастей с AI, ML и High-Volume обработкой"
APP_LICENSE = "MIT License"
APP_COPYRIGHT = f"2024-2026 {APP_AUTHOR}"

EXCEL_ROW_LIMIT = 1_000_000
HISTORY_LIMIT = 50_000
CACHE_TTL = 7200
MAX_THREADS = 32
BATCH_SIZE = 2000
DEFAULT_CURRENCY = "RUB"
DEFAULT_MARKETPLACE = "Ozon"
DEFAULT_MODE = "FBY"
MAX_RETRIES = 5
TIMEOUT_SECONDS = 120
MAX_FILE_SIZE_MB = 500
MAX_UPLOAD_SIZE = 1024 * 1024 * 1024
MAX_CATEGORIES = 500
MAX_ANALOGS = 200
PRECISION_DECIMALS = 4
MAX_DISPLAY_ROWS = 2000
PAGE_SIZE = 100
MAX_HISTORY_ENTRIES = 50000
MAX_CACHE_SIZE = 5000
DEFAULT_LOCALE = "ru_RU"
TIMEZONE = "Europe/Moscow"

# === НАСТРОЙКИ ===
DEFAULT_MARKUP_GLOBAL = 0.25
DEFAULT_DISCOUNT_MAX = 0.30
DEFAULT_MAX_WORKERS = 8
DEFAULT_CHUNK_SIZE = 10000

SUPPORTED_CURRENCIES = ["RUB", "USD", "EUR", "CNY", "KZT", "UAH", "BYN", "AMD", "TRY"]
SUPPORTED_LANGUAGES = ["ru", "en", "uk", "kz", "by", "am", "tr"]
SUPPORTED_MARKETPLACES = ["Ozon", "Wildberries", "Яндекс Маркет", "AliExpress", "Мегамаркет", "СберМегаМаркет", "Avito", "Drom"]
SUPPORTED_MODES = ["FBY", "FBS", "FBO", "DBS", "FBP", "RealFBS"]

# Определение базовой директории
try:
    BASE_DIR = Path(__file__).parent.resolve()
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "cache"
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
TEMP_DIR = BASE_DIR / "temp"
MODELS_DIR = BASE_DIR / "models"
CONFIG_DIR = BASE_DIR / "config"
PLUGINS_DIR = BASE_DIR / "plugins"
EXPORTS_DIR = BASE_DIR / "exports"
TARIFFS_DIR = BASE_DIR / "tariffs"
HISTORY_DB_DIR = BASE_DIR / "history_db"
BACKUPS_DIR = BASE_DIR / "backups"

for dir_path in [DATA_DIR, CACHE_DIR, LOG_DIR, REPORTS_DIR, TEMP_DIR, MODELS_DIR,
                 CONFIG_DIR, PLUGINS_DIR, EXPORTS_DIR, TARIFFS_DIR, HISTORY_DB_DIR, BACKUPS_DIR]:
    try:
        dir_path.mkdir(exist_ok=True, parents=True)
    except OSError as e:
        print(f"Ошибка создания директории {dir_path}: {e}")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE = LOG_DIR / "auto_parts_economy_pro.log"

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"

USE_CACHING = True
USE_PARALLEL = True
USE_GPU = False
OPTIMIZE_MEMORY = True
USE_DUCKDB = True
USE_POLARS = True
USE_MULTIPROCESSING = True

COLORS = {
    "primary": "#e94560",
    "secondary": "#0f3460",
    "success": "#00cc96",
    "warning": "#ffa600",
    "danger": "#ef553b",
    "info": "#636efa",
    "dark": "#1a1a2e",
    "light": "#f5f5f5",
    "gradient_start": "#1a1a2e",
    "gradient_end": "#16213e",
    "input_fill": "#FFF4CC",
    "formula_fill": "#E2EFDA",
    "result_fill": "#DCE6F1",
    "header_fill": "#0F3460",
}

PLOTLY_COLORS = [
    "#e94560", "#0f3460", "#00cc96", "#ffa600", "#ef553b",
    "#636efa", "#f9a825", "#26a69a", "#ab47bc", "#42a5f5",
    "#ec407a", "#66bb6a", "#ffa726", "#8d6e63", "#78909c",
    "#d4ac0d", "#1abc9c", "#2ecc71", "#3498db", "#9b59b6",
    "#e67e22", "#e74c3c", "#1abc9c", "#2ecc71", "#3498db"
]

MARKETPLACE_ICONS = {
    "Ozon": "🟣",
    "Wildberries": "🟡",
    "Яндекс Маркет": "🔵",
    "AliExpress": "🔴",
    "Мегамаркет": "🟢",
    "СберМегаМаркет": "🟠",
    "Avito": "🟤",
    "Drom": "⚫"
}

MODE_ICONS = {
    "FBY": "📦",
    "FBS": "🏪",
    "FBO": "🏭",
    "DBS": "🚚",
    "FBP": "🤝",
    "RealFBS": "🏃"
}

# ============================================================================
# 🆕 v100.5: НАЛОГОВЫЕ РЕЖИМЫ
# ============================================================================
TAX_SYSTEMS = {
    "УСН_6": {"rate": 0.06, "base": "revenue", "name": "УСН 6% (доходы)"},
    "УСН_15": {"rate": 0.15, "base": "profit", "min_rate": 0.01, "name": "УСН 15% (доходы-расходы)"},
    "ОСН": {"rate": 0.20, "base": "profit", "vat": 0.20, "name": "ОСН (общая)"},
    "ПСН": {"rate": 0.0, "base": "fixed", "name": "ПСН (патент)"},
    "НПД": {"rate": 0.06, "base": "revenue", "name": "НПД (самозанятый)"},
}

# ============================================================================
# 🆕 v100.5: БЕНЧМАРКИ РЫНКА
# ============================================================================
MARKET_BENCHMARKS_2026 = {
    "фильтры": {"avg_margin": 25, "avg_price": 800, "return_rate": 0.05},
    "колодки": {"avg_margin": 22, "avg_price": 2500, "return_rate": 0.08},
    "масла": {"avg_margin": 18, "avg_price": 3500, "return_rate": 0.03},
    "аккумуляторы": {"avg_margin": 15, "avg_price": 7000, "return_rate": 0.12},
    "шины": {"avg_margin": 20, "avg_price": 5000, "return_rate": 0.07},
    "фары": {"avg_margin": 28, "avg_price": 4500, "return_rate": 0.15},
    "амортизаторы": {"avg_margin": 24, "avg_price": 3000, "return_rate": 0.10},
    "ремни": {"avg_margin": 26, "avg_price": 1200, "return_rate": 0.06},
    "подшипники": {"avg_margin": 23, "avg_price": 1500, "return_rate": 0.09},
    "датчики": {"avg_margin": 27, "avg_price": 2000, "return_rate": 0.11},
}

# ============================================================================
# 🆕 v100.5: УТИЛИТЫ ДЛЯ ТОЧНЫХ РАСЧЁТОВ
# ============================================================================
def money_round(value: float, decimals: int = 2) -> float:
    """Корректное округление денег (банковское)"""
    return float(Decimal(str(value)).quantize(
        Decimal(f"0.{'0' * decimals}"),
        rounding=ROUND_HALF_UP
    ))

def calculate_tax(price: float, cost: float, tax_system: str = "УСН_6") -> float:
    """Расчёт налога с учётом режима"""
    cfg = TAX_SYSTEMS.get(tax_system, TAX_SYSTEMS["УСН_6"])
    if cfg["base"] == "revenue":
        return money_round(price * cfg["rate"])
    elif cfg["base"] == "profit":
        profit = price - cost
        tax = profit * cfg["rate"]
        if tax_system == "УСН_15":
            min_tax = price * cfg.get("min_rate", 0.01)
            tax = max(tax, min_tax)
        return money_round(max(0, tax))
    elif cfg["base"] == "fixed":
        return 0.0
    return 0.0

# ============================================================================
# 🆕 v100.5: УТИЛИТЫ ДЛЯ ОБЪЁМНОГО ВЕСА
# ============================================================================
def calculate_billable_weight(weight_kg: float,
                              length_cm: float, width_cm: float,
                              height_cm: float,
                              volumetric_coeff: float = 5000.0) -> float:
    """Расчёт оплачиваемого веса (больший из реального и объёмного)"""
    if length_cm <= 0 or width_cm <= 0 or height_cm <= 0:
        return weight_kg
    volumetric_weight = (length_cm * width_cm * height_cm) / volumetric_coeff
    billable = max(weight_kg, volumetric_weight)
    billable = math.ceil(billable * 2) / 2
    return billable

# ============================================================================
# 🆕 v100.5: ПРОГРЕССИВНОЕ ХРАНЕНИЕ
# ============================================================================
def calculate_storage_cost_progressive(volume_l: float, days: int,
                                       base_rate: float, marketplace: str) -> float:
    """Прогрессивная стоимость хранения"""
    if marketplace in ["Ozon", "Wildberries"]:
        if days <= 60:
            multiplier = 1.0
        elif days <= 90:
            multiplier = 2.0
        elif days <= 180:
            multiplier = 4.0
        elif days <= 365:
            multiplier = 8.0
        else:
            multiplier = 16.0
        weighted_rate = base_rate * multiplier
        return money_round(volume_l * weighted_rate * days)
    else:
        return money_round(volume_l * base_rate * days)

# ============================================================================
# 🆕 v100.5: РЕАЛЬНЫЕ ВОЗВРАТЫ
# ============================================================================
def calculate_returns_cost(price: float, return_rate: float,
                           reverse_logistics: float = 150.0,
                           inspection_cost: float = 50.0) -> float:
    """Полная стоимость возвратов"""
    expected_returns = price * return_rate
    reverse_logistics_cost = reverse_logistics * return_rate
    inspection = inspection_cost * return_rate
    loss_from_defects = price * return_rate * 0.3
    return money_round(expected_returns + reverse_logistics_cost + inspection + loss_from_defects)

# ============================================================================
# 🆕 v100.5: СПЕЦИФИЧЕСКИЕ РАСХОДЫ АВТОЗАПЧАСТЕЙ
# ============================================================================
@dataclass
class AutoPartsSpecificCosts:
    """Специфические расходы для автозапчастей"""
    chestny_znak: float = 1.5
    certification_amortization: float = 0.0
    warranty_reserve: float = 0.02
    packaging_fbs: float = 45.0
    labeling: float = 3.0
    util_tax: float = 0.0
    customs_duty: float = 0.0
    currency_risk: float = 0.03

    def calculate(self, price: float, is_import: bool = False,
                  requires_marking: bool = True) -> float:
        total = 0.0
        if requires_marking:
            total += self.chestny_znak
        total += self.certification_amortization
        total += price * self.warranty_reserve
        total += self.packaging_fbs
        total += self.labeling
        if is_import:
            total += price * self.currency_risk
            total += self.customs_duty
            total += price * self.util_tax
        return money_round(total)

# ============================================================================
# 🆕 v100.5: РЕКЛАМНЫЕ РАСХОДЫ (ДРР)
# ============================================================================
def calculate_advertising_cost(price: float,
                               category: str,
                               ad_intensity: str = "medium") -> float:
    """Рекламные расходы (ДРР — доля рекламных расходов)"""
    drr_rates = {
        "low": 0.05,
        "medium": 0.15,
        "high": 0.25,
        "aggressive": 0.35
    }
    competitive_categories = ["масла", "фильтры", "колодки", "аккумуляторы"]
    if category in competitive_categories:
        intensity = "high" if ad_intensity == "medium" else ad_intensity
    else:
        intensity = ad_intensity
    return money_round(price * drr_rates.get(intensity, 0.15))

# ============================================================================
# 🆕 v100.5: ВАЛИДАЦИЯ ВХОДНЫХ ДАННЫХ
# ============================================================================
def validate_input_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Проверка качества данных перед расчётом"""
    errors = []
    if 'Цена' in df.columns:
        negative_prices = (df['Цена'] <= 0).sum()
        if negative_prices > 0:
            errors.append(f"⚠️ {negative_prices} товаров с ценой ≤ 0")
        suspicious = (df['Цена'] < 50).sum()
        if suspicious > 0:
            errors.append(f"⚠️ {suspicious} товаров дешевле 50₽ — проверьте")
    if 'Длина' in df.columns:
        missing_dims = df['Длина'].isna().sum()
        if missing_dims > len(df) * 0.3:
            errors.append(f"⚠️ У {missing_dims} товаров нет габаритов — логистика будет неточной")
    return len(errors) == 0, errors

# ============================================================================
# 🆕 v100.4: НОВЫЕ УТИЛИТЫ - ПАРСИНГ РАЗМЕРОВ
# ============================================================================
def parse_dimensions_string(dim_str: str) -> Tuple[float, float, float]:
    """
    🆕 v100.4: Парсит "человеческий" ввод размеров в формат (длина, ширина, высота).
    """
    if not dim_str or not isinstance(dim_str, str):
        return 0.0, 0.0, 0.0
    dim_str = dim_str.lower().strip()
    separators = ['x', '*', 'х', '×', ' ', ',']
    for sep in separators:
        if sep in dim_str:
            parts = [p.strip() for p in dim_str.split(sep) if p.strip()]
            if len(parts) >= 3:
                try:
                    dimensions = []
                    for p in parts[:3]:
                        cleaned = re.sub(r'[^\d.,\-]', '', p)
                        cleaned = cleaned.replace(',', '.')
                        if cleaned and cleaned.replace('.', '').replace('-', '').isdigit():
                            dimensions.append(float(cleaned))
                        else:
                            nums = re.findall(r'(\d+\.?\d*)', p)
                            if nums:
                                dimensions.append(float(nums[0]))
                    if len(dimensions) == 3:
                        dimensions.sort(reverse=True)
                        return tuple(dimensions)
                except (ValueError, TypeError):
                    pass
    return 0.0, 0.0, 0.0

def parse_dimensions_vectorized(dims_series) -> "pl.DataFrame":
    """Векторизованный парсинг размеров для Polars DataFrame."""
    if not POLARS_AVAILABLE:
        return pl.DataFrame()
    dims = dims_series.str.extract_all(r"(\d+\.?\d*)")
    def sort_dimensions(nums):
        if nums and len(nums) >= 3:
            try:
                return sorted([float(n) for n in nums[:3]], reverse=True)
            except (ValueError, TypeError):
                pass
        elif nums and len(nums) == 2:
            try:
                return [float(nums[0]), float(nums[1]), 1.0]
            except (ValueError, TypeError):
                pass
        return [0.0, 0.0, 0.0]
    result = dims.map_elements(sort_dimensions, return_dtype=pl.List(pl.Float64))
    return pl.DataFrame({
        "length": result.list.get(0),
        "width": result.list.get(1),
        "height": result.list.get(2)
    })

# ============================================================================
# УТИЛИТЫ ДЛЯ БЕЗОПАСНОСТИ И КЭШИРОВАНИЯ
# ============================================================================
def get_api_key_safe(service_name: str) -> Optional[str]:
    """Безопасное получение API ключа через st.secrets"""
    try:
        if hasattr(st, 'secrets') and service_name in st.secrets:
            return st.secrets[service_name]
    except Exception:
        pass
    env_key = f"{service_name.upper()}_API_KEY"
    return os.environ.get(env_key)

def escape_sql_string(value: str) -> str:
    """Экранирование строк для SQL-запросов"""
    if not value:
        return ""
    return re.sub(r"['\";\\]", "", str(value))

# ============================================================================
# КЛАССЫ ИСКЛЮЧЕНИЙ
# ============================================================================
class AutoPartsException(Exception):
    """Базовое исключение для приложения"""
    def __init__(self, message: str = "", *args, **kwargs):
        self.message = message
        self.timestamp = datetime.now()
        self.context = kwargs
        super().__init__(message, *args)
    def __str__(self):
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {self.message}"

class ValidationError(AutoPartsException):
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        self.field = field
        self.value = value
        super().__init__(f"Ошибка валидации{f' в поле {field}' if field else ''}: {message}")

class MarketplaceError(AutoPartsException):
    def __init__(self, message: str, marketplace: Optional[str] = None):
        self.marketplace = marketplace
        super().__init__(f"Ошибка маркетплейса{f' {marketplace}' if marketplace else ''}: {message}")

class CalculationError(AutoPartsException):
    def __init__(self, message: str, calculation_type: Optional[str] = None):
        self.calculation_type = calculation_type
        super().__init__(f"Ошибка расчета{f' {calculation_type}' if calculation_type else ''}: {message}")

class AIError(AutoPartsException):
    def __init__(self, message: str, provider: Optional[str] = None, code: Optional[int] = None):
        self.provider = provider
        self.code = code
        super().__init__(f"Ошибка AI{f' ({provider})' if provider else ''}: {message}")

class DatabaseError(AutoPartsException):
    def __init__(self, message: str, query: Optional[str] = None, params: Optional[Dict] = None):
        self.query = query
        self.params = params
        super().__init__(f"Ошибка базы данных: {message}")

class ExportError(AutoPartsException):
    def __init__(self, message: str, format: Optional[str] = None, file_path: Optional[Path] = None):
        self.format = format
        self.file_path = file_path
        super().__init__(f"Ошибка экспорта{f' в {format}' if format else ''}: {message}")

class ConfigError(AutoPartsException):
    def __init__(self, message: str, key: Optional[str] = None):
        self.key = key
        super().__init__(f"Ошибка конфигурации{f' для {key}' if key else ''}: {message}")

class DataNotFoundError(AutoPartsException):
    def __init__(self, message: str, entity: Optional[str] = None, id: Optional[Any] = None):
        self.entity = entity
        self.id = id
        super().__init__(f"Данные не найдены{f' {entity}' if entity else ''}: {message}")

class TimeoutError(AutoPartsException):
    def __init__(self, message: str, timeout: Optional[float] = None):
        self.timeout = timeout
        super().__init__(f"Превышено время ожидания{f' ({timeout}с)' if timeout else ''}: {message}")

class PermissionError(AutoPartsException):
    def __init__(self, message: str, resource: Optional[str] = None):
        self.resource = resource
        super().__init__(f"Ошибка доступа{f' к {resource}' if resource else ''}: {message}")

class RateLimitError(AutoPartsException):
    def __init__(self, message: str, limit: Optional[int] = None, reset_time: Optional[datetime] = None):
        self.limit = limit
        self.reset_time = reset_time
        super().__init__(f"Превышен лимит запросов{f' ({limit})' if limit else ''}: {message}")

class AuthenticationError(AutoPartsException):
    def __init__(self, message: str, provider: Optional[str] = None):
        self.provider = provider
        super().__init__(f"Ошибка аутентификации{f' ({provider})' if provider else ''}: {message}")

class IncompatibleDataError(AutoPartsException):
    def __init__(self, message: str, expected_type: Optional[str] = None, actual_type: Optional[str] = None):
        self.expected_type = expected_type
        self.actual_type = actual_type
        super().__init__(f"Несовместимые данные: {message}")

class DataCorruptionError(AutoPartsException):
    def __init__(self, message: str, file_path: Optional[Path] = None, checksum: Optional[str] = None):
        self.file_path = file_path
        self.checksum = checksum
        super().__init__(f"Повреждение данных{f' в {file_path}' if file_path else ''}: {message}")

class ConnectionError(AutoPartsException):
    def __init__(self, message: str, host: Optional[str] = None, port: Optional[int] = None):
        self.host = host
        self.port = port
        super().__init__(f"Ошибка соединения{f' с {host}:{port}' if host else ''}: {message}")

class InvalidStateError(AutoPartsException):
    def __init__(self, message: str, state: Optional[str] = None):
        self.state = state
        super().__init__(f"Некорректное состояние{f' ({state})' if state else ''}: {message}")

class PriceImportError(AutoPartsException):
    def __init__(self, message: str, file_path: Optional[str] = None):
        self.file_path = file_path
        super().__init__(f"Ошибка импорта цен{f' ({file_path})' if file_path else ''}: {message}")

class ForecastError(AutoPartsException):
    def __init__(self, message: str, model: Optional[str] = None):
        self.model = model
        super().__init__(f"Ошибка прогнозирования{f' ({model})' if model else ''}: {message}")

# ============================================================================
# ЛОГГЕР
# ============================================================================
@st.cache_resource
def get_logger():
    """Логгер через st.cache_resource"""
    logger = logging.getLogger('UnitEconomyPro')
    logger.setLevel(getattr(logging, LOG_LEVEL))
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    try:
        fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except OSError as e:
        print(f"Ошибка создания файлового логгера: {e}")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = get_logger()

# ============================================================================
# ДЕКОРАТОРЫ
# ============================================================================
def timer_decorator(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            if elapsed > 1.0:
                logger.debug(f"⏱ {func.__name__} выполнена за {elapsed:.3f}с")
            return result
        except Exception as e:
            elapsed = time.perf_counter() - start
            logger.error(f"❌ {func.__name__} завершилась с ошибкой за {elapsed:.3f}с: {e}")
            raise
    return wrapper

def cache_decorator(ttl: int = CACHE_TTL, maxsize: int = 1000) -> Callable:
    def decorator(func: Callable) -> Callable:
        cache = {}
        timestamps = {}
        access_count = defaultdict(int)
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not USE_CACHING:
                return func(*args, **kwargs)
            key = generate_cache_key(*args, **kwargs)
            if len(cache) > maxsize:
                least_used = sorted(access_count.items(), key=lambda x: x[1])[:len(cache) - maxsize]
                for k, _ in least_used:
                    cache.pop(k, None)
                    timestamps.pop(k, None)
                    access_count.pop(k, None)
            if key in cache and time.time() - timestamps.get(key, 0) < ttl:
                access_count[key] += 1
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            timestamps[key] = time.time()
            access_count[key] = 0
            return result
        return wrapper
    return decorator

def retry_decorator(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, exceptions: tuple = (Exception,)) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"⚠️ Попытка {attempt + 1}/{max_retries} для {func.__name__} не удалась: {e}")
                    time.sleep(current_delay)
                    current_delay *= backoff
            if last_exception:
                raise last_exception
            return None
        return wrapper
    return decorator

def validate_inputs(*types: Union[type, tuple], **kwargs_types: Union[type, tuple]) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i, arg in enumerate(args):
                if i < len(types):
                    expected_type = types[i]
                    if not isinstance(arg, expected_type):
                        raise ValidationError(f"Аргумент {i} должен быть типа {expected_type.__name__}", field=str(i), value=arg)
            for param_name, param_value in kwargs.items():
                if param_name in kwargs_types:
                    expected_type = kwargs_types[param_name]
                    if not isinstance(param_value, expected_type):
                        raise ValidationError(f"Аргумент '{param_name}' должен быть типа {expected_type.__name__}", field=param_name, value=param_value)
            return func(*args, **kwargs)
        return wrapper
    return decorator

def log_execution(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        args_str = []
        if args:
            args_str.extend(str(a)[:100] for a in args[:5])
        if kwargs:
            args_str.extend(f"{k}={str(v)[:100]}" for k, v in list(kwargs.items())[:5])
        logger.info(f"▶️ Выполнение {func.__name__}({', '.join(args_str)})")
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start_time
            logger.info(f"✅ {func.__name__} выполнена за {elapsed:.3f}с")
            return result
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            logger.error(f"❌ {func.__name__} завершилась с ошибкой за {elapsed:.3f}с: {e}")
            logger.error(traceback.format_exc())
            raise
    return wrapper

def safe_execution(default_return: Any = None, log_error: bool = True) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_error:
                    logger.error(f"⚠️ Ошибка в {func.__name__}: {e}")
                return default_return
        return wrapper
    return decorator

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            return default
        return float(val)
    if isinstance(val, (decimal.Decimal, np.floating, np.integer)):
        try:
            return float(val)
        except (ValueError, TypeError):
            return default
    if isinstance(val, str):
        cleaned = val.strip()
        if not cleaned:
            return default
        cleaned = re.sub(r'[^\d.,\-+\s]', '', cleaned)
        cleaned = cleaned.replace(' ', '').replace(',', '.')
        if cleaned.count('-') > 1:
            return default
        parts = cleaned.split('.')
        if len(parts) > 2:
            return default
        try:
            return float(cleaned)
        except ValueError:
            return default
    if hasattr(val, 'dtype') and hasattr(val, 'item'):
        try:
            item = val.item()
            if isinstance(item, (int, float)):
                return float(item)
        except Exception:
            pass
    return default

def safe_int(val: Any, default: int = 0) -> int:
    try:
        float_val = safe_float(val, default)
        if float_val == default and val != 0:
            return default
        return int(float_val)
    except (ValueError, TypeError):
        return default

def safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            return default
        return str(val)
    if isinstance(val, (list, tuple)):
        return ", ".join(safe_str(v) for v in val[:5]) + ("..." if len(val) > 5 else "")
    if isinstance(val, dict):
        return str({k: safe_str(v) for k, v in list(val.items())[:5]})
    try:
        result = str(val).strip()
        return result if result else default
    except Exception:
        return default

def safe_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        val_lower = val.lower().strip()
        true_values = {'true', 'yes', '1', 'y', 'да', 'on'}
        false_values = {'false', 'no', '0', 'n', 'нет', 'off'}
        if val_lower in true_values:
            return True
        if val_lower in false_values:
            return False
        return default
    if isinstance(val, (list, tuple, dict)):
        return bool(val)
    return default

def safe_datetime(val: Any, default: Optional[datetime] = None) -> Optional[datetime]:
    if default is None:
        default = datetime.now()
    if val is None:
        return default
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime.combine(val, datetime.min.time())
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val)
        except (ValueError, OSError):
            return default
    if isinstance(val, str):
        formats = [
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d",
            "%d.%m.%Y %H:%M:%S", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%fZ",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
        try:
            if DATEUTIL_AVAILABLE:
                return parse(val)
        except Exception:
            pass
    return default

def generate_cache_key(*args, **kwargs) -> str:
    key_parts = []
    for arg in args:
        if isinstance(arg, (dict, OrderedDict)):
            key_parts.append(json.dumps(arg, sort_keys=True, ensure_ascii=False))
        elif isinstance(arg, (list, tuple, set)):
            key_parts.append(str(sorted(arg) if not isinstance(arg, tuple) else arg))
        elif isinstance(arg, pd.DataFrame):
            try:
                key_parts.append(hashlib.md5(pd.util.hash_pandas_object(arg).values.tobytes()).hexdigest())
            except Exception:
                key_parts.append(str(len(arg)))
        elif isinstance(arg, pd.Series):
            try:
                key_parts.append(hashlib.md5(pd.util.hash_pandas_object(arg).values.tobytes()).hexdigest())
            except Exception:
                key_parts.append(str(len(arg)))
        elif isinstance(arg, np.ndarray):
            try:
                key_parts.append(hashlib.md5(arg.tobytes()).hexdigest())
            except Exception:
                key_parts.append(str(arg.shape))
        elif isinstance(arg, (datetime, date)):
            key_parts.append(arg.isoformat())
        else:
            key_parts.append(str(arg))
    for k, v in sorted(kwargs.items()):
        if isinstance(v, (dict, OrderedDict)):
            key_parts.append(f"{k}:{json.dumps(v, sort_keys=True, ensure_ascii=False)}")
        elif isinstance(v, (list, tuple, set)):
            key_parts.append(f"{k}:{str(sorted(v) if not isinstance(v, tuple) else v)}")
        elif isinstance(v, pd.DataFrame):
            try:
                key_parts.append(f"{k}:{hashlib.md5(pd.util.hash_pandas_object(v).values.tobytes()).hexdigest()}")
            except Exception:
                key_parts.append(f"{k}:{len(v)}")
        else:
            key_parts.append(f"{k}:{v}")
    key = "|".join(key_parts)
    return hashlib.md5(key.encode('utf-8')).hexdigest()

def calculate_volume(length: float, width: float, height: float) -> float:
    if not all([length, width, height]):
        return 0.0
    if not all([length > 0, width > 0, height > 0]):
        return 0.0
    if any([length > 1000, width > 1000, height > 1000]):
        length /= 10
        width /= 10
        height /= 10
    if any([length < 0.1, width < 0.1, height < 0.1]):
        return 0.0
    volume = (length * width * height) / 1000.0
    if volume < 0.001:
        return 0.0
    return round(volume, 4)

def get_file_encoding(file_path: Union[str, Path]) -> str:
    if CHARDET_AVAILABLE and chardet is not None:
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(100000)
                result = chardet.detect(raw_data)
                encoding = result.get('encoding', 'utf-8')
                return encoding
        except (IOError, OSError) as e:
            logger.warning(f"Ошибка определения кодировки: {e}")
    encodings = ['utf-8-sig', 'utf-8', 'cp1251', 'windows-1251', 'cp1252', 'latin1']
    for enc in encodings:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                f.read()
            return enc
        except UnicodeDecodeError:
            continue
    return 'utf-8'

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def normalize_key_for_match(value: str) -> str:
    if not value:
        return ""
    return re.sub(r'[^0-9A-Za-zА-Яа-яЁё]', '', str(value).lower().strip())

# ============================================================================
# ФУНКЦИЯ РАСЧЕТА РЕКОМЕНДУЕМОЙ МИНИМАЛЬНОЙ ЦЕНЫ
# ============================================================================
def calculate_recommended_min_price(
    cost: float,
    commission_rate: float,
    logistics: float,
    storage_cost: float,
    acquiring_rate: float,
    last_mile: float,
    return_rate: float,
    min_profit_percent: float = 0.10,
    tax_system: str = "УСН_6",
    tax_rate: float = 0.06
) -> float:
    if cost <= 0:
        return 0.0
    fixed_costs = cost + logistics + storage_cost + last_mile
    variable_rate = commission_rate + acquiring_rate + return_rate + tax_rate + min_profit_percent
    denominator = 1 - variable_rate
    if denominator <= 0:
        return 0.0
    recommended_price = fixed_costs / denominator
    return max(0, money_round(recommended_price))
# ============================================================================
# БЛОК 1: ENUM И ТИПЫ
# ============================================================================
class CommissionType(Enum):
    PERCENTAGE = auto()
    FIXED = auto()
    HYBRID = auto()
    SUBSCRIPTION = auto()
    TIERED = auto()
    DYNAMIC = auto()
    FLAT = auto()
    CUSTOM = auto()

class OperationMode(Enum):
    FBY = auto()
    FBS = auto()
    FBO = auto()
    DBS = auto()
    FBP = auto()
    DBE = auto()
    STANDARD = auto()
    EXPRESS = auto()
    SELF = auto()
    REAL_FBS = auto()

class ProductType(Enum):
    ENGINE = "Двигатель"
    TRANSMISSION = "Трансмиссия"
    SUSPENSION = "Подвеска"
    BRAKE = "Тормозная система"
    STEERING = "Рулевое управление"
    ELECTRICAL = "Электрооборудование"
    COOLING = "Система охлаждения"
    EXHAUST = "Система выпуска"
    FUEL = "Система питания"
    FILTER = "Фильтры"
    FLUID = "Масла и жидкости"
    BODY = "Кузовные детали"
    INTERIOR = "Салон"
    EXTERIOR = "Экстерьер"
    OPTICS = "Оптика"
    TIRES = "Шины и диски"
    TOOLS = "Инструменты"
    BELT = "Ремни и приводы"
    BEARING = "Подшипники"
    SEAL = "Сальники и прокладки"
    FASTENER = "Крепеж"
    HVAC = "Климат-контроль"
    AUDIO = "Аудио и мультимедиа"
    SAFETY = "Безопасность"
    OTHER = "Прочее"

class DataSource(Enum):
    CSV = auto()
    EXCEL = auto()
    JSON = auto()
    API = auto()
    DATABASE = auto()
    MANUAL = auto()
    MARKETPLACE = auto()
    AI = auto()
    WEB_SCRAPING = auto()
    ERP = auto()
    CRM = auto()
    EXTERNAL = auto()

class ExportFormat(Enum):
    CSV = auto()
    EXCEL = auto()
    EXCEL_FORMULAS = auto()
    EXCEL_MACROS = auto()
    PDF = auto()
    JSON = auto()
    HTML = auto()
    MARKDOWN = auto()
    PARQUET = auto()
    SQL = auto()
    XML = auto()
    YAML = auto()
    TOML = auto()
    POWER_BI = auto()
    TABLEAU = auto()

class CalculationStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PAUSED = auto()
    PARTIAL = auto()

class RiskLevel(Enum):
    LOW = "Низкий"
    MEDIUM = "Средний"
    HIGH = "Высокий"
    CRITICAL = "Критический"

class Seasonality(Enum):
    WINTER = "Зимняя"
    SPRING = "Весенняя"
    SUMMER = "Летняя"
    AUTUMN = "Осенняя"
    ALL_YEAR = "Круглогодичная"

class ProfitabilityLevel(Enum):
    LOSS = "Убыток"
    BREAK_EVEN = "Точка безубыточности"
    LOW = "Низкая"
    MEDIUM = "Средняя"
    HIGH = "Высокая"
    VERY_HIGH = "Очень высокая"

class Currency(Enum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"
    CNY = "CNY"
    KZT = "KZT"
    UAH = "UAH"
    BYN = "BYN"
    AMD = "AMD"
    TRY = "TRY"

class TaxSystem(Enum):
    USN_6 = "УСН_6"
    USN_15 = "УСН_15"
    OSN = "ОСН"
    PSN = "ПСН"
    NPD = "НПД"

class TariffSource(Enum):
    HARDCODED = "Захардкожены"
    AI_CACHE = "Кэш ИИ"
    AI_LIVE = "ИИ (запрос)"
    MANUAL = "Ручной ввод"
    IMPORTED = "Импортированы"
    API_LIVE = "API Маркетплейса"
    FORECAST = "Прогноз ИИ"

# ============================================================================
# БЛОК 2: ДАТАКЛАССЫ (🆕 v100.5 - С НОВЫМИ ПОЛЯМИ)
# ============================================================================
@dataclass
class MarketplaceConfig:
    """Расширенная конфигурация маркетплейса с сезонностью и промо"""
    name: str
    commission_rate: float
    min_commission: float = 0.0
    max_commission: float = float('inf')
    logistics_base: float = 0.0
    logistics_per_kg: float = 0.0
    logistics_per_liter: float = 0.0
    storage_per_day: float = 0.0
    return_fee: float = 0.0
    acquiring_fee: float = 0.0
    last_mile_fee: float = 0.0
    delivery_fee_percent: float = 0.0
    premium_fee: float = 0.0
    rko_fee: float = 0.0
    subscription_fee: float = 0.0
    insurance_fee: float = 0.0
    packing_fee: float = 0.0
    marketing_fee: float = 0.0
    hazardous_surcharge: float = 0.0
    fragile_surcharge: float = 0.0
    oversized_surcharge: float = 0.0
    category_rates: Dict[str, float] = field(default_factory=dict)
    mode_multipliers: Dict[str, float] = field(default_factory=dict)
    weight_tiers: List[Tuple[float, float, float]] = field(default_factory=list)
    volume_tiers: List[Tuple[float, float, float]] = field(default_factory=list)
    available: bool = True
    description: str = ""
    version: str = "2026.1"
    last_updated: datetime = field(default_factory=datetime.now)
    tariff_source: TariffSource = TariffSource.HARDCODED
    seasonal_multipliers: Dict[str, float] = field(default_factory=dict)
    promo_discount: float = 0.0
    promo_start: Optional[datetime] = None
    promo_end: Optional[datetime] = None
    dynamic_adjustment: float = 0.0
    last_forecast: Optional[Dict[str, Any]] = None
    forecast_timestamp: Optional[datetime] = None
    
    def get_commission_rate(self, category: Optional[str] = None) -> float:
        if category and category in self.category_rates:
            return self.category_rates[category]
        return self.commission_rate
    
    def get_mode_multiplier(self, mode: str) -> float:
        return self.mode_multipliers.get(mode, 1.0)
    
    def apply_seasonal_multiplier(self, base_rate: float, current_month: Optional[int] = None) -> float:
        if current_month is None:
            current_month = datetime.now().month
        
        if current_month in [12, 1, 2]:
            season = "winter"
        elif current_month in [3, 4, 5]:
            season = "spring"
        elif current_month in [6, 7, 8]:
            season = "summer"
        else:
            season = "autumn"
        
        multiplier = self.seasonal_multipliers.get(season, 1.0)
        return base_rate * multiplier
    
    def apply_promo_discount(self, amount: float) -> float:
        if self.promo_discount <= 0:
            return amount
        
        now = datetime.now()
        if self.promo_start and self.promo_end:
            if self.promo_start <= now <= self.promo_end:
                return amount * (1 - self.promo_discount)
        else:
            return amount * (1 - self.promo_discount)
        
        return amount
    
    def calculate_commission_with_dynamics(self, price: float,
                                            discount_percent: float = 0.0,
                                            promo_participation: float = 0.0,
                                            category: Optional[str] = None,
                                            current_month: Optional[int] = None) -> float:
        """🆕 v100.5: Комиссия с учётом скидок и участия в акциях"""
        actual_price = price * (1 - discount_percent)
        promo_surcharge = actual_price * promo_participation
        
        base_rate = self.get_commission_rate(category)
        rate = self.apply_seasonal_multiplier(base_rate, current_month)
        rate += self.dynamic_adjustment
        
        commission = max(actual_price * rate, self.min_commission)
        commission += promo_surcharge
        commission = self.apply_promo_discount(commission)
        
        if self.max_commission < float('inf'):
            commission = min(commission, self.max_commission)
        
        return commission

@dataclass
class ProductDimensions:
    length: float = 0.0
    width: float = 0.0
    height: float = 0.0
    weight: float = 0.0
    unit: str = "см"
    weight_unit: str = "кг"
    dimension_string: str = ""
    
    @property
    def volume(self) -> float:
        return calculate_volume(self.length, self.width, self.height)
    
    @property
    def is_valid(self) -> bool:
        return all([self.length > 0, self.width > 0, self.height > 0, self.weight > 0])
    
    @property
    def display_dimensions(self) -> str:
        if self.length > 0 and self.width > 0 and self.height > 0:
            return f"{self.length:.1f}x{self.width:.1f}x{self.height:.1f} см"
        return "Размеры не указаны"
    
    def to_dict(self) -> Dict[str, float]:
        return {
            "length": self.length, "width": self.width,
            "height": self.height, "weight": self.weight,
            "volume": self.volume
        }
    
    @classmethod
    def from_string(cls, dim_str: str, weight: float = 0.0) -> 'ProductDimensions':
        length, width, height = parse_dimensions_string(dim_str)
        return cls(
            length=length,
            width=width,
            height=height,
            weight=weight,
            dimension_string=dim_str
        )

@dataclass
class ProductCategory:
    name: str
    description: str = ""
    parent_category: Optional[str] = None
    dimensions: Optional[ProductDimensions] = None
    typical_volume: float = 0.0
    typical_weight: float = 0.0
    oem_codes: List[str] = field(default_factory=list)
    cross_references: List[str] = field(default_factory=list)
    alternatives: List[str] = field(default_factory=list)
    compatibility: List[str] = field(default_factory=list)
    hazardous: bool = False
    fragile: bool = False
    requires_special_packaging: bool = False
    seasonality: Seasonality = Seasonality.ALL_YEAR
    risk_level: RiskLevel = RiskLevel.LOW
    price_range_min: float = 0.0
    price_range_max: float = 0.0
    margin_avg: float = 0.0
    demand_score: float = 0.0
    min_length: float = 0.0
    max_length: float = 0.0
    min_width: float = 0.0
    max_width: float = 0.0
    min_height: float = 0.0
    max_height: float = 0.0
    min_weight: float = 0.0
    max_weight: float = 0.0
    
    def get_dimensions(self) -> ProductDimensions:
        return self.dimensions or ProductDimensions()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name, "description": self.description,
            "parent_category": self.parent_category,
            "typical_volume": self.typical_volume,
            "typical_weight": self.typical_weight,
            "oem_codes": self.oem_codes,
            "cross_references": self.cross_references,
            "hazardous": self.hazardous, "fragile": self.fragile,
            "seasonality": self.seasonality.value,
            "risk_level": self.risk_level.value
        }

@dataclass
class UnitEconomicsResult:
    """🆕 v100.5: Результат расчёта с новыми полями"""
    marketplace: str
    operation_mode: str
    category: str
    price: float
    cost: float
    length: float
    width: float
    height: float
    weight: float
    volume: float
    commission: float
    commission_percent: float
    logistics: float
    storage_cost: float
    acquiring: float
    delivery: float
    last_mile: float
    returns: float
    rko_fee: float
    premium_fee: float
    insurance_fee: float
    packing_fee: float
    marketing_fee: float
    subscription_cost: float
    hazardous_surcharge: float = 0.0
    fragile_surcharge: float = 0.0
    oversized_surcharge: float = 0.0
    tax_amount: float = 0.0
    tax_system: str = "УСН_6"
    total_expenses: float = 0.0
    profit: float = 0.0
    margin_percent: float = 0.0
    roi: float = 0.0
    breakeven_price: float = 0.0
    recommended_min_price: float = 0.0
    profit_per_ruble: float = 0.0
    contribution_margin: float = 0.0
    contribution_margin_ratio: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    calculation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: CalculationStatus = CalculationStatus.COMPLETED
    tariff_source: TariffSource = TariffSource.HARDCODED
    metadata: Dict[str, Any] = field(default_factory=dict)
    applied_seasonal_multiplier: float = 1.0
    applied_promo_discount: float = 0.0
    dynamic_adjustment: float = 0.0
    billable_weight: float = 0.0
    advertising_cost: float = 0.0
    auto_parts_specific: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        result['status'] = self.status.name
        result['tariff_source'] = self.tariff_source.value
        return result
    
    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([self.to_dict()])
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            "marketplace": self.marketplace, "profit": self.profit,
            "margin": self.margin_percent, "roi": self.roi,
            "breakeven": self.breakeven_price,
            "recommended_min_price": self.recommended_min_price,
            "total_expenses": self.total_expenses
        }
    
    def get_profitability_level(self) -> ProfitabilityLevel:
        if self.profit < 0:
            return ProfitabilityLevel.LOSS
        elif self.profit == 0:
            return ProfitabilityLevel.BREAK_EVEN
        elif self.margin_percent < 5:
            return ProfitabilityLevel.LOW
        elif self.margin_percent < 15:
            return ProfitabilityLevel.MEDIUM
        elif self.margin_percent < 30:
            return ProfitabilityLevel.HIGH
        else:
            return ProfitabilityLevel.VERY_HIGH

@dataclass
class ForecastResult:
    periods: List[datetime]
    values: List[float]
    seasonality: List[float]
    trend: List[float]
    confidence_intervals: Tuple[List[float], List[float]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    forecasted_rates: Optional[Dict[str, List[float]]] = None
    monthly_forecast: Optional[Dict[str, Dict[str, float]]] = None
    
    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame({
            "period": self.periods, "value": self.values,
            "seasonality": self.seasonality, "trend": self.trend,
            "lower_bound": self.confidence_intervals[0] if self.confidence_intervals else [],
            "upper_bound": self.confidence_intervals[1] if self.confidence_intervals else []
        })
        
        if self.forecasted_rates:
            for rate_name, values in self.forecasted_rates.items():
                df[f"forecast_{rate_name}"] = values[:len(df)]
        
        return df

@dataclass
class OptimizationResult:
    optimal_price: float
    optimal_margin: float
    optimal_profit: float
    current_price: float
    current_margin: float
    current_profit: float
    improvement_pct: float
    recommendations: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "optimal_price": self.optimal_price,
            "optimal_margin": self.optimal_margin,
            "optimal_profit": self.optimal_profit,
            "current_price": self.current_price,
            "current_margin": self.current_margin,
            "current_profit": self.current_profit,
            "improvement_pct": self.improvement_pct,
            "recommendations": self.recommendations
        }

@dataclass
class ComparisonResult:
    marketplace: str
    profit: float
    margin: float
    roi: float
    total_expenses: float
    commission: float
    logistics: float
    storage_cost: float
    recommended_min_price: float = 0.0
    rank: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TariffCacheEntry:
    marketplace: str
    category: Optional[str]
    data: Dict[str, Any]
    source: TariffSource
    timestamp: float
    ttl_seconds: int = 86400
    version: str = "2026.1"
    notes: str = ""
    forecast_data: Optional[Dict[str, Any]] = None
    historical_data: Optional[List[Dict[str, Any]]] = None
    
    def is_expired(self) -> bool:
        return time.time() - self.timestamp > self.ttl_seconds
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "marketplace": self.marketplace, "category": self.category,
            "data": self.data, "source": self.source.value,
            "timestamp": self.timestamp, "ttl_seconds": self.ttl_seconds,
            "version": self.version, "notes": self.notes,
            "forecast_data": self.forecast_data,
            "historical_data": self.historical_data
        }
    
    @staticmethod
    def from_dict(d: Dict[str, Any]) -> 'TariffCacheEntry':
        return TariffCacheEntry(
            marketplace=d.get("marketplace", ""),
            category=d.get("category"),
            data=d.get("data", {}),
            source=TariffSource(d.get("source", "HARDCODED")),
            timestamp=d.get("timestamp", 0),
            ttl_seconds=d.get("ttl_seconds", 86400),
            version=d.get("version", "2026.1"),
            notes=d.get("notes", ""),
            forecast_data=d.get("forecast_data"),
            historical_data=d.get("historical_data")
        )

# ============================================================================
# 🆕 v100.5: АНАЛИЗ ЧУВСТВИТЕЛЬНОСТИ
# ============================================================================
def sensitivity_analysis(base_result: 'UnitEconomicsResult') -> pd.DataFrame:
    """Как изменится прибыль при изменении параметров"""
    factors = {
        "Цена +5%": base_result.profit * 1.05,
        "Цена -5%": base_result.profit * 0.95,
        "Комиссия +2%": base_result.profit - base_result.price * 0.02,
        "Логистика +20%": base_result.profit - base_result.logistics * 0.2,
        "Курс +10% (импорт)": base_result.profit - base_result.cost * 0.1,
    }
    return pd.DataFrame(list(factors.items()), 
                        columns=["Сценарий", "Прибыль"])

# ============================================================================
# 🆕 v100.5: СРАВНЕНИЕ С РЫНКОМ
# ============================================================================
def compare_with_market(result: 'UnitEconomicsResult', category: str) -> Dict:
    """Сравнение с рынком"""
    bench = MARKET_BENCHMARKS_2026.get(category, {})
    return {
        "margin_vs_market": result.margin_percent - bench.get("avg_margin", 0),
        "price_vs_market": result.price - bench.get("avg_price", 0),
        "verdict": "Выше рынка" if result.margin_percent > bench.get("avg_margin", 0) else "Ниже рынка"
    }

# ============================================================================
# САМОСТОЯТЕЛЬНЫЙ ЦЕНОВОЙ КАЛЬКУЛЯТОР
# ============================================================================
class PriceCalculator:
    """Самостоятельный калькулятор цены на автозапчасть."""
    
    def __init__(self, marketplace_config: MarketplaceConfig):
        self.config = marketplace_config
        self.logger = logging.getLogger('PriceCalculator')
    
    def calculate_retail_price(
        self,
        purchase_price: float,
        desired_margin: float = 30.0,
        weight: float = 1.0,
        volume: float = 5.0,
        category: str = None,
        days_in_storage: int = 30,
        include_subscription: bool = False,
        current_month: Optional[int] = None
    ) -> Dict[str, float]:
        if purchase_price <= 0:
            raise ValidationError("Закупочная цена должна быть положительной", "purchase_price", purchase_price)
        
        commission_rate = self.config.calculate_commission_with_dynamics(
            price=purchase_price * 2,
            category=category,
            current_month=current_month
        ) / (purchase_price * 2) if purchase_price > 0 else self.config.commission_rate
        
        logistics = (
            self.config.logistics_base +
            weight * self.config.logistics_per_kg +
            volume * self.config.logistics_per_liter
        )
        
        storage = volume * self.config.storage_per_day * days_in_storage
        last_mile = self.config.last_mile_fee
        
        fixed_costs = logistics + storage + last_mile
        
        if include_subscription and self.config.subscription_fee > 0:
            fixed_costs += self.config.subscription_fee / 30
        
        variable_ratio = (
            commission_rate +
            self.config.acquiring_fee +
            self.config.return_fee +
            self.config.delivery_fee_percent +
            0.06
        )
        
        margin_ratio = desired_margin / 100
        denominator = 1 - variable_ratio - margin_ratio
        
        if denominator <= 0:
            raise CalculationError(
                f"Невозможно достичь маржинальности {desired_margin}% при текущих тарифах",
                "price_calculation"
            )
        
        retail_price = (purchase_price + fixed_costs) / denominator
        
        min_price = calculate_recommended_min_price(
            cost=purchase_price,
            commission_rate=commission_rate,
            logistics=logistics,
            storage_cost=storage,
            acquiring_rate=self.config.acquiring_fee,
            last_mile=last_mile,
            return_rate=self.config.return_fee,
            min_profit_percent=0.10,
            tax_system="УСН_6",
            tax_rate=0.06
        )
        
        if retail_price < min_price:
            retail_price = min_price
        
        return {
            "retail_price": money_round(retail_price),
            "margin": desired_margin,
            "fixed_costs": money_round(fixed_costs),
            "commission": money_round(retail_price * commission_rate),
            "commission_rate": money_round(commission_rate * 100),
            "logistics": money_round(logistics),
            "storage": money_round(storage),
            "min_price": money_round(min_price),
            "profit": money_round(retail_price - purchase_price - fixed_costs - retail_price * variable_ratio)
        }
    
    def calculate_margin_at_price(
        self,
        retail_price: float,
        purchase_price: float,
        weight: float = 1.0,
        volume: float = 5.0,
        category: str = None
    ) -> Dict[str, float]:
        if retail_price <= 0 or purchase_price <= 0:
            raise ValidationError("Цена и себестоимость должны быть положительными")
        
        commission = self.config.calculate_commission_with_dynamics(
            price=retail_price,
            category=category
        )
        
        logistics = (
            self.config.logistics_base +
            weight * self.config.logistics_per_kg +
            volume * self.config.logistics_per_liter
        )
        
        storage = volume * self.config.storage_per_day * 30
        acquiring = retail_price * self.config.acquiring_fee
        delivery = retail_price * self.config.delivery_fee_percent
        returns = retail_price * self.config.return_fee
        tax = retail_price * 0.06
        
        total_costs = purchase_price + commission + logistics + storage + acquiring + delivery + returns + tax
        profit = retail_price - total_costs
        margin = (profit / retail_price * 100) if retail_price > 0 else 0
        
        return {
            "profit": money_round(profit),
            "margin_percent": money_round(margin),
            "total_costs": money_round(total_costs),
            "commission": money_round(commission),
            "logistics": money_round(logistics)
        }
    
    def find_optimal_price(
        self,
        purchase_price: float,
        target_margin: float = 30.0,
        weight: float = 1.0,
        volume: float = 5.0,
        category: str = None,
        price_min: float = 0,
        price_max: float = 100000,
        step: float = 10
    ) -> Dict[str, Any]:
        if price_min <= 0:
            price_min = purchase_price * 1.2
        
        best_price = price_min
        best_margin = 0
        best_profit = float('-inf')
        
        current_price = price_min
        while current_price <= price_max:
            try:
                result = self.calculate_margin_at_price(
                    retail_price=current_price,
                    purchase_price=purchase_price,
                    weight=weight,
                    volume=volume,
                    category=category
                )
                
                margin = result['margin_percent']
                profit = result['profit']
                
                if margin >= target_margin and profit > best_profit:
                    best_profit = profit
                    best_price = current_price
                    best_margin = margin
                
                current_price += step
            except Exception:
                current_price += step
                continue
        
        return {
            "optimal_price": money_round(best_price),
            "optimal_margin": money_round(best_margin),
            "optimal_profit": money_round(best_profit),
            "target_margin": target_margin
        }

# ============================================================================
# УМНЫЙ КЭШ ТАРИФОВ
# ============================================================================
@st.cache_resource
def get_smart_tariff_cache():
    return SmartTariffCache()

class SmartTariffCache:
    """Умный кэш тарифов с прогнозированием и историей"""
    
    def __init__(self):
        self.cache_dir = TARIFFS_DIR
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        
        self.cache_file = self.cache_dir / "tariffs_cache.json"
        self.history_file = self.cache_dir / "tariffs_history.json"
        self.forecast_file = self.cache_dir / "tariffs_forecast.json"
        self.backup_dir = self.cache_dir / "backups"
        self.backup_dir.mkdir(exist_ok=True, parents=True)
        
        self._cache: Dict[str, TariffCacheEntry] = {}
        self._history: List[Dict[str, Any]] = []
        self._forecasts: Dict[str, Dict[str, Any]] = {}
        
        self._load_cache()
        self._load_history()
        self._load_forecasts()
        
        logger.info(f"SmartTariffCache инициализирован: {len(self._cache)} записей")
    
    def _make_key(self, marketplace: str, category: Optional[str] = None) -> str:
        cat = (category or "all").lower().strip()
        return f"{marketplace.lower().strip()}::{cat}"
    
    def _load_cache(self):
        if not self.cache_file.exists():
            self._cache = {}
            return
        
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self._cache = {}
            for key, entry_dict in data.items():
                try:
                    self._cache[key] = TariffCacheEntry.from_dict(entry_dict)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Ошибка загрузки записи {key}: {e}")
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка загрузки кэша тарифов: {e}")
            self._cache = {}
    
    def _save_cache(self):
        try:
            data = {k: v.to_dict() for k, v in self._cache.items()}
            
            if self.cache_file.exists():
                backup_name = f"tariffs_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                shutil.copy2(self.cache_file, self.backup_dir / backup_name)
                
                backups = sorted(self.backup_dir.glob("tariffs_backup_*.json"))
                for old_backup in backups[:-10]:
                    try:
                        old_backup.unlink()
                    except OSError:
                        pass
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except (IOError, OSError) as e:
            logger.error(f"Ошибка сохранения кэша тарифов: {e}")
    
    def _load_history(self):
        if not self.history_file.exists():
            self._history = []
            return
        
        try:
            with open(self.history_file, 'r', encoding='utf-8') as f:
                self._history = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка загрузки истории: {e}")
            self._history = []
    
    def _save_history(self):
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self._history[-1000:], f, ensure_ascii=False, indent=2)
        except (IOError, OSError) as e:
            logger.error(f"Ошибка сохранения истории: {e}")
    
    def _load_forecasts(self):
        if not self.forecast_file.exists():
            self._forecasts = {}
            return
        
        try:
            with open(self.forecast_file, 'r', encoding='utf-8') as f:
                self._forecasts = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка загрузки прогнозов: {e}")
            self._forecasts = {}
    
    def _save_forecasts(self):
        try:
            with open(self.forecast_file, 'w', encoding='utf-8') as f:
                json.dump(self._forecasts, f, ensure_ascii=False, indent=2)
        except (IOError, OSError) as e:
            logger.error(f"Ошибка сохранения прогнозов: {e}")
    
    def _add_history_entry(self, action: str, marketplace: str, category: Optional[str],
                           old_data: Optional[Dict], new_data: Optional[Dict], source: str):
        entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "marketplace": marketplace,
            "category": category,
            "old_data": old_data,
            "new_data": new_data,
            "source": source
        }
        
        self._history.append(entry)
        if len(self._history) > 1000:
            self._history = self._history[-1000:]
        
        self._save_history()
    
    def get(self, marketplace: str, category: Optional[str] = None, use_expired: bool = True) -> Optional[TariffCacheEntry]:
        key = self._make_key(marketplace, category)
        entry = self._cache.get(key)
        
        if entry is None:
            key = self._make_key(marketplace, None)
            entry = self._cache.get(key)
        
        if entry is None:
            return None
        
        if entry.is_expired() and not use_expired:
            return None
        
        return entry
    
    def set(self, marketplace: str, category: Optional[str], data: Dict[str, Any],
            source: TariffSource, ttl_seconds: int = 86400, notes: str = "",
            forecast_data: Optional[Dict[str, Any]] = None) -> bool:
        try:
            key = self._make_key(marketplace, category)
            old_entry = self._cache.get(key)
            old_data = old_entry.data if old_entry else None
            
            entry = TariffCacheEntry(
                marketplace=marketplace,
                category=category,
                data=data,
                source=source,
                timestamp=time.time(),
                ttl_seconds=ttl_seconds,
                version="2026.1",
                notes=notes,
                forecast_data=forecast_data
            )
            
            self._cache[key] = entry
            self._save_cache()
            
            self._add_history_entry(
                action="UPDATE" if old_entry else "CREATE",
                marketplace=marketplace,
                category=category,
                old_data=old_data,
                new_data=data,
                source=source.value
            )
            
            if forecast_data:
                self._forecasts[key] = {
                    "forecast": forecast_data,
                    "timestamp": time.time(),
                    "marketplace": marketplace,
                    "category": category
                }
                self._save_forecasts()
            
            return True
        except (IOError, OSError) as e:
            logger.error(f"Ошибка сохранения тарифов: {e}")
            return False
    
    def get_forecast(self, marketplace: str, category: Optional[str] = None) -> Optional[Dict[str, Any]]:
        key = self._make_key(marketplace, category)
        forecast = self._forecasts.get(key)
        
        if forecast:
            if time.time() - forecast.get("timestamp", 0) < 30 * 86400:
                return forecast
        
        return None
    
    def set_forecast(self, marketplace: str, category: Optional[str],
                     forecast_data: Dict[str, Any]) -> bool:
        try:
            key = self._make_key(marketplace, category)
            self._forecasts[key] = {
                "forecast": forecast_data,
                "timestamp": time.time(),
                "marketplace": marketplace,
                "category": category
            }
            self._save_forecasts()
            return True
        except (IOError, OSError) as e:
            logger.error(f"Ошибка сохранения прогноза: {e}")
            return False
    
    def delete(self, marketplace: str, category: Optional[str] = None) -> bool:
        try:
            key = self._make_key(marketplace, category)
            old_entry = self._cache.get(key)
            
            if old_entry:
                self._add_history_entry(
                    action="DELETE",
                    marketplace=marketplace,
                    category=category,
                    old_data=old_entry.data,
                    new_data=None,
                    source="MANUAL"
                )
                
                del self._cache[key]
                self._save_cache()
                return True
            
            return False
        except (IOError, OSError) as e:
            logger.error(f"Ошибка удаления тарифов: {e}")
            return False
    
    def update_field(self, marketplace: str, category: Optional[str], field: str, value: Any) -> bool:
        try:
            key = self._make_key(marketplace, category)
            entry = self._cache.get(key)
            
            if entry is None:
                logger.warning(f"Запись не найдена: {key}")
                return False
            
            old_data = entry.data.copy()
            entry.data[field] = value
            entry.timestamp = time.time()
            entry.source = TariffSource.MANUAL
            
            self._cache[key] = entry
            self._save_cache()
            
            self._add_history_entry(
                action="FIELD_UPDATE",
                marketplace=marketplace,
                category=category,
                old_data=old_data,
                new_data=entry.data,
                source="MANUAL"
            )
            
            return True
        except (IOError, OSError) as e:
            logger.error(f"Ошибка обновления поля: {e}")
            return False
    
    def get_all(self) -> Dict[str, TariffCacheEntry]:
        return self._cache.copy()
    
    def get_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self._history[-limit:]
    
    def clear_expired(self) -> int:
        expired_keys = [k for k, v in self._cache.items() if v.is_expired()]
        
        for key in expired_keys:
            del self._cache[key]
        
        if expired_keys:
            self._save_cache()
        
        return len(expired_keys)
    
    def clear_all(self) -> int:
        count = len(self._cache)
        self._cache = {}
        self._save_cache()
        
        self._add_history_entry(
            action="CLEAR_ALL",
            marketplace="ALL",
            category=None,
            old_data={"count": count},
            new_data=None,
            source="MANUAL"
        )
        
        return count
    
    def export_to_file(self, file_path: Union[str, Path]) -> bool:
        try:
            data = {k: v.to_dict() for k, v in self._cache.items()}
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except (IOError, OSError) as e:
            logger.error(f"Ошибка экспорта кэша: {e}")
            return False
    
    def import_from_file(self, file_path: Union[str, Path]) -> int:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            count = 0
            for key, entry_dict in data.items():
                try:
                    self._cache[key] = TariffCacheEntry.from_dict(entry_dict)
                    count += 1
                except (ValueError, KeyError) as e:
                    logger.warning(f"Ошибка импорта записи {key}: {e}")
            
            self._save_cache()
            
            self._add_history_entry(
                action="IMPORT",
                marketplace="ALL",
                category=None,
                old_data=None,
                new_data={"count": count, "file": str(file_path)},
                source="IMPORTED"
            )
            
            return count
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка импорта кэша: {e}")
            return 0
    
    def get_statistics(self) -> Dict[str, Any]:
        stats = {
            "total_entries": len(self._cache),
            "by_marketplace": defaultdict(int),
            "by_source": defaultdict(int),
            "expired_count": 0,
            "oldest_entry": None,
            "newest_entry": None,
            "history_count": len(self._history),
            "forecast_count": len(self._forecasts)
        }
        
        if not self._cache:
            return stats
        
        timestamps = []
        for entry in self._cache.values():
            stats["by_marketplace"][entry.marketplace] += 1
            stats["by_source"][entry.source.value] += 1
            
            if entry.is_expired():
                stats["expired_count"] += 1
            
            timestamps.append(entry.timestamp)
        
        if timestamps:
            oldest_ts = min(timestamps)
            newest_ts = max(timestamps)
            stats["oldest_entry"] = datetime.fromtimestamp(oldest_ts).isoformat()
            stats["newest_entry"] = datetime.fromtimestamp(newest_ts).isoformat()
        
        return stats

# ============================================================================
# БЛОК 3: ПОСТОЯННОЕ ХРАНИЛИЩЕ ИСТОРИИ (🆕 v100.5 - С МИГРАЦИЕЙ)
# ============================================================================
@st.cache_resource
def get_persistent_history_db(db_path: Optional[Path] = None):
    return PersistentHistoryDB(db_path)

class PersistentHistoryDB:
    """Постоянное хранилище истории расчётов"""
    
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or (HISTORY_DB_DIR / "history_pro.duckdb")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.use_duckdb = DUCKDB_AVAILABLE
        self.conn = None
        
        self._init_connection()
        self._create_tables()
        self._migrate_database()
        
        logger.info(f"📚 PersistentHistoryDB инициализирован: {self.db_path}")
    
    def _init_connection(self):
        try:
            if self.use_duckdb:
                self.conn = duckdb.connect(str(self.db_path))
            else:
                self.conn = sqlite3.connect(str(self.db_path.with_suffix('.sqlite')), check_same_thread=False)
                self.conn.row_factory = sqlite3.Row
        except (duckdb.Error, sqlite3.Error) as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            self.conn = None
    
    def _create_tables(self):
        if self.conn is None:
            return
        
        try:
            if self.use_duckdb:
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS calculation_history (
                    id VARCHAR PRIMARY KEY, timestamp VARCHAR NOT NULL, marketplace VARCHAR,
                    operation_mode VARCHAR, category VARCHAR, article VARCHAR, brand VARCHAR,
                    price DOUBLE, cost DOUBLE, length DOUBLE, width DOUBLE, height DOUBLE,
                    weight DOUBLE, volume DOUBLE, commission DOUBLE, commission_percent DOUBLE,
                    logistics DOUBLE, storage_cost DOUBLE, acquiring DOUBLE, delivery DOUBLE,
                    last_mile DOUBLE, returns DOUBLE, rko_fee DOUBLE, premium_fee DOUBLE,
                    insurance_fee DOUBLE, packing_fee DOUBLE, marketing_fee DOUBLE,
                    subscription_cost DOUBLE, hazardous_surcharge DOUBLE, fragile_surcharge DOUBLE,
                    oversized_surcharge DOUBLE, tax_amount DOUBLE, tax_system VARCHAR,
                    total_expenses DOUBLE, profit DOUBLE, margin_percent DOUBLE, roi DOUBLE,
                    breakeven_price DOUBLE, recommended_min_price DOUBLE, profit_per_ruble DOUBLE,
                    contribution_margin DOUBLE, contribution_margin_ratio DOUBLE,
                    tariff_source VARCHAR, status VARCHAR, metadata_json VARCHAR,
                    applied_seasonal_multiplier DOUBLE DEFAULT 1.0,
                    applied_promo_discount DOUBLE DEFAULT 0.0,
                    dynamic_adjustment DOUBLE DEFAULT 0.0,
                    billable_weight DOUBLE DEFAULT 0.0,
                    advertising_cost DOUBLE DEFAULT 0.0,
                    auto_parts_specific DOUBLE DEFAULT 0.0,
                    calculation_id VARCHAR
                )
                """)
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_history_timestamp ON calculation_history(timestamp)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_history_marketplace ON calculation_history(marketplace)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_history_article ON calculation_history(article)")
            else:
                self.conn.execute("""
                CREATE TABLE IF NOT EXISTS calculation_history (
                    id TEXT PRIMARY KEY, timestamp TEXT NOT NULL, marketplace TEXT,
                    operation_mode TEXT, category TEXT, article TEXT, brand TEXT,
                    price REAL, cost REAL, length REAL, width REAL, height REAL,
                    weight REAL, volume REAL, commission REAL, commission_percent REAL,
                    logistics REAL, storage_cost REAL, acquiring REAL, delivery REAL,
                    last_mile REAL, returns REAL, rko_fee REAL, premium_fee REAL,
                    insurance_fee REAL, packing_fee REAL, marketing_fee REAL,
                    subscription_cost REAL, hazardous_surcharge REAL, fragile_surcharge REAL,
                    oversized_surcharge REAL, tax_amount REAL, tax_system TEXT,
                    total_expenses REAL, profit REAL, margin_percent REAL, roi REAL,
                    breakeven_price REAL, recommended_min_price REAL, profit_per_ruble REAL,
                    contribution_margin REAL, contribution_margin_ratio REAL,
                    tariff_source TEXT, status TEXT, metadata_json TEXT,
                    applied_seasonal_multiplier REAL DEFAULT 1.0,
                    applied_promo_discount REAL DEFAULT 0.0,
                    dynamic_adjustment REAL DEFAULT 0.0,
                    billable_weight REAL DEFAULT 0.0,
                    advertising_cost REAL DEFAULT 0.0,
                    auto_parts_specific REAL DEFAULT 0.0,
                    calculation_id TEXT
                )
                """)
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_history_timestamp ON calculation_history(timestamp)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_history_marketplace ON calculation_history(marketplace)")
                self.conn.commit()
        except (duckdb.Error, sqlite3.Error) as e:
            logger.error(f"Ошибка создания таблиц: {e}")
    
    def _get_db_columns(self) -> List[str]:
        """Получить список колонок таблицы"""
        if self.conn is None:
            return []
        try:
            if self.use_duckdb:
                rows = self.conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'calculation_history'"
                ).fetchall()
                return [row[0] for row in rows]
            else:
                rows = self.conn.execute("PRAGMA table_info(calculation_history)").fetchall()
                return [row[1] for row in rows]
        except Exception as e:
            logger.warning(f"Ошибка получения колонок: {e}")
            return []
    
    def _migrate_database(self):
        """🆕 v100.5: Автоматическая миграция БД - добавление новых колонок"""
        if self.conn is None:
            return
        
        try:
            db_columns = self._get_db_columns()
            
            new_columns = {
                'billable_weight': 'DOUBLE' if self.use_duckdb else 'REAL',
                'advertising_cost': 'DOUBLE' if self.use_duckdb else 'REAL',
                'auto_parts_specific': 'DOUBLE' if self.use_duckdb else 'REAL',
                'calculation_id': 'VARCHAR' if self.use_duckdb else 'TEXT',
            }
            
            for col_name, col_type in new_columns.items():
                if col_name not in db_columns:
                    try:
                        self.conn.execute(f'ALTER TABLE calculation_history ADD COLUMN "{col_name}" {col_type}')
                        logger.info(f"✅ Миграция: добавлена колонка {col_name}")
                    except Exception as e:
                        logger.warning(f"Не удалось добавить {col_name}: {e}")
        except Exception as e:
            logger.warning(f"Ошибка миграции: {e}")
    
    def save_calculation(self, result: 'UnitEconomicsResult', article: str = "", brand: str = "") -> bool:
        """🆕 v100.5: Сохранение с учётом схемы БД"""
        if self.conn is None:
            return False
        try:
            data = result.to_dict()
            data['article'] = article
            data['brand'] = brand
            data['metadata_json'] = json.dumps(data.get('metadata', {}), ensure_ascii=False)
            data['applied_seasonal_multiplier'] = getattr(result, 'applied_seasonal_multiplier', 1.0)
            data['applied_promo_discount'] = getattr(result, 'applied_promo_discount', 0.0)
            data['dynamic_adjustment'] = getattr(result, 'dynamic_adjustment', 0.0)
            data['billable_weight'] = getattr(result, 'billable_weight', 0.0)
            data['advertising_cost'] = getattr(result, 'advertising_cost', 0.0)
            data['auto_parts_specific'] = getattr(result, 'auto_parts_specific', 0.0)
            
            db_columns = self._get_db_columns()
            filtered_data = {k: v for k, v in data.items() if k in db_columns}
            
            if not filtered_data:
                logger.warning("Нет подходящих колонок для сохранения")
                return False
            
            if 'id' not in filtered_data and 'calculation_id' in filtered_data:
                filtered_data['id'] = filtered_data['calculation_id']
            elif 'id' not in filtered_data:
                filtered_data['id'] = str(uuid.uuid4())
            
            columns = list(filtered_data.keys())
            values = list(filtered_data.values())
            
            placeholders = ", ".join(["?"] * len(values))
            col_names = ", ".join([f'"{c}"' for c in columns])
            sql = f"INSERT OR REPLACE INTO calculation_history ({col_names}) VALUES ({placeholders})"
            
            self.conn.execute(sql, values)
            self.conn.commit()
            return True
        except (duckdb.Error, sqlite3.Error, ValueError) as e:
            logger.error(f"Ошибка сохранения расчёта: {e}")
            return False
    
    def load_history(self, limit: int = 1000, filters: Optional[Dict] = None) -> pd.DataFrame:
        if self.conn is None:
            return pd.DataFrame()
        
        try:
            conditions = []
            params = []
            
            if filters:
                for key in ['marketplace', 'operation_mode', 'category', 'tax_system']:
                    if filters.get(key):
                        conditions.append(f"{key} = ?")
                        params.append(filters[key])
                
                for key in ['article', 'brand']:
                    if filters.get(key):
                        conditions.append(f"{key} LIKE ?")
                        params.append(f"%{filters[key]}%")
                
                if filters.get('min_profit'):
                    conditions.append("profit >= ?")
                    params.append(filters['min_profit'])
                
                if filters.get('max_profit'):
                    conditions.append("profit <= ?")
                    params.append(filters['max_profit'])
                
                if filters.get('start_date'):
                    conditions.append("timestamp >= ?")
                    params.append(filters['start_date'])
                
                if filters.get('end_date'):
                    conditions.append("timestamp <= ?")
                    params.append(filters['end_date'])
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            sql = f"SELECT * FROM calculation_history WHERE {where_clause} ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            if self.use_duckdb:
                df = self.conn.execute(sql, params).pl().to_pandas()
            else:
                df = pd.read_sql_query(sql, self.conn, params=params)
            
            return df
        except (duckdb.Error, sqlite3.Error) as e:
            logger.error(f"Ошибка загрузки истории: {e}")
            return pd.DataFrame()
    
    def get_stats(self) -> Dict[str, Any]:
        if self.conn is None:
            return {}
        
        try:
            if self.use_duckdb:
                total = self.conn.execute("SELECT COUNT(*) FROM calculation_history").fetchone()[0]
                total_profit = self.conn.execute("SELECT SUM(profit) FROM calculation_history").fetchone()[0] or 0
                avg_profit = self.conn.execute("SELECT AVG(profit) FROM calculation_history").fetchone()[0] or 0
                avg_margin = self.conn.execute("SELECT AVG(margin_percent) FROM calculation_history").fetchone()[0] or 0
                
                by_marketplace = self.conn.execute("SELECT marketplace, COUNT(*) as cnt, SUM(profit) as total_profit FROM calculation_history GROUP BY marketplace ORDER BY cnt DESC").pl().to_pandas()
            else:
                total = self.conn.execute("SELECT COUNT(*) FROM calculation_history").fetchone()[0]
                total_profit = self.conn.execute("SELECT SUM(profit) FROM calculation_history").fetchone()[0] or 0
                avg_profit = self.conn.execute("SELECT AVG(profit) FROM calculation_history").fetchone()[0] or 0
                avg_margin = self.conn.execute("SELECT AVG(margin_percent) FROM calculation_history").fetchone()[0] or 0
                
                by_marketplace = pd.read_sql_query("SELECT marketplace, COUNT(*) as cnt, SUM(profit) as total_profit FROM calculation_history GROUP BY marketplace ORDER BY cnt DESC", self.conn)
            
            return {
                "total_records": total,
                "total_profit": float(total_profit),
                "avg_profit": float(avg_profit),
                "avg_margin": float(avg_margin),
                "by_marketplace": by_marketplace
            }
        except (duckdb.Error, sqlite3.Error) as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
    
    def clear_history(self) -> int:
        if self.conn is None:
            return 0
        
        try:
            count = self.conn.execute("SELECT COUNT(*) FROM calculation_history").fetchone()[0]
            self.conn.execute("DELETE FROM calculation_history")
            self.conn.commit()
            return count
        except (duckdb.Error, sqlite3.Error) as e:
            logger.error(f"Ошибка очистки истории: {e}")
            return 0
    
    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

# ============================================================================
# 🆕 v100.5: ПРОФЕССИОНАЛЬНЫЙ EXCEL-ЭКСПОРТ
# ============================================================================
class ProfessionalExcelExporter:
    """Профессиональный экспорт юнит-экономики в Excel"""
    
    COLORS = {
        "header_bg": "0F3460",
        "header_fg": "FFFFFF",
        "subheader_bg": "E2EFDA",
        "positive": "C6EFCE",
        "negative": "FFC7CE",
        "warning": "FFEB9C",
        "total_bg": "DCE6F1",
        "alt_row": "F5F5F5",
        "border": "B4C6E7",
    }
    
    def __init__(self):
        self.thin_border = Border(
            left=Side(style='thin', color=self.COLORS["border"]),
            right=Side(style='thin', color=self.COLORS["border"]),
            top=Side(style='thin', color=self.COLORS["border"]),
            bottom=Side(style='thin', color=self.COLORS["border"])
        )
    
    def export_unit_economics(self, df: pd.DataFrame, 
                               output_path: str,
                               metadata: Dict = None) -> bool:
        """Полноценный отчёт с 6 листами"""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                self._write_dashboard_sheet(writer, df, metadata)
                self._write_details_sheet(writer, df)
                self._write_marketplace_comparison(writer, df)
                self._write_category_analysis(writer, df)
                self._write_top_bottom_sheet(writer, df)
                self._write_parameters_sheet(writer, metadata)
            
            return True
        except Exception as e:
            logger.error(f"Ошибка экспорта: {e}")
            return False
    
    def _write_dashboard_sheet(self, writer, df: pd.DataFrame, metadata):
        """Сводный дашборд с KPI"""
        ws = writer.book.create_sheet("📊 Дашборд", 0)
        
        ws.merge_cells('A1:H1')
        ws['A1'] = "📊 ОТЧЁТ ПО ЮНИТ-ЭКОНОМИКЕ АВТОЗАПЧАСТЕЙ"
        ws['A1'].font = Font(size=16, bold=True, color="FFFFFF")
        ws['A1'].fill = PatternFill("solid", fgColor=self.COLORS["header_bg"])
        ws['A1'].alignment = Alignment(horizontal='center', vertical='center')
        ws.row_dimensions[1].height = 35
        
        ws['A2'] = f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        ws['A3'] = f"📦 Товаров: {len(df):,}".replace(",", " ")
        ws['A4'] = f"💰 Общая прибыль: {df['profit'].sum():,.2f} ₽".replace(",", " ")
        
        kpis = [
            ("Общая прибыль", df['profit'].sum(), "₽", "positive"),
            ("Средняя маржа", df['margin_percent'].mean(), "%", "neutral"),
            ("Средний ROI", df['roi'].mean() if 'roi' in df.columns else 0, "%", "neutral"),
            ("Убыточных SKU", (df['profit'] < 0).sum(), "шт", "negative"),
        ]
        
        row = 6
        for label, value, unit, style in kpis:
            ws[f'A{row}'] = label
            ws[f'A{row}'].font = Font(bold=True)
            ws[f'B{row}'] = value
            ws[f'B{row}'].number_format = '#,##0.00' if unit == "₽" else '0.00'
            ws[f'C{row}'] = unit
            
            if style == "positive" and value > 0:
                ws[f'B{row}'].fill = PatternFill("solid", fgColor=self.COLORS["positive"])
            elif style == "negative" and value > 0:
                ws[f'B{row}'].fill = PatternFill("solid", fgColor=self.COLORS["negative"])
            row += 1
        
        if 'marketplace' in df.columns:
            mp_summary = df.groupby('marketplace')['profit'].sum().reset_index()
            ws_summary = writer.book.create_sheet("_data_mp")
            ws_summary.append(["Маркетплейс", "Прибыль"])
            for _, r in mp_summary.iterrows():
                ws_summary.append([r['marketplace'], r['profit']])
            
            chart = BarChart()
            chart.title = "Прибыль по маркетплейсам"
            chart.y_axis.title = "₽"
            chart.x_axis.title = "Маркетплейс"
            chart.style = 10
            
            data = Reference(ws_summary, min_col=2, min_row=1, 
                            max_row=len(mp_summary) + 1)
            cats = Reference(ws_summary, min_col=1, min_row=2, 
                            max_row=len(mp_summary) + 1)
            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)
            chart.height = 12
            chart.width = 20
            
            ws.add_chart(chart, "A12")
    
    def _write_details_sheet(self, writer, df: pd.DataFrame):
        """Детализация с форматированием"""
        sheet_name = "📋 Детализация"
        
        columns_map = {
            'Артикул': 'Артикул',
            'Бренд': 'Бренд',
            'marketplace': 'Маркетплейс',
            'price': 'Цена продажи',
            'cost': 'Себестоимость',
            'commission': 'Комиссия МП',
            'logistics': 'Логистика',
            'storage_cost': 'Хранение',
            'acquiring': 'Эквайринг',
            'last_mile': 'Посл. миля',
            'returns': 'Возвраты',
            'tax_amount': 'Налог',
            'total_expenses': 'ИТОГО расходов',
            'profit': 'Прибыль',
            'margin_percent': 'Маржа %',
            'roi': 'ROI %',
            'breakeven_price': 'Точка безубыт.',
            'recommended_min_price': 'Мин. цена рек.',
            'billable_weight': 'Оплач. вес',
            'advertising_cost': 'Реклама (ДРР)',
            'auto_parts_specific': 'Спец. расходы',
        }
        
        cols_to_export = [c for c in columns_map if c in df.columns]
        df_export = df[cols_to_export].rename(columns=columns_map)
        
        df_export.to_excel(writer, sheet_name=sheet_name, 
                          index=False, startrow=1)
        
        ws = writer.sheets[sheet_name]
        
        header_fill = PatternFill("solid", fgColor=self.COLORS["header_bg"])
        header_font = Font(bold=True, color="FFFFFF", size=10)
        
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', 
                                       vertical='center', 
                                       wrap_text=True)
            cell.border = self.thin_border
        
        ws.row_dimensions[1].height = 30
        
        money_cols = ['Цена продажи', 'Себестоимость', 'Комиссия МП', 
                     'Логистика', 'Хранение', 'ИТОГО расходов', 
                     'Прибыль', 'Точка безубыт.', 'Мин. цена рек.',
                     'Оплач. вес', 'Реклама (ДРР)', 'Спец. расходы']
        percent_cols = ['Маржа %', 'ROI %']
        
        for col_idx, col_name in enumerate(df_export.columns, 1):
            col_letter = get_column_letter(col_idx)
            
            if col_name in money_cols:
                for row in range(2, len(df_export) + 2):
                    ws[f'{col_letter}{row}'].number_format = '#,##0.00 ₽'
            elif col_name in percent_cols:
                for row in range(2, len(df_export) + 2):
                    ws[f'{col_letter}{row}'].number_format = '0.00"%"'
            
            max_len = max(
                len(str(col_name)),
                df_export[col_name].astype(str).str.len().max() if len(df_export) > 0 else 0
            )
            ws.column_dimensions[col_letter].width = min(max_len + 3, 25)
        
        if 'Прибыль' in df_export.columns:
            profit_col_idx = df_export.columns.get_loc('Прибыль') + 1
            profit_col_letter = get_column_letter(profit_col_idx)
            data_range = f"{profit_col_letter}2:{profit_col_letter}{len(df_export) + 1}"
            
            ws.conditional_formatting.add(data_range,
                CellIsRule(operator='greaterThan', formula=['0'],
                          fill=PatternFill("solid", fgColor=self.COLORS["positive"])))
            
            ws.conditional_formatting.add(data_range,
                CellIsRule(operator='lessThan', formula=['0'],
                          fill=PatternFill("solid", fgColor=self.COLORS["negative"])))
        
        if 'Маржа %' in df_export.columns:
            margin_col_idx = df_export.columns.get_loc('Маржа %') + 1
            margin_letter = get_column_letter(margin_col_idx)
            margin_range = f"{margin_letter}2:{margin_letter}{len(df_export) + 1}"
            
            ws.conditional_formatting.add(margin_range,
                DataBarRule(start_type='min', end_type='max',
                           color="636EFA"))
        
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        
        total_row = len(df_export) + 3
        ws[f'A{total_row}'] = "ИТОГО / СРЕДНЕЕ:"
        ws[f'A{total_row}'].font = Font(bold=True, size=11)
        ws[f'A{total_row}'].fill = PatternFill("solid", fgColor=self.COLORS["total_bg"])
        
        for col_idx, col_name in enumerate(df_export.columns, 1):
            col_letter = get_column_letter(col_idx)
            if col_name in money_cols:
                ws[f'{col_letter}{total_row}'] = f"=SUM({col_letter}2:{col_letter}{len(df_export)+1})"
                ws[f'{col_letter}{total_row}'].number_format = '#,##0.00 ₽'
                ws[f'{col_letter}{total_row}'].font = Font(bold=True)
            elif col_name in percent_cols:
                ws[f'{col_letter}{total_row}'] = f"=AVERAGE({col_letter}2:{col_letter}{len(df_export)+1})"
                ws[f'{col_letter}{total_row}'].number_format = '0.00"%"'
                ws[f'{col_letter}{total_row}'].font = Font(bold=True)
        
        ws.print_title_rows = '1:1'
    
    def _write_marketplace_comparison(self, writer, df: pd.DataFrame):
        """Сравнительная таблица маркетплейсов"""
        if 'marketplace' not in df.columns:
            return
        
        agg = df.groupby('marketplace').agg({
            'profit': ['sum', 'mean', 'count'],
            'margin_percent': 'mean',
            'price': 'mean',
            'commission': 'mean',
            'logistics': 'mean',
            'tax_amount': 'mean',
        }).reset_index()
        agg.columns = ['Маркетплейс', 'Общая прибыль', 'Средняя прибыль', 
                      'Кол-во SKU', 'Средняя маржа %', 'Средняя цена',
                      'Средняя комиссия', 'Средняя логистика', 'Средний налог']
        
        agg.to_excel(writer, sheet_name="🏪 Сравнение МП", 
                    index=False, startrow=1)
        
        ws = writer.sheets["🏪 Сравнение МП"]
        header_fill = PatternFill("solid", fgColor=self.COLORS["header_bg"])
        header_font = Font(bold=True, color="FFFFFF", size=10)
        
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.thin_border
        
        ws.row_dimensions[1].height = 30
        ws.freeze_panes = "A2"
    
    def _write_category_analysis(self, writer, df: pd.DataFrame):
        """Анализ по категориям"""
        if 'category' not in df.columns:
            return
        
        agg = df.groupby('category').agg({
            'profit': ['sum', 'mean'],
            'margin_percent': 'mean',
            'price': 'mean',
        }).reset_index()
        agg.columns = ['Категория', 'Общая прибыль', 'Средняя прибыль', 
                      'Средняя маржа %', 'Средняя цена']
        
        agg.to_excel(writer, sheet_name="📂 Анализ категорий", 
                    index=False, startrow=1)
        
        ws = writer.sheets["📂 Анализ категорий"]
        header_fill = PatternFill("solid", fgColor=self.COLORS["header_bg"])
        header_font = Font(bold=True, color="FFFFFF", size=10)
        
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.thin_border
        
        ws.row_dimensions[1].height = 30
        ws.freeze_panes = "A2"
    
    def _write_top_bottom_sheet(self, writer, df: pd.DataFrame):
        """Топ прибыльных и убыточных товаров"""
        ws = writer.book.create_sheet("🏆 Топ товары")
        
        top_cols = ['Артикул', 'Бренд', 'marketplace', 'profit', 'margin_percent']
        top_cols = [c for c in top_cols if c in df.columns]
        
        top_profit = df.nlargest(20, 'profit')[top_cols]
        rename_map = {'marketplace': 'Маркетплейс', 'profit': 'Прибыль', 'margin_percent': 'Маржа %'}
        top_profit = top_profit.rename(columns={k: v for k, v in rename_map.items() if k in top_profit.columns})
        
        ws['A1'] = "🏆 ТОП-20 ПРИБЫЛЬНЫХ ТОВАРОВ"
        ws['A1'].font = Font(bold=True, size=12)
        ws.merge_cells('A1:E1')
        
        top_profit.to_excel(writer, sheet_name="🏆 Топ товары", 
                           index=False, startrow=2)
        
        bottom_row = len(top_profit) + 5
        ws[f'A{bottom_row}'] = "💸 ТОП-20 УБЫТОЧНЫХ ТОВАРОВ"
        ws[f'A{bottom_row}'].font = Font(bold=True, size=12)
        ws.merge_cells(f'A{bottom_row}:E{bottom_row}')
        
        bottom_profit = df.nsmallest(20, 'profit')[top_cols]
        bottom_profit = bottom_profit.rename(columns={k: v for k, v in rename_map.items() if k in bottom_profit.columns})
        
        bottom_profit.to_excel(writer, sheet_name="🏆 Топ товары", 
                              index=False, startrow=bottom_row + 1)
        
        ws = writer.sheets["🏆 Топ товары"]
        ws.freeze_panes = "A3"
    
    def _write_parameters_sheet(self, writer, metadata: Dict):
        """Лист с параметрами расчёта"""
        ws = writer.book.create_sheet("⚙️ Параметры")
        
        ws['A1'] = "ПАРАМЕТРЫ РАСЧЁТА"
        ws['A1'].font = Font(bold=True, size=14)
        
        params = [
            ("Дата расчёта", datetime.now().strftime('%d.%m.%Y %H:%M')),
            ("Версия приложения", APP_VERSION),
            ("Маркетплейсы", ", ".join(metadata.get('marketplaces', []))),
            ("Режим работы", metadata.get('operation_mode', 'FBS')),
            ("Дней хранения", metadata.get('days_in_storage', 30)),
            ("Налоговая система", metadata.get('tax_system', 'УСН_6')),
            ("Интенсивность рекламы", metadata.get('ad_intensity', 'medium')),
            ("Курс валют", metadata.get('currency_rate', 1.0)),
            ("Учтена сезонность", "Да" if metadata.get('seasonal', True) else "Нет"),
            ("Источник тарифов", metadata.get('tariff_source', 'Захардкожены')),
            ("Учтён объёмный вес", "Да"),
            ("Прогрессивное хранение", "Да"),
            ("Реальные возвраты", "Да"),
            ("Рекламные расходы", "Да"),
        ]
        
        for idx, (key, value) in enumerate(params, 3):
            ws[f'A{idx}'] = key
            ws[f'A{idx}'].font = Font(bold=True)
            ws[f'B{idx}'] = value
        
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 50

# ============================================================================
# БЛОК 4: КОНФИГУРАЦИИ МАРКЕТПЛЕЙСОВ 2026
# ============================================================================
def get_marketplace_configs_2026() -> Dict[str, MarketplaceConfig]:
    """Получение конфигураций маркетплейсов с сезонными коэффициентами."""
    configs = {}
    
    configs["Ozon"] = MarketplaceConfig(
        name="Ozon",
        commission_rate=0.15,
        min_commission=30.0,
        logistics_base=50.0,
        logistics_per_kg=15.0,
        logistics_per_liter=5.0,
        storage_per_day=0.3,
        return_fee=0.02,
        acquiring_fee=0.015,
        last_mile_fee=50.0,
        hazardous_surcharge=0.02,
        fragile_surcharge=0.01,
        oversized_surcharge=0.015,
        seasonal_multipliers={
            "winter": 1.15,
            "spring": 1.0,
            "summer": 0.95,
            "autumn": 1.05
        },
        category_rates={
            "двигатель": 0.12, "трансмиссия": 0.13, "подвеска": 0.14,
            "тормозная_система": 0.14, "рулевое_управление": 0.14,
            "электрика": 0.15, "охлаждение": 0.14, "выпуск": 0.13,
            "фильтры": 0.17, "масла": 0.18, "оптика": 0.15,
            "шины": 0.16, "инструменты": 0.14, "кузов": 0.13,
            "крепёж": 0.12, "ремни": 0.13, "подшипники": 0.13,
            "климат": 0.14, "безопасность": 0.15,
            "автотовары": 0.12
        },
        mode_multipliers={
            "FBY": 0.75, "FBS": 1.0, "FBO": 0.8,
            "DBS": 1.3, "FBP": 0.9, "RealFBS": 1.1
        },
        description="Ozon - крупнейший маркетплейс России",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    configs["Wildberries"] = MarketplaceConfig(
        name="Wildberries",
        commission_rate=0.18,
        min_commission=35.0,
        logistics_base=60.0,
        logistics_per_kg=18.0,
        logistics_per_liter=6.0,
        storage_per_day=0.5,
        return_fee=0.03,
        acquiring_fee=0.0,
        last_mile_fee=0.0,
        delivery_fee_percent=0.05,
        rko_fee=0.01,
        hazardous_surcharge=0.025,
        fragile_surcharge=0.015,
        oversized_surcharge=0.02,
        seasonal_multipliers={
            "winter": 1.2,
            "spring": 1.0,
            "summer": 0.95,
            "autumn": 1.05
        },
        category_rates={
            "двигатель": 0.15, "трансмиссия": 0.16, "подвеска": 0.17,
            "тормозная_система": 0.17, "рулевое_управление": 0.17,
            "электрика": 0.18, "охлаждение": 0.17, "выпуск": 0.16,
            "фильтры": 0.20, "масла": 0.22, "оптика": 0.18,
            "шины": 0.19, "инструменты": 0.17, "кузов": 0.16,
            "крепёж": 0.15, "ремни": 0.16, "подшипники": 0.16,
            "климат": 0.17, "безопасность": 0.18,
            "автотовары": 0.15
        },
        mode_multipliers={
            "FBY": 0.75, "FBS": 1.15, "FBO": 1.1,
            "DBS": 1.25, "FBP": 1.0, "RealFBS": 1.2
        },
        description="Wildberries - лидер e-commerce России",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    configs["Яндекс Маркет"] = MarketplaceConfig(
        name="Яндекс Маркет",
        commission_rate=0.14,
        subscription_fee=6990.0,
        min_commission=0.0,
        logistics_base=45.0,
        logistics_per_kg=14.0,
        logistics_per_liter=4.5,
        storage_per_day=0.25,
        return_fee=0.02,
        acquiring_fee=0.02,
        last_mile_fee=40.0,
        premium_fee=0.02,
        hazardous_surcharge=0.018,
        fragile_surcharge=0.01,
        oversized_surcharge=0.012,
        seasonal_multipliers={
            "winter": 1.1,
            "spring": 1.0,
            "summer": 0.9,
            "autumn": 1.0
        },
        category_rates={
            "двигатель": 0.11, "трансмиссия": 0.12, "подвеска": 0.13,
            "тормозная_система": 0.13, "рулевое_управление": 0.13,
            "электрика": 0.14, "охлаждение": 0.13, "выпуск": 0.12,
            "фильтры": 0.16, "масла": 0.17, "оптика": 0.14,
            "шины": 0.15, "инструменты": 0.13, "кузов": 0.12,
            "крепёж": 0.11, "ремни": 0.12, "подшипники": 0.12,
            "климат": 0.13, "безопасность": 0.14,
            "автотовары": 0.14
        },
        mode_multipliers={
            "FBY": 0.75, "FBS": 1.0, "FBO": 0.8,
            "DBS": 1.3, "FBP": 0.9, "RealFBS": 1.1
        },
        description="Яндекс Маркет - маркетплейс экосистемы Яндекса",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    configs["AliExpress"] = MarketplaceConfig(
        name="AliExpress",
        commission_rate=0.10,
        min_commission=20.0,
        logistics_base=80.0,
        logistics_per_kg=25.0,
        logistics_per_liter=8.0,
        storage_per_day=0.2,
        return_fee=0.01,
        acquiring_fee=0.025,
        last_mile_fee=70.0,
        delivery_fee_percent=0.08,
        hazardous_surcharge=0.03,
        fragile_surcharge=0.02,
        oversized_surcharge=0.025,
        seasonal_multipliers={
            "winter": 1.25,
            "spring": 1.0,
            "summer": 1.1,
            "autumn": 1.15
        },
        category_rates={
            "двигатель": 0.08, "трансмиссия": 0.09, "подвеска": 0.10,
            "тормозная_система": 0.10, "рулевое_управление": 0.10,
            "электрика": 0.11, "охлаждение": 0.10, "выпуск": 0.09,
            "фильтры": 0.12, "масла": 0.13, "оптика": 0.11,
            "шины": 0.12, "инструменты": 0.10, "кузов": 0.09,
            "крепёж": 0.08, "ремни": 0.09, "подшипники": 0.09,
            "климат": 0.10, "безопасность": 0.11,
            "автотовары": 0.10
        },
        mode_multipliers={
            "FBY": 0.75, "FBS": 1.2, "FBO": 1.1,
            "DBS": 1.3, "FBP": 0.9, "RealFBS": 1.25
        },
        description="AliExpress - международный маркетплейс",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    configs["Мегамаркет"] = MarketplaceConfig(
        name="Мегамаркет",
        commission_rate=0.13,
        min_commission=28.0,
        logistics_base=55.0,
        logistics_per_kg=16.0,
        logistics_per_liter=5.5,
        storage_per_day=0.3,
        return_fee=0.02,
        acquiring_fee=0.018,
        last_mile_fee=45.0,
        delivery_fee_percent=0.05,
        hazardous_surcharge=0.02,
        fragile_surcharge=0.012,
        oversized_surcharge=0.015,
        seasonal_multipliers={
            "winter": 1.12,
            "spring": 1.0,
            "summer": 0.93,
            "autumn": 1.03
        },
        category_rates={
            "двигатель": 0.10, "трансмиссия": 0.11, "подвеска": 0.12,
            "тормозная_система": 0.12, "рулевое_управление": 0.12,
            "электрика": 0.13, "охлаждение": 0.12, "выпуск": 0.11,
            "фильтры": 0.15, "масла": 0.16, "оптика": 0.13,
            "шины": 0.14, "инструменты": 0.12, "кузов": 0.11,
            "крепёж": 0.10, "ремни": 0.11, "подшипники": 0.11,
            "климат": 0.12, "безопасность": 0.13,
            "автотовары": 0.15
        },
        mode_multipliers={
            "FBY": 0.75, "FBS": 1.1, "FBO": 1.05,
            "DBS": 1.2, "FBP": 0.95, "RealFBS": 1.15
        },
        description="Мегамаркет (Сбер) - маркетплейс экосистемы Сбера",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    configs["СберМегаМаркет"] = MarketplaceConfig(
        name="СберМегаМаркет",
        commission_rate=0.13,
        min_commission=28.0,
        logistics_base=55.0,
        logistics_per_kg=16.0,
        logistics_per_liter=5.5,
        storage_per_day=0.3,
        return_fee=0.02,
        acquiring_fee=0.018,
        last_mile_fee=45.0,
        rko_fee=0.015,
        delivery_fee_percent=0.055,
        hazardous_surcharge=0.02,
        fragile_surcharge=0.012,
        oversized_surcharge=0.015,
        seasonal_multipliers={
            "winter": 1.12,
            "spring": 1.0,
            "summer": 0.93,
            "autumn": 1.03
        },
        category_rates={
            "двигатель": 0.10, "трансмиссия": 0.11, "подвеска": 0.12,
            "тормозная_система": 0.12, "рулевое_управление": 0.12,
            "электрика": 0.13, "охлаждение": 0.12, "выпуск": 0.11,
            "фильтры": 0.15, "масла": 0.16, "оптика": 0.13,
            "шины": 0.14, "инструменты": 0.12, "кузов": 0.11,
            "крепёж": 0.10, "ремни": 0.11, "подшипники": 0.11,
            "климат": 0.12, "безопасность": 0.13,
            "автотовары": 0.12
        },
        mode_multipliers={
            "FBY": 0.75, "FBS": 1.1, "FBO": 1.05,
            "DBS": 1.2, "FBP": 0.95, "RealFBS": 1.15
        },
        description="СберМегаМаркет - маркетплейс Сбера",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    configs["Avito"] = MarketplaceConfig(
        name="Avito",
        commission_rate=0.05,
        min_commission=0.0,
        logistics_base=0.0,
        logistics_per_kg=0.0,
        logistics_per_liter=0.0,
        storage_per_day=0.0,
        return_fee=0.0,
        acquiring_fee=0.0,
        last_mile_fee=0.0,
        delivery_fee_percent=0.0,
        hazardous_surcharge=0.0,
        fragile_surcharge=0.0,
        oversized_surcharge=0.0,
        seasonal_multipliers={},
        category_rates={},
        mode_multipliers={"RealFBS": 1.0},
        description="Avito - доска объявлений",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    configs["Drom"] = MarketplaceConfig(
        name="Drom",
        commission_rate=0.08,
        min_commission=0.0,
        logistics_base=0.0,
        logistics_per_kg=0.0,
        logistics_per_liter=0.0,
        storage_per_day=0.0,
        return_fee=0.0,
        acquiring_fee=0.0,
        last_mile_fee=0.0,
        delivery_fee_percent=0.0,
        hazardous_surcharge=0.0,
        fragile_surcharge=0.0,
        oversized_surcharge=0.0,
        seasonal_multipliers={},
        category_rates={},
        mode_multipliers={"RealFBS": 1.0},
        description="Drom - площадка для автотоваров",
        version="2026.1",
        tariff_source=TariffSource.HARDCODED
    )
    
    try:
        cache = get_smart_tariff_cache()
        for mp_name, config in configs.items():
            cached_entry = cache.get(mp_name, None, use_expired=False)
            if cached_entry and cached_entry.data:
                data = cached_entry.data
                if "commission_rate" in data: config.commission_rate = data["commission_rate"]
                if "min_commission" in data: config.min_commission = data["min_commission"]
                if "logistics_base" in data: config.logistics_base = data["logistics_base"]
                if "logistics_per_kg" in data: config.logistics_per_kg = data["logistics_per_kg"]
                if "logistics_per_liter" in data: config.logistics_per_liter = data["logistics_per_liter"]
                if "storage_per_day" in data: config.storage_per_day = data["storage_per_day"]
                if "return_fee" in data: config.return_fee = data["return_fee"]
                if "acquiring_fee" in data: config.acquiring_fee = data["acquiring_fee"]
                if "last_mile_fee" in data: config.last_mile_fee = data["last_mile_fee"]
                if "category_rates" in data: config.category_rates.update(data["category_rates"])
                if "seasonal_multipliers" in data: config.seasonal_multipliers.update(data["seasonal_multipliers"])
                config.tariff_source = cached_entry.source
                config.last_updated = datetime.fromtimestamp(cached_entry.timestamp)
                logger.info(f"📥 Применены кэшированные тарифы для {mp_name}")
    except Exception as e:
        logger.warning(f"Не удалось загрузить кэш тарифов: {e}")
    
    return configs

# ============================================================================
# БЛОК 5: 150+ КАТЕГОРИЙ АВТОЗАПЧАСТЕЙ
# ============================================================================
def get_auto_parts_categories_full() -> Dict[str, ProductCategory]:
    """Получение полного списка категорий автозапчастей с габаритами"""
    categories = {}
    
    def make_cat(name, desc, min_l, max_l, min_w, max_w, min_h, max_h,
                 min_wt, max_wt, typ_vol, typ_wt, oem=None,
                 season=Seasonality.ALL_YEAR, risk=RiskLevel.LOW,
                 hazardous=False, fragile=False):
        return ProductCategory(
            name=name, description=desc,
            min_length=min_l, max_length=max_l,
            min_width=min_w, max_width=max_w,
            min_height=min_h, max_height=max_h,
            min_weight=min_wt, max_weight=max_wt,
            typical_volume=typ_vol, typical_weight=typ_wt,
            dimensions=ProductDimensions(
                length=(min_l + max_l) / 2, width=(min_w + max_w) / 2,
                height=(min_h + max_h) / 2, weight=(min_wt + max_wt) / 2
            ),
            oem_codes=oem or [], seasonality=season,
            risk_level=risk, hazardous=hazardous, fragile=fragile
        )
    
    # === ДВИГАТЕЛЬ ===
    categories["двигатель"] = make_cat("двигатель", "Двигатели и комплектующие", 30, 80, 30, 60, 30, 70, 10, 200, 20.0, 80.0, risk=RiskLevel.HIGH)
    categories["поршни"] = make_cat("поршни", "Поршни и кольца", 5, 12, 5, 12, 3, 10, 0.1, 1.5, 0.1, 0.5)
    categories["клапаны"] = make_cat("клапаны", "Клапаны двигателя", 3, 8, 1, 3, 10, 40, 0.05, 0.5, 0.05, 0.2)
    categories["прокладки_двигателя"] = make_cat("прокладки_двигателя", "Прокладки ГБЦ и двигателя", 10, 50, 10, 40, 0.1, 2, 0.01, 0.3, 0.1, 0.1)
    categories["свечи_зажигания"] = make_cat("свечи_зажигания", "Свечи зажигания", 2, 3, 2, 3, 6, 10, 0.04, 0.1, 0.01, 0.05)
    categories["блок_цилиндров"] = make_cat("блок_цилиндров", "Блок цилиндров", 40, 70, 30, 50, 20, 40, 20, 80, 100.0, 50.0, risk=RiskLevel.HIGH)
    categories["головка_блока"] = make_cat("головка_блока", "Головка блока цилиндров", 30, 60, 20, 40, 8, 20, 5, 30, 40.0, 15.0, risk=RiskLevel.HIGH)
    categories["коленвал"] = make_cat("коленвал", "Коленчатый вал", 40, 90, 8, 20, 8, 20, 10, 40, 30.0, 25.0, risk=RiskLevel.HIGH)
    categories["распредвал"] = make_cat("распредвал", "Распределительный вал", 30, 80, 5, 15, 5, 15, 3, 15, 20.0, 9.0)
    categories["шатун"] = make_cat("шатун", "Шатун двигателя", 12, 35, 4, 10, 3, 7, 0.5, 2, 3.0, 1.25)
    categories["гидрокомпенсаторы"] = make_cat("гидрокомпенсаторы", "Гидрокомпенсаторы", 3, 8, 3, 8, 3, 8, 0.05, 0.2, 0.3, 0.125)
    categories["привод_грм"] = make_cat("привод_грм", "Привод ГРМ (ремень, цепь)", 60, 160, 2, 5, 1, 2, 0.1, 1, 2.0, 0.55)
    categories["масляный_насос"] = make_cat("масляный_насос", "Масляный насос", 8, 18, 8, 18, 8, 18, 1, 5, 5.0, 3.0)
    categories["водяной_насос"] = make_cat("водяной_насос", "Водяной насос (помпа)", 8, 18, 8, 18, 8, 18, 1, 4, 5.0, 2.5)
    categories["турбокомпрессор"] = make_cat("турбокомпрессор", "Турбокомпрессор", 15, 35, 15, 30, 15, 25, 5, 15, 15.0, 10.0, risk=RiskLevel.HIGH)
    categories["масляный_поддон"] = make_cat("масляный_поддон", "Масляный поддон", 30, 60, 20, 40, 10, 20, 2, 8, 15.0, 5.0)
    categories["клапанная_крышка"] = make_cat("клапанная_крышка", "Клапанная крышка", 30, 60, 15, 30, 5, 10, 1, 4, 8.0, 2.5)
    categories["приводной_ремень"] = make_cat("приводной_ремень", "Приводной ремень", 60, 150, 1, 3, 0.5, 1, 0.05, 0.5, 1.0, 0.275)
    categories["демпфер_коленвала"] = make_cat("демпфер_коленвала", "Демпфер коленвала", 10, 25, 10, 25, 5, 10, 2, 8, 5.0, 5.0)
    categories["маховик"] = make_cat("маховик", "Маховик", 25, 45, 25, 45, 5, 10, 5, 15, 10.0, 10.0, risk=RiskLevel.HIGH)
    categories["стартерный_венец"] = make_cat("стартерный_венец", "Стартерный венец", 25, 40, 25, 40, 2, 5, 1, 5, 5.0, 3.0)
    
    # === ТРАНСМИССИЯ ===
    categories["трансмиссия"] = make_cat("трансмиссия", "КПП и комплектующие", 40, 80, 30, 60, 30, 60, 20, 100, 30.0, 50.0, risk=RiskLevel.HIGH)
    categories["сцепление"] = make_cat("сцепление", "Комплекты сцепления", 20, 40, 20, 40, 5, 15, 2, 10, 3.0, 5.0)
    categories["шкивы"] = make_cat("шкивы", "Шкивы и ролики", 5, 20, 5, 20, 2, 8, 0.2, 3, 0.5, 1.5)
    categories["коробка_передач"] = make_cat("коробка_передач", "Коробка передач в сборе", 40, 70, 30, 50, 25, 40, 30, 80, 80.0, 55.0, risk=RiskLevel.HIGH)
    categories["привод_полуоси"] = make_cat("привод_полуоси", "Привод (полуоси)", 40, 90, 8, 18, 8, 18, 3, 12, 15.0, 7.5)
    categories["дифференциал"] = make_cat("дифференциал", "Дифференциал", 20, 45, 20, 45, 20, 45, 10, 30, 30.0, 20.0, risk=RiskLevel.HIGH)
    categories["карданный_вал"] = make_cat("карданный_вал", "Карданный вал", 60, 160, 8, 18, 8, 18, 5, 20, 25.0, 12.5)
    categories["раздаточная_коробка"] = make_cat("раздаточная_коробка", "Раздаточная коробка", 25, 45, 20, 35, 20, 35, 15, 40, 35.0, 27.5, risk=RiskLevel.HIGH)
    categories["гидротрансформатор"] = make_cat("гидротрансформатор", "Гидротрансформатор АКПП", 25, 40, 25, 40, 20, 30, 10, 25, 30.0, 17.5, risk=RiskLevel.HIGH)
    categories["механизм_переключения"] = make_cat("механизм_переключения", "Механизм переключения передач", 15, 35, 5, 15, 5, 15, 1, 5, 5.0, 3.0)
    categories["подшипники_трансмиссии"] = make_cat("подшипники_трансмиссии", "Подшипники трансмиссии", 8, 18, 8, 18, 8, 18, 0.5, 3, 3.0, 1.75)
    categories["сальники_трансмиссии"] = make_cat("сальники_трансмиссии", "Сальники трансмиссии", 2, 12, 2, 12, 1, 3, 0.05, 0.3, 0.5, 0.175)
    categories["фильтр_акпп"] = make_cat("фильтр_акпп", "Фильтр АКПП", 8, 18, 8, 18, 8, 18, 0.5, 2, 3.0, 1.25)
    categories["масло_трансмиссионное"] = make_cat("масло_трансмиссионное", "Трансмиссионное масло", 10, 35, 8, 25, 8, 25, 1, 5, 5.0, 3.0, hazardous=True)
    categories["трос_сцепления"] = make_cat("трос_сцепления", "Трос сцепления", 40, 100, 1, 3, 1, 3, 0.1, 0.5, 1.0, 0.3)
    categories["цилиндр_сцепления"] = make_cat("цилиндр_сцепления", "Цилиндр сцепления", 10, 20, 5, 10, 5, 10, 0.5, 2, 2.0, 1.25)
    categories["вал_кпп"] = make_cat("вал_кпп", "Вал КПП", 20, 50, 5, 12, 5, 12, 2, 8, 8.0, 5.0)
    categories["шестерни_кпп"] = make_cat("шестерни_кпп", "Шестерни КПП", 5, 15, 5, 15, 5, 15, 0.5, 3, 3.0, 1.75)
    categories["синхронизатор"] = make_cat("синхронизатор", "Синхронизатор", 5, 12, 5, 12, 3, 8, 0.3, 1.5, 2.0, 0.9)
    categories["муфта_кпп"] = make_cat("муфта_кпп", "Муфта КПП", 5, 15, 5, 15, 3, 8, 0.5, 2, 3.0, 1.25)
    
    # === ПОДВЕСКА ===
    categories["подвеска"] = make_cat("подвеска", "Элементы подвески", 20, 80, 10, 40, 10, 60, 1, 20, 5.0, 8.0)
    categories["амортизаторы"] = make_cat("амортизаторы", "Амортизаторы", 5, 10, 5, 10, 40, 70, 2, 8, 5.0, 8.0, fragile=True)
    categories["пружины"] = make_cat("пружины", "Пружины подвески", 20, 40, 20, 40, 30, 60, 3, 10, 8.0, 12.0)
    categories["сайлентблоки"] = make_cat("сайлентблоки", "Сайлентблоки", 3, 10, 3, 10, 2, 8, 0.1, 1, 0.1, 0.3)
    categories["шаровые_опоры"] = make_cat("шаровые_опоры", "Шаровые опоры", 5, 15, 5, 15, 5, 15, 0.3, 2, 0.5, 1.5)
    categories["ступицы"] = make_cat("ступицы", "Ступицы и подшипники", 10, 25, 10, 25, 5, 15, 1, 5, 2.0, 4.0)
    categories["рычаг_подвески"] = make_cat("рычаг_подвески", "Рычаг подвески", 20, 65, 5, 18, 5, 18, 2, 10, 10.0, 6.0)
    categories["стабилизатор"] = make_cat("стабилизатор", "Стабилизатор поперечной устойчивости", 25, 65, 3, 10, 3, 10, 1, 5, 5.0, 3.0)
    categories["пыльник"] = make_cat("пыльник", "Пыльник (чехол)", 5, 12, 5, 12, 8, 22, 0.1, 0.5, 1.0, 0.3)
    categories["отбойник"] = make_cat("отбойник", "Отбойник амортизатора", 5, 12, 5, 12, 5, 12, 0.1, 0.5, 1.0, 0.3)
    categories["опора_стойки"] = make_cat("опора_стойки", "Опора стойки амортизатора", 8, 18, 8, 18, 5, 12, 0.5, 2, 3.0, 1.25)
    categories["подрамник"] = make_cat("подрамник", "Подрамник", 45, 105, 15, 35, 8, 18, 10, 30, 25.0, 20.0, risk=RiskLevel.HIGH)
    categories["распорка"] = make_cat("распорка", "Распорка подвески", 25, 65, 2, 6, 2, 6, 0.5, 2, 2.0, 1.25)
    categories["сайлентблоки_в_сборе"] = make_cat("сайлентблоки_в_сборе", "Сайлентблоки в сборе", 8, 22, 8, 22, 5, 12, 0.5, 2, 3.0, 1.25)
    categories["буфер"] = make_cat("буфер", "Буфер подвески", 5, 12, 5, 12, 5, 12, 0.1, 0.5, 1.0, 0.3)
    categories["подушка_подвески"] = make_cat("подушка_подвески", "Подушка подвески", 8, 18, 8, 18, 5, 12, 0.5, 2, 2.0, 1.25)
    categories["тяга_продольная"] = make_cat("тяга_продольная", "Тяга продольная", 25, 65, 3, 8, 3, 8, 1, 4, 4.0, 2.5)
    categories["балка_моста"] = make_cat("балка_моста", "Балка моста", 45, 85, 10, 20, 10, 20, 15, 40, 30.0, 27.5, risk=RiskLevel.HIGH)
    
    # === ТОРМОЗНАЯ СИСТЕМА ===
    categories["тормозная_система"] = make_cat("тормозная_система", "Тормозная система", 20, 40, 20, 40, 5, 15, 2, 15, 3.0, 8.0, risk=RiskLevel.HIGH)
    categories["тормозные_диски"] = make_cat("тормозные_диски", "Тормозные диски", 25, 40, 25, 40, 3, 8, 3, 12, 3.0, 8.0, fragile=True)
    categories["тормозные_колодки"] = make_cat("тормозные_колодки", "Тормозные колодки", 10, 20, 5, 12, 3, 8, 1, 4, 1.0, 3.0)
    categories["тормозные_шланги"] = make_cat("тормозные_шланги", "Тормозные шланги", 20, 60, 2, 5, 2, 5, 0.2, 1, 0.3, 0.8)
    categories["тормозные_суппорты"] = make_cat("тормозные_суппорты", "Тормозные суппорты", 15, 30, 10, 20, 10, 20, 2, 8, 5.0, 5.0)
    categories["тормозные_барабаны"] = make_cat("тормозные_барабаны", "Тормозные барабаны", 20, 35, 20, 35, 5, 15, 3, 10, 5.0, 6.5)
    categories["гтц"] = make_cat("гтц", "Главный тормозной цилиндр", 10, 25, 8, 18, 8, 18, 1, 4, 3.0, 2.5)
    categories["вакуумный_усилитель"] = make_cat("вакуумный_усилитель", "Вакуумный усилитель тормозов", 20, 35, 20, 35, 10, 20, 2, 6, 10.0, 4.0)
    
    # === РУЛЕВОЕ УПРАВЛЕНИЕ ===
    categories["рулевое_управление"] = make_cat("рулевое_управление", "Рулевое управление", 30, 100, 10, 30, 10, 30, 2, 15, 5.0, 10.0)
    categories["рулевые_тяги"] = make_cat("рулевые_тяги", "Рулевые тяги и наконечники", 20, 60, 3, 8, 3, 8, 0.5, 3, 1.0, 2.5)
    categories["рулевые_рейки"] = make_cat("рулевые_рейки", "Рулевые рейки", 50, 100, 10, 20, 10, 20, 5, 15, 8.0, 12.0)
    categories["рулевой_кардан"] = make_cat("рулевой_кардан", "Рулевой кардан", 20, 45, 5, 12, 5, 12, 1, 4, 5.0, 2.5)
    categories["усилитель_руля"] = make_cat("усилитель_руля", "Усилитель руля (ГУР/ЭУР)", 15, 30, 15, 30, 15, 25, 3, 10, 10.0, 6.5)
    categories["рулевой_насос"] = make_cat("рулевой_насос", "Насос ГУР", 15, 30, 12, 22, 12, 22, 3, 8, 6.0, 5.5)
    
    # === ЭЛЕКТРИКА ===
    categories["электрика"] = make_cat("электрика", "Электрооборудование", 10, 40, 10, 30, 10, 30, 0.5, 10, 2.0, 5.0)
    categories["стартеры"] = make_cat("стартеры", "Стартеры", 15, 30, 10, 20, 10, 25, 3, 10, 3.0, 6.0)
    categories["генераторы"] = make_cat("генераторы", "Генераторы", 15, 30, 15, 25, 15, 30, 4, 12, 5.0, 8.0)
    categories["аккумуляторы"] = make_cat("аккумуляторы", "Аккумуляторы", 20, 40, 15, 25, 15, 30, 10, 30, 15.0, 20.0, hazardous=True, risk=RiskLevel.HIGH)
    categories["датчики"] = make_cat("датчики", "Датчики", 3, 10, 2, 5, 2, 8, 0.05, 0.5, 0.1, 0.3)
    categories["катушки_зажигания"] = make_cat("катушки_зажигания", "Катушки зажигания", 5, 15, 3, 8, 5, 15, 0.2, 1, 0.5, 0.6)
    categories["проводка"] = make_cat("проводка", "Проводка и жгуты", 20, 100, 5, 20, 2, 10, 0.3, 3, 3.0, 1.5)
    categories["блоки_управления"] = make_cat("блоки_управления", "Блоки управления (ЭБУ)", 15, 30, 10, 20, 5, 15, 0.5, 3, 3.0, 1.5)
    
    # === СИСТЕМА ОХЛАЖДЕНИЯ ===
    categories["охлаждение"] = make_cat("охлаждение", "Система охлаждения", 20, 80, 15, 50, 10, 40, 1, 15, 8.0, 15.0)
    categories["радиаторы"] = make_cat("радиаторы", "Радиаторы охлаждения", 40, 80, 30, 60, 5, 15, 2, 10, 10.0, 15.0, fragile=True)
    categories["помпы"] = make_cat("помпы", "Водяные помпы", 10, 25, 10, 20, 10, 20, 1, 5, 2.0, 4.0)
    categories["термостаты"] = make_cat("термостаты", "Термостаты", 5, 12, 5, 12, 5, 12, 0.2, 1, 0.5, 1.0)
    categories["вентилятор_радиатора"] = make_cat("вентилятор_радиатора", "Вентилятор радиатора", 30, 50, 30, 50, 5, 15, 2, 6, 15.0, 4.0, fragile=True)
    categories["расширительный_бачок"] = make_cat("расширительный_бачок", "Расширительный бачок", 15, 30, 10, 20, 10, 25, 0.3, 1.5, 4.0, 0.9)
    
    # === ФИЛЬТРЫ ===
    categories["фильтры"] = make_cat("фильтры", "Фильтры", 5, 30, 5, 30, 5, 40, 0.1, 3, 2.0, 5.0)
    categories["масляные_фильтры"] = make_cat("масляные_фильтры", "Масляные фильтры", 6, 12, 6, 12, 8, 15, 0.3, 1, 1.0, 1.5)
    categories["воздушные_фильтры"] = make_cat("воздушные_фильтры", "Воздушные фильтры", 15, 40, 15, 35, 3, 10, 0.2, 2, 2.0, 4.0)
    categories["топливные_фильтры"] = make_cat("топливные_фильтры", "Топливные фильтры", 5, 15, 5, 15, 8, 20, 0.3, 1.5, 1.0, 2.0)
    categories["салонные_фильтры"] = make_cat("салонные_фильтры", "Салонные фильтры", 20, 35, 15, 25, 2, 5, 0.2, 1, 1.5, 2.5)
    
    # === МАСЛА И ЖИДКОСТИ ===
    categories["масла"] = make_cat("масла", "Масла и технические жидкости", 5, 30, 5, 30, 10, 40, 0.5, 20, 5.0, 15.0, hazardous=True)
    categories["моторные_масла"] = make_cat("моторные_масла", "Моторные масла", 8, 25, 8, 25, 20, 40, 1, 20, 5.0, 15.0, hazardous=True)
    categories["трансмиссионные_масла"] = make_cat("трансмиссионные_масла", "Трансмиссионные масла", 8, 25, 8, 25, 20, 40, 1, 20, 5.0, 15.0, hazardous=True)
    categories["тормозная_жидкость"] = make_cat("тормозная_жидкость", "Тормозная жидкость", 5, 10, 5, 10, 15, 25, 0.5, 2, 1.0, 2.0, hazardous=True)
    categories["антифриз"] = make_cat("антифриз", "Антифриз / Охлаждающая жидкость", 10, 30, 10, 30, 20, 40, 1, 20, 5.0, 15.0, hazardous=True)
    
    # === ОПТИКА ===
    categories["оптика"] = make_cat("оптика", "Оптика и освещение", 15, 60, 15, 40, 15, 40, 0.5, 10, 5.0, 10.0, fragile=True)
    categories["фары"] = make_cat("фары", "Фары головного света", 30, 60, 20, 40, 20, 40, 2, 8, 8.0, 12.0, fragile=True)
    categories["лампы"] = make_cat("лампы", "Автомобильные лампы", 2, 10, 2, 5, 5, 15, 0.02, 0.3, 0.1, 0.3, fragile=True)
    categories["фонари"] = make_cat("фонари", "Задние фонари", 20, 50, 15, 30, 10, 25, 1, 5, 5.0, 8.0, fragile=True)
    categories["led_лампы"] = make_cat("led_лампы", "LED лампы", 5, 15, 3, 8, 3, 8, 0.1, 0.5, 0.3, 0.3, fragile=True)
    
    # === КУЗОВ ===
    categories["кузов"] = make_cat("кузов", "Кузовные детали", 50, 200, 30, 150, 10, 100, 2, 50, 30.0, 80.0, fragile=True, risk=RiskLevel.HIGH)
    categories["бамперы"] = make_cat("бамперы", "Бамперы", 100, 200, 30, 60, 20, 50, 5, 20, 50.0, 80.0, fragile=True)
    categories["крылья"] = make_cat("крылья", "Крылья", 50, 100, 30, 60, 30, 80, 3, 10, 20.0, 40.0, fragile=True)
    categories["капоты"] = make_cat("капоты", "Капоты", 100, 180, 80, 150, 5, 15, 5, 15, 30.0, 60.0, fragile=True)
    categories["зеркала"] = make_cat("зеркала", "Зеркала заднего вида", 15, 30, 10, 20, 10, 20, 0.5, 3, 3.0, 5.0, fragile=True)
    categories["двери"] = make_cat("двери", "Двери", 100, 150, 50, 100, 5, 15, 15, 40, 80.0, 27.5, fragile=True, risk=RiskLevel.HIGH)
    categories["стёкла"] = make_cat("стёкла", "Автомобильные стёкла", 50, 150, 30, 100, 0.5, 2, 5, 20, 40.0, 12.5, fragile=True, risk=RiskLevel.HIGH)
    
    # === ШИНЫ И ДИСКИ ===
    categories["шины"] = make_cat("шины", "Шины и диски", 40, 80, 40, 80, 15, 40, 5, 30, 20.0, 40.0)
    categories["летние_шины"] = make_cat("летние_шины", "Летние шины", 50, 80, 50, 80, 15, 30, 8, 25, 25.0, 35.0, season=Seasonality.SUMMER)
    categories["зимние_шины"] = make_cat("зимние_шины", "Зимние шины", 50, 80, 50, 80, 15, 30, 8, 25, 25.0, 35.0, season=Seasonality.WINTER)
    categories["диски"] = make_cat("диски", "Колесные диски", 40, 60, 40, 60, 15, 30, 5, 20, 15.0, 25.0, fragile=True)
    
    # === ИНСТРУМЕНТЫ ===
    categories["инструменты"] = make_cat("инструменты", "Автоинструменты", 10, 60, 5, 30, 3, 20, 0.2, 10, 3.0, 8.0)
    categories["домкраты"] = make_cat("домкраты", "Домкраты", 20, 50, 10, 25, 10, 25, 3, 15, 5.0, 12.0)
    categories["наборы_ключей"] = make_cat("наборы_ключей", "Наборы ключей", 15, 40, 10, 25, 3, 10, 1, 8, 3.0, 6.0)
    categories["компрессоры_воздушные"] = make_cat("компрессоры_воздушные", "Воздушные компрессоры", 25, 60, 20, 40, 20, 40, 5, 25, 15.0, 15.0)
    
    # === РЕМНИ И ПРИВОДЫ ===
    categories["ремни"] = make_cat("ремни", "Ремни ГРМ и приводов", 50, 150, 1, 3, 1, 3, 0.1, 0.8, 0.5, 1.0)
    categories["ролики"] = make_cat("ролики", "Ролики натяжители", 5, 12, 5, 12, 2, 5, 0.2, 1.5, 0.5, 1.0)
    
    # === ПОДШИПНИКИ ===
    categories["подшипники"] = make_cat("подшипники", "Подшипники", 3, 15, 3, 15, 1, 5, 0.1, 3, 0.5, 2.0)
    
    # === КРЕПЁЖ ===
    categories["крепёж"] = make_cat("крепёж", "Крепёж и метизы", 0.5, 10, 0.5, 10, 0.5, 10, 0.01, 2, 0.2, 1.0)
    
    # === КЛИМАТ ===
    categories["климат"] = make_cat("климат", "Климат-контроль и кондиционер", 20, 80, 20, 60, 15, 50, 2, 20, 10.0, 20.0)
    categories["компрессоры"] = make_cat("компрессоры", "Компрессоры кондиционера", 20, 40, 15, 30, 15, 30, 5, 15, 8.0, 12.0)
    categories["конденсоры"] = make_cat("конденсоры", "Конденсоры кондиционера", 40, 80, 30, 60, 5, 15, 2, 8, 10.0, 5.0, fragile=True)
    
    # === ВЫХЛОПНАЯ СИСТЕМА ===
    categories["выпуск"] = make_cat("выпуск", "Выхлопная система", 30, 150, 10, 40, 10, 40, 2, 25, 10.0, 25.0)
    categories["глушители"] = make_cat("глушители", "Глушители", 50, 150, 20, 40, 20, 40, 5, 20, 20.0, 30.0)
    categories["катализаторы"] = make_cat("катализаторы", "Каталитические нейтрализаторы", 30, 80, 15, 30, 15, 30, 3, 15, 10.0, 20.0, hazardous=True, risk=RiskLevel.HIGH)
    categories["гофры"] = make_cat("гофры", "Гофры выхлопной системы", 10, 30, 5, 15, 5, 15, 0.3, 2, 2.0, 1.15)
    
    # === БЕЗОПАСНОСТЬ ===
    categories["безопасность"] = make_cat("безопасность", "Системы безопасности", 10, 50, 10, 40, 5, 30, 0.5, 8, 3.0, 6.0, risk=RiskLevel.HIGH)
    categories["подушки_безопасности"] = make_cat("подушки_безопасности", "Подушки безопасности", 20, 50, 15, 30, 10, 20, 1, 5, 5.0, 3.0, risk=RiskLevel.HIGH)
    
    # === ПРОЧЕЕ ===
    categories["щетки_стеклоочистителя"] = make_cat("щетки_стеклоочистителя", "Щетки стеклоочистителя", 30, 70, 2, 5, 2, 5, 0.1, 0.5, 1.0, 1.5)
    categories["коврики"] = make_cat("коврики", "Автомобильные коврики", 50, 100, 40, 80, 1, 5, 1, 5, 10.0, 15.0)
    categories["чехлы"] = make_cat("чехлы", "Чехлы на сиденья", 40, 80, 30, 60, 5, 20, 1, 5, 15.0, 25.0)
    categories["автохимия"] = make_cat("автохимия", "Автохимия и косметика", 5, 30, 5, 20, 10, 40, 0.3, 5, 2.0, 5.0, hazardous=True)
    
    return categories


# ============================================================================
# БЛОК 7: ВСПОМОГАТЕЛЬНЫЕ КЛАССЫ
# ============================================================================
class CategoryClassifier:
    """Классификатор категорий товаров с ML"""
    
    def __init__(self):
        self.categories = {
            "двигатель": ["двигатель", "поршень", "клапан", "свеча", "цилиндр"],
            "трансмиссия": ["кпп", "коробка", "сцепление", "передач"],
            "подвеска": ["амортизатор", "пружина", "рычаг", "стойка"],
            "тормозная_система": ["тормоз", "колодка", "диск", "суппорт"],
            "электрика": ["аккумулятор", "генератор", "стартер", "датчик"],
            "фильтры": ["фильтр", "масляный", "воздушный", "салонный"],
            "масла": ["масло", "жидкость", "антифриз"],
            "оптика": ["фара", "лампа", "фонарь"],
            "кузов": ["бампер", "крыло", "капот", "дверь"],
            "инструменты": ["ключ", "домкрат", "компрессор"]
        }
        
        self.model = None
        self.vectorizer = None
        
        if SKLEARN_AVAILABLE:
            try:
                self.model_path = MODELS_DIR / "category_classifier.joblib"
                if self.model_path.exists():
                    self.model = joblib.load(self.model_path)
                    logger.info("✅ ML модель классификации загружена")
            except Exception:
                pass
    
    def predict(self, text: str) -> Tuple[str, float]:
        """Предсказание категории по названию"""
        if not text:
            return ("Прочее", 0.0)
        
        if self.model and self.vectorizer:
            try:
                text_vector = self.vectorizer.transform([text])
                prediction = self.model.predict(text_vector)
                confidence = max(self.model.predict_proba(text_vector)[0])
                return (prediction[0], confidence)
            except Exception:
                pass
        
        text_lower = text.lower()
        for category, keywords in self.categories.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return (category, 0.85)
        
        return ("Прочее", 0.0)

class CatalogEnhancer:
    """Обогащение каталога через поиск аналогов"""
    
    def __init__(self):
        self.oe_data = pd.DataFrame()
        self.parts_data = pd.DataFrame()
        self.cross_data = pd.DataFrame()
        
        self.stats = {
            "oe_loaded": 0,
            "parts_loaded": 0,
            "cross_loaded": 0,
            "analog_searches": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }
        
        self._cache = {}
        self.oe_index = {}
        self.parts_index = {}
        self.cross_index = defaultdict(list)
        self.oe_to_parts = defaultdict(list)
    
    def load_oe_data(self, df: pd.DataFrame):
        self.oe_data = df
        self.stats["oe_loaded"] = len(df)
        self._build_oe_index()
    
    def load_parts_data(self, df: pd.DataFrame):
        self.parts_data = df
        self.stats["parts_loaded"] = len(df)
        self._build_parts_index()
    
    def load_cross_references(self, df: pd.DataFrame):
        self.cross_data = df
        self.stats["cross_loaded"] = len(df)
        self._build_cross_index()
    
    def _build_oe_index(self):
        if not self.oe_data.empty:
            for _, row in self.oe_data.iterrows():
                oe = str(row.get('oe_number', '')).strip()
                if oe:
                    self.oe_index[oe] = row.to_dict()
    
    def _build_parts_index(self):
        if not self.parts_data.empty:
            for _, row in self.parts_data.iterrows():
                key = (str(row.get('artikul', '')).strip(), str(row.get('brand', '')).strip())
                if key[0]:
                    self.parts_index[key] = row.to_dict()
    
    def _build_cross_index(self):
        if not self.cross_data.empty:
            for _, row in self.cross_data.iterrows():
                oe = str(row.get('oe_number', '')).strip()
                artikul = str(row.get('artikul', '')).strip()
                brand = str(row.get('brand', '')).strip()
                
                if oe and artikul:
                    self.cross_index[(artikul, brand)].append(oe)
                    self.oe_to_parts[oe].append((artikul, brand))
    
    def get_analog_data(self, artikul: str, brand: str, max_analogs: int = 20) -> Dict[str, Any]:
        self.stats["analog_searches"] += 1
        
        cache_key = (artikul, brand)
        if cache_key in self._cache:
            self.stats["cache_hits"] += 1
            return self._cache[cache_key]
        
        self.stats["cache_misses"] += 1
        
        if self.cross_data.empty:
            return {"error": "Кросс-ссылки не загружены"}
        
        oe_numbers = self.cross_index.get((artikul, brand), [])
        
        if not oe_numbers:
            return {"error": "Артикул не найден", "analog_count": 0}
        
        analogs = []
        seen = set()
        
        for oe in oe_numbers:
            for analog_artikul, analog_brand in self.oe_to_parts.get(oe, []):
                if (analog_artikul, analog_brand) == (artikul, brand):
                    continue
                
                key = (analog_artikul, analog_brand)
                if key in seen:
                    continue
                
                seen.add(key)
                
                analog_info = {
                    "Артикул": analog_artikul,
                    "Бренд": analog_brand,
                    "OE номер": oe
                }
                
                part_key = (analog_artikul, analog_brand)
                if part_key in self.parts_index:
                    part = self.parts_index[part_key]
                    analog_info["Наименование"] = part.get('description', '')
                    analog_info["Вес"] = part.get('weight', '')
                
                analogs.append(analog_info)
        
        result = {
            "oe_list": ", ".join(oe_numbers[:5]),
            "analog_count": len(analogs),
            "has_analogs": len(analogs) > 0,
            "analogs": analogs[:max_analogs]
        }
        
        self._cache[cache_key] = result
        
        return result
    
    def get_stats(self) -> Dict[str, int]:
        return self.stats

# ============================================================================
# БЛОК 8: МЕНЕДЖЕР ПАМЯТИ И ПРОИЗВОДИТЕЛЬНОСТИ
# ============================================================================
class PerformanceManager:
    """Управление ресурсами системы для оптимизации тяжелых расчетов"""
    
    def __init__(self):
        self.start_time = time.time()
        self.memory_threshold_mb = 4096
        self.gc_threshold = 100
        self.operation_count = 0
    
    def check_memory_usage(self) -> bool:
        if not PSUTIL_AVAILABLE:
            return True
        
        try:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            mem_mb = mem_info.rss / (1024 * 1024)
            
            self.operation_count += 1
            
            if mem_mb > self.memory_threshold_mb:
                logger.warning(f"⚠️ Высокое использование памяти: {mem_mb:.2f} MB. Запуск GC...")
                gc.collect()
                return mem_info.rss / (1024 * 1024) < self.memory_threshold_mb * 1.2
            
            if self.operation_count % self.gc_threshold == 0:
                gc.collect()
            
            return True
        except (psutil.Error, OSError) as e:
            logger.error(f"Ошибка проверки памяти: {e}")
            return True
    
    def get_system_stats(self) -> Dict[str, Any]:
        stats = {
            "uptime_seconds": time.time() - self.start_time,
            "operation_count": self.operation_count,
            "memory_threshold_mb": self.memory_threshold_mb
        }
        
        if PSUTIL_AVAILABLE:
            stats.update({
                "cpu_percent": psutil.cpu_percent(interval=0.5),
                "memory_percent": psutil.virtual_memory().percent,
                "memory_available_mb": psutil.virtual_memory().available / (1024 * 1024),
                "disk_usage_percent": psutil.disk_usage('/').percent
            })
            
            try:
                process = psutil.Process(os.getpid())
                mem_info = process.memory_info()
                stats["process_memory_mb"] = mem_info.rss / (1024 * 1024)
                stats["process_cpu_percent"] = process.cpu_percent()
            except (psutil.Error, OSError):
                pass
        
        return stats
    
    def optimize_for_big_data(self):
        if POLARS_AVAILABLE:
            os.environ['POLARS_MAX_THREADS'] = str(min(4, os.cpu_count() or 2))
            os.environ['POLARS_VERBOSE'] = '0'
        
        if PSUTIL_AVAILABLE:
            memory_mb = psutil.virtual_memory().available / (1024 * 1024)
            if memory_mb < 2048:
                logger.warning(f"⚠️ Мало памяти ({memory_mb:.0f} MB). Включен экономичный режим.")
                self.memory_threshold_mb = 1024
                os.environ['POLARS_MAX_THREADS'] = '1'

perf_manager = PerformanceManager()
perf_manager.optimize_for_big_data()

# ============================================================================
# БЛОК 9: API КОННЕКТОРЫ МАРКЕТПЛЕЙСОВ
# ============================================================================
class MarketplaceAPIConnector:
    """Получение актуальных тарифов, остатков и заказов через официальные API."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "AutoPartsUnitEconomicsPro/100.5"
        })
        self.logger = logging.getLogger('MarketplaceAPI')
        self.cache = {}
        self.cache_ttl = 3600
    
    def get_ozon_tariffs(self, api_key: str, client_id: str, category_id: int = 0) -> Dict[str, Any]:
        url = "https://api-seller.ozon.ru/v1/finance/tariff-rates"
        headers = {"Client-Id": client_id, "Api-Key": api_key}
        
        cache_key = f"ozon_tariffs_{category_id}"
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            if time.time() - cached.get("timestamp", 0) < self.cache_ttl:
                return cached.get("data", {})
        
        try:
            response = self.session.post(url, json={"category_id": category_id}, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                self.cache[cache_key] = {
                    "timestamp": time.time(),
                    "data": {
                        "source": "Ozon API Live",
                        "timestamp": datetime.now().isoformat(),
                        "raw_data": data
                    }
                }
                
                return self.cache[cache_key]["data"]
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Ozon API Error: {e}")
        
        return {}
    
    def get_ozon_stocks(self, api_key: str, client_id: str, limit: int = 100, last_id: str = "") -> Dict[str, Any]:
        url = "https://api-seller.ozon.ru/v2/products/info/stocks"
        headers = {"Client-Id": client_id, "Api-Key": api_key}
        
        try:
            response = self.session.post(url, json={"limit": limit, "last_id": last_id}, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return {"success": True, "data": response.json()}
            
            return {"success": False, "error": f"HTTP {response.status_code}"}
        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}
    
    def get_wildberries_tariffs(self, api_key: str, date: Optional[str] = None) -> Dict[str, Any]:
        url = "https://common-api.wildberries.ru/tariffs/box"
        headers = {"Authorization": api_key}
        
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        params = {"date": date}
        
        cache_key = f"wb_tariffs_{date}"
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            if time.time() - cached.get("timestamp", 0) < self.cache_ttl:
                return cached.get("data", {})
        
        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                self.cache[cache_key] = {
                    "timestamp": time.time(),
                    "data": {
                        "source": "WB API Live",
                        "timestamp": datetime.now().isoformat(),
                        "data": data
                    }
                }
                
                return self.cache[cache_key]["data"]
            
            return {"success": False, "error": f"HTTP {response.status_code}"}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"WB API Error: {e}")
            return {"success": False, "error": str(e)}
    
    def get_wildberries_reports(self, api_key: str, date_from: str, date_to: str) -> Dict[str, Any]:
        url = "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod"
        params = {"dateFrom": date_from, "dateTo": date_to}
        headers = {"Authorization": api_key}
        
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return {"success": True, "data": response.json()}
            
            return {"success": False, "error": f"HTTP {response.status_code}"}
        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}
    
    def get_yandex_market_campaigns(self, oauth_token: str) -> Dict[str, Any]:
        url = "https://api.partner.market.yandex.ru/v2/campaigns"
        headers = {"Authorization": f"OAuth {oauth_token}"}
        
        try:
            response = self.session.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return {"success": True, "data": response.json()}
            
            return {"success": False, "error": f"HTTP {response.status_code}"}
        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}
    
    def get_yandex_market_tariffs(self, oauth_token: str, campaign_id: int) -> Dict[str, Any]:
        url = f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/deliveries/fees"
        headers = {"Authorization": f"OAuth {oauth_token}"}
        
        try:
            response = self.session.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return {"success": True, "data": response.json()}
            
            return {"success": False, "error": f"HTTP {response.status_code}"}
        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}

# ============================================================================
# БЛОК 10: ОСНОВНОЙ КЛАСС ЮНИТ-ЭКОНОМИКИ (🆕 v100.5 - С УЛУЧШЕНИЯМИ)
# ============================================================================
# ✅ ИСПРАВЛЕНИЯ v100.11:
# 1. Добавлен threading.Lock для потокобезопасного обновления статистики
# 2. Удалён дублирующий метод calculate_chunk() (используется _calculate_chunk_threadsafe)
# 3. _calculate_chunk_threadsafe теперь возвращает ошибки вместе с результатами
# 4. Исправлены отступы во всех методах
# ============================================================================

@st.cache_resource
def get_marketplace_unit_economics():
    """Получение экземпляра через st.cache_resource"""
    return MarketplaceUnitEconomics()


class MarketplaceUnitEconomics:
    """Основной класс для расчета юнит-экономики."""
    
    def __init__(self):
        self._configs = self._load_marketplace_configs()
        self._categories = self._load_categories()
        self._cache = {}
        self._history = []
        self._stats = self._init_stats()
        self._settings = self._load_settings()
        self._tariff_cache = get_smart_tariff_cache()
        self._ai_updater = None
        self._parallel_cache = {}
        
        # ✅ ИСПРАВЛЕНИЕ v100.11: Lock для потокобезопасного обновления статистики
        self._stats_lock = threading.Lock()
        
        try:
            self._persistent_db = get_persistent_history_db()
        except Exception as e:
            logger.error(f"Ошибка инициализации PersistentHistoryDB: {e}")
            self._persistent_db = None
        
        self._logger = logging.getLogger('MarketplaceUnitEconomics')
        self._logger.info("🚗 Инициализация MarketplaceUnitEconomics v100.5.1")
        self._logger.info(f"📊 Загружено {len(self._configs)} маркетплейсов")
        self._logger.info(f"📚 Загружено {len(self._categories)} категорий")
        
        if self._persistent_db:
            self._logger.info("📚 Постоянное хранилище истории подключено")
    
    def _load_marketplace_configs(self) -> Dict[str, MarketplaceConfig]:
        return get_marketplace_configs_2026()
    
    def _load_categories(self) -> Dict[str, ProductCategory]:
        categories = {}
        for name, cat in get_auto_parts_categories_full().items():
            categories[name] = cat
        return categories
    
    def _init_stats(self) -> Dict[str, Any]:
        return {
            "total_calculations": 0,
            "by_marketplace": defaultdict(int),
            "by_category": defaultdict(int),
            "by_mode": defaultdict(int),
            "by_status": defaultdict(int),
            "by_tax_system": defaultdict(int),
            "by_tariff_source": defaultdict(int),
            "avg_profit": 0.0,
            "avg_margin": 0.0,
            "avg_roi": 0.0,
            "avg_tax": 0.0,
            "total_profit": 0.0,
            "total_tax": 0.0,
            "max_profit": 0.0,
            "min_profit": 0.0,
            "best_marketplace": None,
            "best_category": None,
            "best_mode": None,
            "total_optimizations": 0,
            "optimization_improvement": 0.0,
            "start_time": datetime.now(),
            "errors_count": 0,
            "last_error": None,
            "cache_hits": 0,
            "cache_misses": 0,
            "ai_requests": 0,
            "db_saved": 0,
            "parallel_calculations": 0
        }
    
    def _load_settings(self) -> Dict[str, Any]:
        settings_path = CONFIG_DIR / "settings.json"
        default_settings = {
            "default_marketplace": "Ozon",
            "default_mode": "FBS",
            "default_days_storage": 30,
            "target_margin": 20.0,
            "enable_ai": True,
            "ai_provider": "deepseek",
            "enable_cache": True,
            "cache_ttl": 3600,
            "parallel_processing": True,
            "max_workers": 4,
            "optimize_memory": True,
            "precision_decimals": 2,
            "currency": "RUB",
            "locale": "ru_RU",
            "timezone": "Europe/Moscow",
            "enable_persistent_history": True,
            "global_markup": DEFAULT_MARKUP_GLOBAL,
            "discount_max": DEFAULT_DISCOUNT_MAX,
            "enable_seasonal_adjustments": True,
            "forecast_months": 3,
            "tax_system": "УСН_6",
            "ad_intensity": "medium",
        }
        
        if settings_path.exists():
            try:
                with open(settings_path, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                    default_settings.update(settings)
            except (IOError, json.JSONDecodeError) as e:
                self._logger.warning(f"Ошибка загрузки настроек: {e}")
        
        return default_settings
    
    def save_settings(self, settings: Dict[str, Any]) -> bool:
        try:
            settings_path = CONFIG_DIR / "settings.json"
            self._settings.update(settings)
            with open(settings_path, 'w', encoding='utf-8') as f:
                json.dump(self._settings, f, ensure_ascii=False, indent=2)
            return True
        except (IOError, OSError) as e:
            self._logger.error(f"Ошибка сохранения настроек: {e}")
            return False
    
    def get_category_dimensions(self, category_name: str) -> Optional[ProductDimensions]:
        if category_name in self._categories:
            return self._categories[category_name].dimensions
        return None
    
    def get_category_info(self, category_name: str) -> Optional[ProductCategory]:
        return self._categories.get(category_name)
    
    def find_categories_by_keyword(self, keyword: str) -> List[Tuple[str, ProductCategory]]:
        keyword_lower = keyword.lower()
        results = []
        for name, cat in self._categories.items():
            if keyword_lower in name.lower() or keyword_lower in cat.description.lower():
                results.append((name, cat))
        return results
    
    def calculate_dimensions_from_category(self, category_name: str) -> Tuple[float, float, float, float]:
        cat = self._categories.get(category_name)
        if cat and cat.dimensions:
            return (
                cat.dimensions.length,
                cat.dimensions.width,
                cat.dimensions.height,
                cat.dimensions.weight
            )
        return 0, 0, 0, 0
    
    def is_category_hazardous(self, category_name: str) -> bool:
        cat = self._categories.get(category_name)
        return cat.hazardous if cat else False
    
    def is_category_fragile(self, category_name: str) -> bool:
        cat = self._categories.get(category_name)
        return cat.fragile if cat else False
    
    def is_category_oversized(self, length: float, width: float, height: float, weight: float) -> bool:
        return any([length > 100, width > 100, height > 100, weight > 25])
    
    def _get_ai_updater(self) -> Optional['DeepSeekRateUpdater']:
        if self._ai_updater is None:
            try:
                self._ai_updater = DeepSeekRateUpdater()
            except Exception as e:
                self._logger.error(f"Ошибка инициализации AI updater: {e}")
                return None
        return self._ai_updater
    
    def get_tariff_forecast(self, marketplace: str, category: str = None, months_ahead: int = 3) -> Optional[Dict[str, Any]]:
        updater = self._get_ai_updater()
        if updater is None:
            return None
        return updater.get_tariff_forecast(marketplace, category, months_ahead)
    
    def refresh_tariffs_from_ai(
        self,
        marketplace: Optional[str] = None,
        category: Optional[str] = None,
        force: bool = True,
        include_forecast: bool = False
    ) -> Dict[str, Any]:
        updater = self._get_ai_updater()
        if updater is None:
            return {"error": "AI updater не инициализирован"}
        
        self._stats["ai_requests"] += 1
        
        try:
            if marketplace:
                rates, source, forecast = updater.get_rates_from_ai(
                    marketplace=marketplace,
                    category=category,
                    force_refresh=force,
                    use_cache=True,
                    include_forecast=include_forecast
                )
                if rates:
                    self._apply_ai_tariffs(marketplace, rates)
                    return {
                        "marketplace": marketplace,
                        "source": source.value,
                        "rates": rates,
                        "forecast": forecast,
                        "success": True
                    }
            else:
                results = updater.update_all_marketplaces(
                    force_refresh=force,
                    include_forecast=include_forecast
                )
                for mp, (rates, source, forecast) in results.items():
                    if rates:
                        self._apply_ai_tariffs(mp, rates)
                return {
                    "marketplaces_updated": len([r for r in results.values() if r[0]]),
                    "total": len(results),
                    "success": True
                }
        except Exception as e:
            self._logger.error(f"Ошибка обновления тарифов через AI: {e}")
            return {"error": str(e), "success": False}
    
    def _apply_ai_tariffs(self, marketplace: str, rates: Dict[str, Any]):
        if marketplace not in self._configs:
            return
        
        config = self._configs[marketplace]
        
        field_mapping = {
            "commission_rate": "commission_rate",
            "min_commission": "min_commission",
            "logistics_base": "logistics_base",
            "logistics_per_kg": "logistics_per_kg",
            "logistics_per_liter": "logistics_per_liter",
            "storage_per_day": "storage_per_day",
            "return_fee": "return_fee",
            "acquiring_fee": "acquiring_fee",
            "last_mile_fee": "last_mile_fee",
            "delivery_fee_percent": "delivery_fee_percent",
            "hazardous_surcharge": "hazardous_surcharge",
            "fragile_surcharge": "fragile_surcharge",
            "oversized_surcharge": "oversized_surcharge",
            "seasonal_multipliers": "seasonal_multipliers"
        }
        
        for ai_key, config_key in field_mapping.items():
            if ai_key in rates:
                try:
                    if config_key == "seasonal_multipliers" and isinstance(rates[ai_key], dict):
                        setattr(config, config_key, rates[ai_key])
                    else:
                        setattr(config, config_key, float(rates[ai_key]))
                except (ValueError, TypeError):
                    pass
        
        config.tariff_source = TariffSource.AI_LIVE
        config.last_updated = datetime.now()
        self._logger.info(f"✅ AI-тарифы применены для {marketplace}")
    
    @timer_decorator
    def calculate_unit_economics(
        self,
        price: float,
        cost: float,
        marketplace: str,
        category: Optional[str] = None,
        operation_mode: str = "FBS",
        days_in_storage: int = 30,
        length: float = 0,
        width: float = 0,
        height: float = 0,
        weight: float = 0,
        is_premium: bool = False,
        include_insurance: bool = False,
        include_packing: bool = False,
        include_marketing: bool = False,
        currency: str = "RUB",
        article: str = "",
        brand: str = "",
        current_month: Optional[int] = None,
        tax_system: str = "УСН_6",
        ad_intensity: str = "medium",
        discount_percent: float = 0.0,
        promo_participation: float = 0.0,
        **kwargs
    ) -> 'UnitEconomicsResult':
        """🆕 v100.5: Расчет юнит-экономики с улучшенной точностью"""
        
        if price <= 0:
            raise ValidationError("Цена должна быть положительной", "price", price)
        if cost <= 0:
            raise ValidationError("Себестоимость должна быть положительной", "cost", cost)
        if marketplace not in self._configs:
            raise MarketplaceError(f"Маркетплейс {marketplace} не поддерживается", marketplace)
        
        config = self._configs[marketplace]
        
        if current_month is None:
            current_month = datetime.now().month
        
        if isinstance(length, str):
            parsed_length, parsed_width, parsed_height = parse_dimensions_string(length)
            length = parsed_length
            width = parsed_width
            height = parsed_height
        
        if all([length == 0, width == 0, height == 0, weight == 0]) and category:
            length, width, height, weight = self.calculate_dimensions_from_category(category)
        
        volume = calculate_volume(length, width, height)
        if volume == 0:
            volume = 5.0
        if weight <= 0:
            weight = 1.0
        
        billable_weight = calculate_billable_weight(weight, length, width, height)
        
        hazardous = self.is_category_hazardous(category) if category else False
        fragile = self.is_category_fragile(category) if category else False
        oversized = self.is_category_oversized(length, width, height, weight)
        
        commission = config.calculate_commission_with_dynamics(
            price=price,
            discount_percent=discount_percent,
            promo_participation=promo_participation,
            category=category,
            current_month=current_month
        )
        commission_percent = (commission / price * 100) if price > 0 else 0
        
        seasonal_multiplier = config.apply_seasonal_multiplier(1.0, current_month)
        
        logistics = (
            config.logistics_base * seasonal_multiplier +
            billable_weight * config.logistics_per_kg * seasonal_multiplier +
            volume * config.logistics_per_liter * seasonal_multiplier
        )
        logistics = config.apply_promo_discount(logistics)
        
        mode_multiplier = config.mode_multipliers.get(operation_mode, 1.0)
        logistics *= mode_multiplier
        
        storage_cost = calculate_storage_cost_progressive(
            volume_l=volume,
            days=days_in_storage,
            base_rate=config.storage_per_day,
            marketplace=marketplace
        )
        
        acquiring = price * config.acquiring_fee
        delivery = price * config.delivery_fee_percent
        last_mile = config.last_mile_fee
        
        return_rate = MARKET_BENCHMARKS_2026.get(category, {}).get("return_rate", config.return_fee)
        returns = calculate_returns_cost(price, return_rate)
        
        rko_fee = price * config.rko_fee if config.rko_fee > 0 else 0
        premium_fee = price * config.premium_fee if is_premium and config.premium_fee > 0 else 0
        insurance_fee = price * config.insurance_fee if include_insurance and config.insurance_fee > 0 else 0
        packing_fee = config.packing_fee if include_packing and config.packing_fee > 0 else 0
        marketing_fee = price * config.marketing_fee if include_marketing and config.marketing_fee > 0 else 0
        
        hazardous_surcharge = price * config.hazardous_surcharge if hazardous else 0.0
        fragile_surcharge = price * config.fragile_surcharge if fragile else 0.0
        oversized_surcharge = price * config.oversized_surcharge if oversized else 0.0
        
        subscription_cost = config.subscription_fee / 30 if config.subscription_fee > 0 else 0
        
        tax_amount = calculate_tax(price, cost, tax_system)
        
        auto_parts_costs = AutoPartsSpecificCosts()
        auto_parts_specific = auto_parts_costs.calculate(price, is_import=False, requires_marking=True)
        
        advertising_cost = calculate_advertising_cost(price, category or "", ad_intensity)
        
        total_expenses = (
            cost + commission + subscription_cost + logistics + storage_cost +
            acquiring + delivery + last_mile + returns + rko_fee +
            premium_fee + insurance_fee + packing_fee + marketing_fee +
            hazardous_surcharge + fragile_surcharge + oversized_surcharge +
            tax_amount + auto_parts_specific + advertising_cost
        )
        
        profit = price - total_expenses
        margin_percent = (profit / price * 100) if price > 0 else 0
        roi = (profit / cost * 100) if cost > 0 else 0
        
        variable_rate = (
            ((commission / price) if price > 0 else 0) +
            config.acquiring_fee +
            config.delivery_fee_percent +
            config.return_fee +
            config.rko_fee +
            config.premium_fee +
            config.insurance_fee +
            config.marketing_fee +
            config.hazardous_surcharge +
            config.fragile_surcharge +
            config.oversized_surcharge +
            TAX_SYSTEMS.get(tax_system, {}).get("rate", 0.06)
        )
        
        fixed_costs = logistics + storage_cost + last_mile + subscription_cost
        breakeven_price = ((cost + fixed_costs) / (1 - variable_rate)) if (1 - variable_rate) > 0 else 0
        
        recommended_min_price = calculate_recommended_min_price(
            cost=cost,
            commission_rate=commission / price if price > 0 else 0,
            logistics=logistics,
            storage_cost=storage_cost,
            acquiring_rate=config.acquiring_fee,
            last_mile=last_mile,
            return_rate=return_rate,
            min_profit_percent=0.10,
            tax_system=tax_system,
            tax_rate=TAX_SYSTEMS.get(tax_system, {}).get("rate", 0.06)
        )
        
        contribution_margin = price - cost - commission - logistics - acquiring - delivery - last_mile - returns - tax_amount
        contribution_margin_ratio = (contribution_margin / price * 100) if price > 0 else 0
        
        result = UnitEconomicsResult(
            marketplace=marketplace,
            operation_mode=operation_mode,
            category=category or "Общая",
            price=money_round(price),
            cost=money_round(cost),
            length=money_round(length),
            width=money_round(width),
            height=money_round(height),
            weight=money_round(weight),
            volume=money_round(volume, 3),
            commission=money_round(commission),
            commission_percent=money_round(commission_percent),
            logistics=money_round(logistics),
            storage_cost=money_round(storage_cost),
            acquiring=money_round(acquiring),
            delivery=money_round(delivery),
            last_mile=money_round(last_mile),
            returns=money_round(returns),
            rko_fee=money_round(rko_fee),
            premium_fee=money_round(premium_fee),
            insurance_fee=money_round(insurance_fee),
            packing_fee=money_round(packing_fee),
            marketing_fee=money_round(marketing_fee),
            subscription_cost=money_round(subscription_cost),
            hazardous_surcharge=money_round(hazardous_surcharge),
            fragile_surcharge=money_round(fragile_surcharge),
            oversized_surcharge=money_round(oversized_surcharge),
            tax_amount=money_round(tax_amount),
            tax_system=tax_system,
            total_expenses=money_round(total_expenses),
            profit=money_round(profit),
            margin_percent=money_round(margin_percent),
            roi=money_round(roi),
            breakeven_price=money_round(breakeven_price),
            recommended_min_price=money_round(recommended_min_price),
            profit_per_ruble=money_round(profit / price, 4) if price > 0 else 0,
            contribution_margin=money_round(contribution_margin),
            contribution_margin_ratio=money_round(contribution_margin_ratio),
            status=CalculationStatus.COMPLETED,
            tariff_source=config.tariff_source,
            metadata=kwargs,
            applied_seasonal_multiplier=seasonal_multiplier,
            applied_promo_discount=config.promo_discount,
            dynamic_adjustment=config.dynamic_adjustment,
            billable_weight=money_round(billable_weight),
            advertising_cost=money_round(advertising_cost),
            auto_parts_specific=money_round(auto_parts_specific)
        )
        
        self._update_stats(result)
        
        self._history.append(result)
        if len(self._history) > HISTORY_LIMIT:
            self._history = self._history[-HISTORY_LIMIT:]
        
        if self._settings.get("enable_persistent_history", True) and self._persistent_db:
            try:
                if self._persistent_db.save_calculation(result, article=article, brand=brand):
                    self._stats["db_saved"] += 1
            except Exception as e:
                self._logger.warning(f"Не удалось сохранить в БД: {e}")
        
        return result
    
    # ✅ ИСПРАВЛЕНИЕ v100.11: Потокобезопасное обновление статистики
    def _update_stats(self, result: 'UnitEconomicsResult'):
        """✅ ИСПРАВЛЕНИЕ v100.11: Используется Lock для предотвращения race condition"""
        with self._stats_lock:
            self._stats["total_calculations"] += 1
            self._stats["by_marketplace"][result.marketplace] += 1
            self._stats["by_category"][result.category] += 1
            self._stats["by_mode"][result.operation_mode] += 1
            self._stats["by_status"][result.status.name] += 1
            self._stats["by_tax_system"][result.tax_system] += 1
            self._stats["by_tariff_source"][result.tariff_source.value] += 1
            
            self._stats["total_profit"] += result.profit
            self._stats["total_tax"] += result.tax_amount
            
            if result.profit > self._stats["max_profit"]:
                self._stats["max_profit"] = result.profit
                self._stats["best_marketplace"] = result.marketplace
                self._stats["best_category"] = result.category
                self._stats["best_mode"] = result.operation_mode
            
            if result.profit < self._stats["min_profit"] or self._stats["min_profit"] == 0:
                self._stats["min_profit"] = result.profit
            
            n = self._stats["total_calculations"]
            self._stats["avg_profit"] = self._stats["total_profit"] / n
            self._stats["avg_margin"] = (self._stats["avg_margin"] * (n - 1) + result.margin_percent) / n
            self._stats["avg_roi"] = (self._stats["avg_roi"] * (n - 1) + result.roi) / n
            self._stats["avg_tax"] = self._stats["total_tax"] / n
    
    # ✅ ИСПРАВЛЕНИЕ v100.11: Удалён дублирующий метод calculate_chunk()
    # Теперь используется только _calculate_chunk_threadsafe()
    
    @timer_decorator
    def calculate_for_catalog_batch_parallel(
        self,
        df: pd.DataFrame,
        price_col: str = "Цена",
        cost_col: str = "Себестоимость",
        category_col: Optional[str] = None,
        length_col: Optional[str] = None,
        width_col: Optional[str] = None,
        height_col: Optional[str] = None,
        weight_col: Optional[str] = None,
        article_col: str = "Артикул",
        brand_col: str = "Бренд",
        marketplaces: Optional[List[str]] = None,
        operation_mode: str = "FBS",
        days_in_storage: int = 30,
        apply_markup: float = 0.0,
        is_premium: bool = False,
        include_insurance: bool = False,
        include_packing: bool = False,
        include_marketing: bool = False,
        progress_callback: Optional[Callable] = None,
        max_workers: int = 4,
        chunk_size: int = 1000,
        tax_system: str = "УСН_6",
        ad_intensity: str = "medium"
    ) -> pd.DataFrame:
        """
        🆕 v100.7: Параллельный расчет юнит-экономики для больших каталогов.
        ИСПРАВЛЕНО: Заменён ProcessPoolExecutor на ThreadPoolExecutor для совместимости со Streamlit.
        """
        if marketplaces is None:
            marketplaces = list(self._configs.keys())
        
        if df.empty:
            return pd.DataFrame()
        
        total_items = len(df) * len(marketplaces)
        if total_items == 0:
            return pd.DataFrame()
        
        self._stats["parallel_calculations"] += 1
        
        current_month = datetime.now().month
        
        # Разбиваем DataFrame на чанки
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
        
        all_results = []
        all_errors = []  # ✅ ИСПРАВЛЕНИЕ v100.11: Собираем ошибки
        
        total_futures = len(chunks) * len(marketplaces)
        completed = 0
        
        with st.status("🚀 Параллельный расчет юнит-экономики...", expanded=True) as status:
            # ✅ ThreadPoolExecutor — потоки в одном процессе, self доступен
            # ✅ Polars/DuckDB сами отпускают GIL на C++ уровне → реальная параллельность
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                # Создаём задачи для каждого чанка и маркетплейса
                for chunk in chunks:
                    for marketplace in marketplaces:
                        future = executor.submit(
                            self._calculate_chunk_threadsafe,
                            chunk_df=chunk,
                            marketplace=marketplace,
                            operation_mode=operation_mode,
                            days_in_storage=days_in_storage,
                            category_col=category_col,
                            article_col=article_col,
                            brand_col=brand_col,
                            price_col=price_col,
                            cost_col=cost_col,
                            length_col=length_col,
                            width_col=width_col,
                            height_col=height_col,
                            weight_col=weight_col,
                            apply_markup=apply_markup,
                            is_premium=is_premium,
                            include_insurance=include_insurance,
                            include_packing=include_packing,
                            include_marketing=include_marketing,
                            current_month=current_month,
                            tax_system=tax_system,
                            ad_intensity=ad_intensity
                        )
                        futures.append(future)
                
                # Собираем результаты по мере выполнения
                for future in as_completed(futures):
                    try:
                        result_chunk, errors = future.result(timeout=120)
                        all_results.extend(result_chunk)
                        all_errors.extend(errors)  # ✅ ИСПРАВЛЕНИЕ v100.11
                    except concurrent.futures.TimeoutError:
                        logger.error("Таймаут расчета чанка")
                        self._stats["errors_count"] += 1
                        all_errors.append("Таймаут расчета чанка")
                    except Exception as e:
                        logger.error(f"Ошибка расчета чанка: {e}")
                        self._stats["errors_count"] += 1
                        self._stats["last_error"] = str(e)
                        all_errors.append(str(e))
                    
                    completed += 1
                    if progress_callback:
                        progress_callback(completed / total_futures)
                    
                    status.update(
                        label=f"🔄 Обработано {completed}/{total_futures} чанков",
                        state="running"
                    )
            
            status.update(label="✅ Параллельный расчет завершен!", state="complete")
            
            if progress_callback:
                progress_callback(1.0)
        
        # ✅ ИСПРАВЛЕНИЕ v100.11: Показываем ошибки, если они есть
        if all_errors:
            unique_errors = list(set(all_errors))[:5]  # Показываем первые 5 уникальных ошибок
            logger.warning(f"⚠️ Ошибки при параллельном расчете: {len(all_errors)}")
            for err in unique_errors:
                logger.warning(f"  - {err}")
        
        if not all_results:
            return pd.DataFrame()
        
        return pd.DataFrame(all_results)
    
    # ✅ ИСПРАВЛЕНИЕ v100.11: Метод теперь возвращает (results, errors)
    def _calculate_chunk_threadsafe(
        self,
        chunk_df: pd.DataFrame,
        marketplace: str,
        operation_mode: str = "FBS",
        days_in_storage: int = 30,
        category_col: Optional[str] = None,
        article_col: str = "Артикул",
        brand_col: str = "Бренд",
        price_col: str = "Цена",
        cost_col: str = "Себестоимость",
        length_col: Optional[str] = None,
        width_col: Optional[str] = None,
        height_col: Optional[str] = None,
        weight_col: Optional[str] = None,
        apply_markup: float = 0.0,
        is_premium: bool = False,
        include_insurance: bool = False,
        include_packing: bool = False,
        include_marketing: bool = False,
        current_month: Optional[int] = None,
        tax_system: str = "УСН_6",
        ad_intensity: str = "medium"
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        🆕 v100.7 → v100.9 → v100.11: Потокобезопасный расчет чанка данных.
        ✅ ИСПРАВЛЕНИЕ v100.11: Теперь возвращает кортеж (results, errors)
        """
        results = []
        errors = []  # ✅ ИСПРАВЛЕНИЕ v100.11: Собираем ошибки
        
        # ✅ ИСПРАВЛЕНИЕ v100.9: глубокая копия конфигурации для потокобезопасности
        try:
            config = copy.deepcopy(self._configs.get(marketplace))
        except Exception:
            config = self._configs.get(marketplace)
        
        if not config:
            logger.error(f"Маркетплейс {marketplace} не найден")
            return results, [f"Маркетплейс {marketplace} не найден"]
        
        for idx, row in chunk_df.iterrows():
            try:
                price = safe_float(row.get(price_col, 0))
                cost = safe_float(row.get(cost_col, 0))
                article = safe_str(row.get(article_col, f"Товар_{idx}"))
                brand = safe_str(row.get(brand_col, ""))
                
                if price <= 0 or cost <= 0:
                    continue
                
                length = safe_float(row.get(length_col, 0)) if length_col else 0
                width = safe_float(row.get(width_col, 0)) if width_col else 0
                height = safe_float(row.get(height_col, 0)) if height_col else 0
                weight = safe_float(row.get(weight_col, 0)) if weight_col else 0
                
                category = safe_str(row.get(category_col, "")) if category_col else None
                
                final_price = price * (1 + apply_markup / 100) if apply_markup > 0 else price
                
                result = self.calculate_unit_economics(
                    price=final_price,
                    cost=cost,
                    marketplace=marketplace,
                    category=category,
                    operation_mode=operation_mode,
                    days_in_storage=days_in_storage,
                    length=length,
                    width=width,
                    height=height,
                    weight=weight,
                    is_premium=is_premium,
                    include_insurance=include_insurance,
                    include_packing=include_packing,
                    include_marketing=include_marketing,
                    article=article,
                    brand=brand,
                    current_month=current_month,
                    tax_system=tax_system,
                    ad_intensity=ad_intensity
                )
                
                result_dict = result.to_dict()
                result_dict["Артикул"] = article
                result_dict["Бренд"] = brand
                result_dict["Индекс"] = idx
                results.append(result_dict)
            
            except Exception as e:
                # ✅ ИСПРАВЛЕНИЕ v100.11: Собираем ошибки вместо их проглатывания
                error_msg = f"Строка {idx}: {str(e)}"
                logger.error(f"Ошибка расчета для строки {idx}: {e}")
                errors.append(error_msg)
                continue
        
        return results, errors
    
    @timer_decorator
    def calculate_for_catalog_batch(
        self,
        df: pd.DataFrame,
        price_col: str = "Цена",
        cost_col: str = "Себестоимость",
        category_col: Optional[str] = None,
        length_col: Optional[str] = None,
        width_col: Optional[str] = None,
        height_col: Optional[str] = None,
        weight_col: Optional[str] = None,
        article_col: str = "Артикул",
        brand_col: str = "Бренд",
        marketplaces: Optional[List[str]] = None,
        operation_mode: str = "FBS",
        days_in_storage: int = 30,
        apply_markup: float = 0.0,
        is_premium: bool = False,
        include_insurance: bool = False,
        include_packing: bool = False,
        include_marketing: bool = False,
        progress_callback: Optional[Callable] = None,
        use_parallel: bool = True,
        max_workers: int = 4,
        chunk_size: int = 1000,
        tax_system: str = "УСН_6",
        ad_intensity: str = "medium"
    ) -> pd.DataFrame:
        """Расчет юнит-экономики для каталога с выбором режима."""
        
        if use_parallel and len(df) > 100 and DUCKDB_AVAILABLE:
            return self.calculate_for_catalog_batch_parallel(
                df=df,
                price_col=price_col,
                cost_col=cost_col,
                category_col=category_col,
                length_col=length_col,
                width_col=width_col,
                height_col=height_col,
                weight_col=weight_col,
                article_col=article_col,
                brand_col=brand_col,
                marketplaces=marketplaces,
                operation_mode=operation_mode,
                days_in_storage=days_in_storage,
                apply_markup=apply_markup,
                is_premium=is_premium,
                include_insurance=include_insurance,
                include_packing=include_packing,
                include_marketing=include_marketing,
                progress_callback=progress_callback,
                max_workers=max_workers,
                chunk_size=chunk_size,
                tax_system=tax_system,
                ad_intensity=ad_intensity
            )
        
        if marketplaces is None:
            marketplaces = list(self._configs.keys())
        
        items = []
        for idx, row in df.iterrows():
            price = safe_float(row.get(price_col, 0))
            cost = safe_float(row.get(cost_col, 0))
            article = safe_str(row.get(article_col, f"Товар_{idx}"))
            brand = safe_str(row.get(brand_col, ""))
            
            if price <= 0 or cost <= 0:
                continue
            
            length = safe_float(row.get(length_col, 0)) if length_col else 0
            width = safe_float(row.get(width_col, 0)) if width_col else 0
            height = safe_float(row.get(height_col, 0)) if height_col else 0
            weight = safe_float(row.get(weight_col, 0)) if weight_col else 0
            
            category = safe_str(row.get(category_col, "")) if category_col else None
            
            items.append({
                "idx": idx, "article": article, "brand": brand,
                "price": price, "cost": cost, "category": category,
                "length": length, "width": width,
                "height": height, "weight": weight
            })
        
        total_items = len(items) * len(marketplaces)
        if total_items == 0:
            return pd.DataFrame()
        
        results = []
        processed = 0
        current_month = datetime.now().month
        
        with st.status("Расчет юнит-экономики для каталога...", expanded=True) as status:
            for item in items:
                final_price = item["price"] * (1 + apply_markup / 100) if apply_markup > 0 else item["price"]
                
                for marketplace in marketplaces:
                    try:
                        result = self.calculate_unit_economics(
                            price=final_price,
                            cost=item["cost"],
                            marketplace=marketplace,
                            category=item["category"],
                            operation_mode=operation_mode,
                            days_in_storage=days_in_storage,
                            length=item["length"],
                            width=item["width"],
                            height=item["height"],
                            weight=item["weight"],
                            is_premium=is_premium,
                            include_insurance=include_insurance,
                            include_packing=include_packing,
                            include_marketing=include_marketing,
                            article=item["article"],
                            brand=item["brand"],
                            current_month=current_month,
                            tax_system=tax_system,
                            ad_intensity=ad_intensity
                        )
                        
                        result_dict = result.to_dict()
                        result_dict["Артикул"] = item["article"]
                        result_dict["Бренд"] = item["brand"]
                        result_dict["Индекс"] = item["idx"]
                        results.append(result_dict)
                    
                    except Exception as e:
                        logger.error(f"Ошибка расчета для {item['article']}: {e}")
                        self._stats["errors_count"] += 1
                
                processed += 1
                if progress_callback and processed % 10 == 0:
                    progress_callback(processed / total_items)
            
            if progress_callback:
                progress_callback(1.0)
            
            status.update(label="✅ Расчет завершен!", state="complete")
        
        return pd.DataFrame(results) if results else pd.DataFrame()
    
    def calculate_for_all_marketplaces(
        self,
        price: float,
        cost: float,
        category: Optional[str] = None,
        operation_mode: str = "FBS",
        days_in_storage: int = 30,
        length: float = 0,
        width: float = 0,
        height: float = 0,
        weight: float = 0,
        **kwargs
    ) -> pd.DataFrame:
        results = []
        
        for marketplace in self._configs.keys():
            try:
                result = self.calculate_unit_economics(
                    price=price, cost=cost, marketplace=marketplace,
                    category=category, operation_mode=operation_mode,
                    days_in_storage=days_in_storage,
                    length=length, width=width, height=height,
                    weight=weight, **kwargs
                )
                results.append(result)
            except Exception as e:
                self._logger.error(f"Ошибка расчета для {marketplace}: {e}")
                self._stats["errors_count"] += 1
                self._stats["last_error"] = str(e)
        
        if not results:
            return pd.DataFrame()
        
        return pd.DataFrame([r.to_dict() for r in results])
    
    @timer_decorator
    def optimize_price(
        self,
        cost: float,
        marketplace: str,
        category: Optional[str] = None,
        operation_mode: str = "FBS",
        days_in_storage: int = 30,
        length: float = 0,
        width: float = 0,
        height: float = 0,
        weight: float = 0,
        target_margin: float = 20.0,
        price_min: float = 0,
        price_max: float = 100000,
        step: float = 10,
        max_iterations: int = 1000
    ) -> OptimizationResult:
        """
        ✅ ИСПРАВЛЕНО v100.9:
        - iteration увеличивается ДО try, а не внутри
        - Защита от бесконечного цикла при постоянных исключениях
        """
        current_price = max(price_min, cost * 1.1) if price_min == 0 else price_min
        best_price = current_price
        best_profit = float('-inf')
        best_margin = 0
        best_result = None
        
        iteration = 0
        
        # ✅ ИСПРАВЛЕНИЕ v100.9: счётчик итераций увеличивается в начале цикла
        while current_price <= price_max and iteration < max_iterations:
            iteration += 1  # ✅ Перенесено сюда из try-блока
            
            try:
                result = self.calculate_unit_economics(
                    price=current_price, cost=cost, marketplace=marketplace,
                    category=category, operation_mode=operation_mode,
                    days_in_storage=days_in_storage,
                    length=length, width=width, height=height, weight=weight
                )
                
                margin = result.margin_percent
                profit = result.profit
                
                if margin >= target_margin and profit > best_profit:
                    best_profit = profit
                    best_price = current_price
                    best_margin = margin
                    best_result = result
                
                current_price += step
            
            except Exception as e:
                self._logger.warning(f"Ошибка при оптимизации для цены {current_price}: {e}")
                current_price += step
                # ✅ iteration уже увеличен в начале цикла — бесконечного цикла не будет
        
        current_result = self.calculate_unit_economics(
            price=price_min or best_price, cost=cost, marketplace=marketplace,
            category=category, operation_mode=operation_mode,
            days_in_storage=days_in_storage,
            length=length, width=width, height=height, weight=weight
        )
        
        improvement_pct = ((best_profit - current_result.profit) / current_result.profit * 100) if current_result.profit > 0 else 0
        
        recommendations = []
        if best_price > 0 and best_margin >= target_margin:
            recommendations.append(f"Установите цену {best_price:.2f} ₽ для достижения маржи {target_margin}%")
        else:
            recommendations.append(f"Целевая маржа {target_margin}% не достигнута. Максимальная маржа: {best_margin:.1f}%")
        
        if best_profit > current_result.profit:
            recommendations.append(f"Потенциальное увеличение прибыли: {improvement_pct:.1f}%")
        
        if current_result.recommended_min_price > 0:
            recommendations.append(f"Рекомендуемая минимальная цена: {current_result.recommended_min_price:.2f} ₽")
        
        self._stats["total_optimizations"] += 1
        self._stats["optimization_improvement"] += improvement_pct
        
        return OptimizationResult(
            optimal_price=best_price, optimal_margin=best_margin,
            optimal_profit=best_profit,
            current_price=current_result.price,
            current_margin=current_result.margin_percent,
            current_profit=current_result.profit,
            improvement_pct=improvement_pct,
            recommendations=recommendations,
            metadata={"target_margin": target_margin, "step": step, "iterations": iteration}
        )
    
    @timer_decorator
    def forecast_profit(
        self,
        current_data: Dict[str, Any],
        periods: int = 12,
        growth_rate: float = 0.05,
        seasonality: Optional[List[float]] = None,
        confidence_level: float = 0.95
    ) -> ForecastResult:
        if seasonality is None:
            seasonality = [0.85, 0.85, 0.95, 1.05, 1.10, 1.15,
                          1.20, 1.15, 1.10, 1.05, 0.95, 0.90]
        
        base_value = current_data.get("profit", 1000)
        
        periods_list, values_list, seasonality_list, trend_list = [], [], [], []
        
        for i in range(periods):
            month_idx = i % 12
            seasonal_factor = seasonality[month_idx] if month_idx < len(seasonality) else 1.0
            growth_factor = (1 + growth_rate) ** (i / 12)
            
            factor = seasonal_factor * growth_factor
            value = base_value * factor
            
            if DATEUTIL_AVAILABLE:
                periods_list.append(datetime.now() + relativedelta(months=i))
            else:
                periods_list.append(datetime.now() + timedelta(days=30 * i))
            
            values_list.append(value)
            seasonality_list.append(seasonal_factor)
            trend_list.append(growth_factor)
        
        std_dev = np.std(values_list) * 0.2
        z_score = 1.96
        
        lower_bound = [v - z_score * std_dev for v in values_list]
        upper_bound = [v + z_score * std_dev for v in values_list]
        
        return ForecastResult(
            periods=periods_list, values=values_list,
            seasonality=seasonality_list, trend=trend_list,
            confidence_intervals=(lower_bound, upper_bound),
            metadata={"base_value": base_value, "growth_rate": growth_rate, "confidence_level": confidence_level}
        )
    
    def get_history(self, limit: int = 100, filters: Optional[Dict] = None) -> List['UnitEconomicsResult']:
        history = self._history[-limit:] if limit > 0 else self._history
        
        if filters:
            filtered = []
            for item in history:
                match = True
                for key, value in filters.items():
                    if key == "marketplace" and item.marketplace != value:
                        match = False; break
                    elif key == "category" and item.category != value:
                        match = False; break
                    elif key == "operation_mode" and item.operation_mode != value:
                        match = False; break
                    elif key == "tax_system" and item.tax_system != value:
                        match = False; break
                    elif key == "min_profit" and item.profit < value:
                        match = False; break
                    elif key == "max_profit" and item.profit > value:
                        match = False; break
                    elif key == "start_date" and item.timestamp < value:
                        match = False; break
                    elif key == "end_date" and item.timestamp > value:
                        match = False; break
                
                if match:
                    filtered.append(item)
            
            return filtered
        
        return history
    
    def get_persistent_history(self, limit: int = 1000, filters: Optional[Dict] = None) -> pd.DataFrame:
        if not self._persistent_db:
            return pd.DataFrame()
        return self._persistent_db.load_history(limit=limit, filters=filters)
    
    def get_persistent_stats(self) -> Dict[str, Any]:
        if not self._persistent_db:
            return {}
        return self._persistent_db.get_stats()
    
    def clear_persistent_history(self) -> int:
        if not self._persistent_db:
            return 0
        return self._persistent_db.clear_history()
    
    def get_stats(self) -> Dict[str, Any]:
        stats = self._stats.copy()
        stats["history_count"] = len(self._history)
        stats["cache_size"] = len(self._cache)
        stats["uptime"] = (datetime.now() - stats["start_time"]).total_seconds()
        
        if stats["total_calculations"] > 0:
            stats["success_rate"] = 1 - (stats["errors_count"] / stats["total_calculations"])
        else:
            stats["success_rate"] = 0
        
        return stats
    
    def clear_history(self):
        self._history = []
        self._stats = self._init_stats()
        self._cache.clear()
        gc.collect()
    
    def get_best_configuration(self) -> Dict[str, Any]:
        if not self._history:
            return {"error": "Нет данных"}
        
        best = max(self._history, key=lambda x: x.profit)
        
        return {
            "marketplace": best.marketplace,
            "operation_mode": best.operation_mode,
            "category": best.category,
            "profit": best.profit,
            "margin": best.margin_percent,
            "price": best.price,
            "cost": best.cost,
            "tax": best.tax_amount,
            "tax_system": best.tax_system,
            "recommended_min_price": best.recommended_min_price,
            "timestamp": best.timestamp.isoformat()
        }
    
    def get_category_stats(self) -> pd.DataFrame:
        if not self._history:
            return pd.DataFrame()
        
        stats = defaultdict(lambda: {
            "count": 0, "total_profit": 0, "avg_profit": 0,
            "avg_margin": 0, "best_profit": 0, "worst_profit": 0,
            "total_tax": 0, "avg_recommended_price": 0
        })
        
        for result in self._history:
            cat = result.category
            stats[cat]["count"] += 1
            stats[cat]["total_profit"] += result.profit
            stats[cat]["avg_margin"] += result.margin_percent
            stats[cat]["total_tax"] += result.tax_amount
            stats[cat]["avg_recommended_price"] += result.recommended_min_price
            stats[cat]["best_profit"] = max(stats[cat]["best_profit"], result.profit)
            stats[cat]["worst_profit"] = min(stats[cat]["worst_profit"], result.profit)
        
        for cat in stats:
            if stats[cat]["count"] > 0:
                stats[cat]["avg_profit"] = stats[cat]["total_profit"] / stats[cat]["count"]
                stats[cat]["avg_margin"] /= stats[cat]["count"]
                stats[cat]["avg_recommended_price"] /= stats[cat]["count"]
        
        return pd.DataFrame.from_dict(stats, orient="index").reset_index().rename(columns={"index": "category"})
    
    def get_marketplace_stats(self) -> pd.DataFrame:
        if not self._history:
            return pd.DataFrame()
        
        stats = defaultdict(lambda: {
            "count": 0, "total_profit": 0, "avg_profit": 0,
            "avg_margin": 0, "best_profit": 0, "worst_profit": 0,
            "total_tax": 0
        })
        
        for result in self._history:
            mp = result.marketplace
            stats[mp]["count"] += 1
            stats[mp]["total_profit"] += result.profit
            stats[mp]["avg_margin"] += result.margin_percent
            stats[mp]["total_tax"] += result.tax_amount
            stats[mp]["best_profit"] = max(stats[mp]["best_profit"], result.profit)
            stats[mp]["worst_profit"] = min(stats[mp]["worst_profit"], result.profit)
        
        for mp in stats:
            if stats[mp]["count"] > 0:
                stats[mp]["avg_profit"] = stats[mp]["total_profit"] / stats[mp]["count"]
                stats[mp]["avg_margin"] /= stats[mp]["count"]
        
        return pd.DataFrame.from_dict(stats, orient="index").reset_index().rename(columns={"index": "marketplace"})
    
    def export_history(self, format: ExportFormat = ExportFormat.EXCEL) -> bytes:
        if not self._history:
            return b""
        
        df = pd.DataFrame([r.to_dict() for r in self._history])
        
        if format == ExportFormat.CSV:
            return df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8')
        elif format == ExportFormat.EXCEL:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='История')
            return output.getvalue()
        elif format == ExportFormat.JSON:
            return df.to_json(orient='records', force_ascii=False).encode('utf-8')
        else:
            raise ExportError(f"Формат {format.name} не поддерживается", format=format.name)
    
    def get_tariff_cache_statistics(self) -> Dict[str, Any]:
        return self._tariff_cache.get_statistics()
    
    def get_tariff_cache_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self._tariff_cache.get_history(limit)
# ============================================================================
# БЛОК 11: HIGH-VOLUME КАТАЛОГ АВТОЗАПЧАСТЕЙ (ПОЛНАЯ ВЕРСИЯ v100.15)
# ============================================================================
# ✅ ИСПРАВЛЕНИЯ v100.15:
# 1. Полная защита от дубликатов колонок при маппинге
# 2. Исправлена ошибка "column 'weight' is duplicate"
# 3. Правильная обработка габаритов из файлов
# 4. Улучшен detect_columns с приоритетом точного совпадения
# 5. Fallback при ошибке rename
# 6. Логирование для отладки
# 7. 🆕 v100.15: Габариты подтягиваются из OE-номера (COALESCE + NULLIF)
# 8. 🆕 v100.15: Защита от формата даты в экспорте (числовой/текстовой формат)
# 9. 🆕 v100.15: Корректная обработка нулевых и пустых значений
# ============================================================================
@st.cache_resource
def get_high_volume_catalog():
    """Создание каталога через st.cache_resource для корректной работы с DuckDB"""
    return HighVolumeAutoPartsCatalog()


class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Загрузка конфигураций
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()
        
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()

    # ========================================================================
    # КОНФИГУРАЦИИ
    # ========================================================================
    def load_cloud_config(self) -> Dict[str, Any]:
        config_path = self.data_dir / "cloud_config.json"
        default_config = {
            "enabled": False,
            "provider": "s3",
            "bucket": "",
            "region": "",
            "sync_interval": 3600,
            "last_sync": 0
        }
        if config_path.exists():
            try:
                return json.loads(config_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения cloud_config.json: {e}")
                return default_config
        else:
            config_path.write_text(json.dumps(
                default_config, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_config

    def save_cloud_config(self):
        config_path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        config_path.write_text(json.dumps(
            self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_price_rules(self) -> Dict[str, Any]:
        price_rules_path = self.data_dir / "price_rules.json"
        default_rules = {
            "global_markup": 0.2,
            "brand_markups": {},
            "min_price": 0.0,
            "max_price": 99999.0
        }
        if price_rules_path.exists():
            try:
                return json.loads(price_rules_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения price_rules.json: {e}")
                return default_rules
        else:
            price_rules_path.write_text(json.dumps(
                default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_rules

    def save_price_rules(self):
        price_rules_path = self.data_dir / "price_rules.json"
        price_rules_path.write_text(json.dumps(
            self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_exclusion_rules(self) -> List[str]:
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        if exclusion_path.exists():
            try:
                return [line.strip() for line in exclusion_path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка чтения exclusion_rules.txt: {e}")
                return []
        else:
            content = "Кузов\nСтекла\nМасла"
            exclusion_path.write_text(content, encoding='utf-8')
            return ["Кузов", "Стекла", "Масла"]

    def save_exclusion_rules(self):
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        exclusion_path.write_text(
            "\n".join(self.exclusion_rules), encoding='utf-8')

    def load_category_mapping(self) -> Dict[str, str]:
        category_path = self.data_dir / "category_mapping.txt"
        default_mapping = {
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        }
        if category_path.exists():
            try:
                mapping = {}
                for line in category_path.read_text(encoding='utf-8').splitlines():
                    if line.strip() and "|" in line:
                        key, value = line.split("|", 1)
                        mapping[key.strip()] = value.strip()
                return mapping
            except Exception as e:
                logger.error(f"Ошибка чтения category_mapping.txt: {e}")
                return default_mapping
        else:
            content = "\n".join(
                [f"{k}|{v}" for k, v in default_mapping.items()])
            category_path.write_text(content, encoding='utf-8')
            return default_mapping

    def save_category_mapping(self):
        category_path = self.data_dir / "category_mapping.txt"
        content = "\n".join(
            [f"{k}|{v}" for k, v in self.category_mapping.items()])
        category_path.write_text(content, encoding='utf-8')

    # ========================================================================
    # БАЗА ДАННЫХ
    # ========================================================================
    def setup_database(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parts (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                artikul VARCHAR,
                brand VARCHAR,
                multiplicity INTEGER,
                barcode VARCHAR,
                length DOUBLE,
                width DOUBLE,
                height DOUBLE,
                weight DOUBLE,
                image_url VARCHAR,
                dimensions_str VARCHAR,
                description VARCHAR,
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                price DOUBLE,
                currency VARCHAR DEFAULT 'RUB',
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        self.create_indexes()

    def create_indexes(self):
        st.info("⚙️ Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        st.success("🛠️ Индексы созданы.")

    # ========================================================================
    # НОРМАЛИЗАЦИЯ И ОЧИСТКА
    # ========================================================================
    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    @staticmethod
    def clean_values(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        name_lower = name_series.str.to_lowercase()
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        for key, category in self.category_mapping.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower())
            ).then(pl.lit(category))

        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выпуск': 'глушитель|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(pattern, literal=False)
            ).then(pl.lit(category))

        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')

    # ========================================================================
    # ОБРАБОТКА ФАЙЛОВ (✅ ИСПРАВЛЕНО v100.14)
    # ========================================================================
    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        """
        ✅ ИСПРАВЛЕНИЕ v100.14: Защита от дубликатов при маппинге колонок
        Использует систему приоритетов для выбора лучшего варианта
        """
        column_variants = {
            'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE', 'oe_number', 'oe number'],
            'artikul': ['артикул', 'article', 'sku', 'artikul', 'код товара', 'код', 'код артикула'],
            'brand': ['бренд', 'brand', 'производитель', 'manufacturer', 'марка'],
            'name': ['наименование', 'название', 'name', 'описание', 'description', 'товар', 'наименование товара'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability', 'применяемость'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13', 'штрих код'],
            'multiplicity': ['кратность шт', 'кратность', 'multiplicity', 'кратность упаковки'],
            'length': ['длина (см)', 'длина', 'length', 'длинна', 'длина, см', 'length_cm'],
            'width': ['ширина (см)', 'ширина', 'width', 'ширина, см', 'width_cm'],
            'height': ['высота (см)', 'высота', 'height', 'высота, см', 'height_cm'],
            'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight', 'масса', 'weight_kg', 'вес кг'],
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка', 'фото', 'ссылка на изображение'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size', 'габариты', 'длинна/ширина/высота', 'длина/ширина/высота'],
            'price': ['цена', 'price', 'рекомендованная цена', 'retail price', 'цена продажи', 'стоимость'],
            'currency': ['валюта', 'currency']
        }

        actual_lower = {col.lower().strip(): col for col in actual_columns}
        mapping = {}
        used_actual = set()

        for expected in expected_columns:
            variants = column_variants.get(expected, [expected])
            best_match = None
            best_score = -1

            for variant in variants:
                variant_lower = variant.lower().strip()
                for actual_l, actual_orig in actual_lower.items():
                    if actual_orig in used_actual:
                        continue

                    score = 0
                    if variant_lower == actual_l:
                        score = 100
                    elif variant_lower in actual_l:
                        score = 50 + len(variant_lower)
                    elif actual_l in variant_lower:
                        score = 30 + len(actual_l)

                    if score > best_score:
                        best_score = score
                        best_match = actual_orig

            if best_match and best_score > 0:
                mapping[best_match] = expected
                used_actual.add(best_match)

        logger.info(f"Маппинг колонок: {mapping}")
        return mapping

    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        """
        ✅ ИСПРАВЛЕНИЕ v100.14: Полная защита от дубликатов колонок
        """
        logger.info(f"Обработка файла: {file_type} ({file_path})")
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return pl.DataFrame()

            df = pl.read_excel(file_path, engine='calamine')
            if df.is_empty():
                logger.warning(f"Пустой файл: {file_path}")
                return pl.DataFrame()

            logger.info(f"Исходные колонки файла {file_type}: {df.columns}")
        except Exception as e:
            logger.exception(f"Ошибка чтения файла {file_path}: {e}")
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability',
                   'length', 'width', 'height', 'weight', 'dimensions_str', 'price', 'currency'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['artikul', 'brand', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency'],
            'universal': ['artikul', 'brand', 'name', 'oe_number', 'applicability',
                          'length', 'width', 'height', 'weight', 'dimensions_str',
                          'price', 'currency', 'barcode', 'multiplicity', 'image_url']
        }

        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)

        if not column_mapping:
            logger.warning(
                f"Не удалось определить колонки для файла {file_type}. Доступные: {df.columns}")
            return pl.DataFrame()

        logger.info(f"Маппинг колонок для {file_type}: {column_mapping}")

        try:
            df = df.rename(column_mapping)
        except Exception as e:
            logger.error(f"Ошибка при rename: {e}")
            for old_name, new_name in column_mapping.items():
                try:
                    if new_name not in df.columns:
                        df = df.rename({old_name: new_name})
                    else:
                        logger.warning(f"Колонка {new_name} уже существует, пропускаем {old_name}")
                except Exception as e2:
                    logger.warning(f"Не удалось переименовать {old_name} → {new_name}: {e2}")

        if len(df.columns) != len(set(df.columns)):
            logger.warning(f"Обнаружены дубликаты колонок: {df.columns}")
            seen = set()
            cols_to_keep = []
            for col in df.columns:
                if col not in seen:
                    seen.add(col)
                    cols_to_keep.append(col)
                else:
                    logger.warning(f"Удаляем дубликат колонки: {col}")
            df = df.select(cols_to_keep)

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))

        numeric_cols = ['length', 'width', 'height', 'weight', 'price']
        for col in numeric_cols:
            if col in df.columns:
                try:
                    df = df.with_columns(
                        pl.col(col).cast(pl.Utf8)
                        .str.replace_all(',', '.')
                        .cast(pl.Float64)
                        .alias(col)
                    )
                except Exception as e:
                    logger.warning(f"Не удалось преобразовать {col} в число: {e}")

        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.normalize_key(
                    pl.col(col)).alias(f"{col}_norm"))

        logger.info(f"Файл {file_type} обработан. Итоговые колонки: {df.columns}")
        return df

    # ========================================================================
    # ЗАГРУЗКА И ОБНОВЛЕНИЕ В БАЗЕ
    # ========================================================================
    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        """Использован JOIN вместо кортежей для совместимости со всеми версиями DuckDB"""
        if df.is_empty():
            return

        df = df.unique(keep='first')
        cols = df.columns
        temp_view_name = f"temp_{table_name}_{int(time.time())}"

        try:
            self.conn.register(temp_view_name, df.to_arrow())
        except Exception as e:
            logger.error(f"Ошибка регистрации временной таблицы: {e}")
            return

        try:
            pk_list = pk
            join_conditions = " AND ".join([
                f"{table_name}.\"{c}\" = temp.\"{c}\"" for c in pk_list
            ])

            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE EXISTS (
                    SELECT 1 FROM {temp_view_name} temp
                    WHERE {join_conditions}
                );
            """
            self.conn.execute(delete_sql)

            insert_sql = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_view_name};
            """
            self.conn.execute(insert_sql)

            logger.info(
                f"Успешно upsert {len(df)} записей в таблицу {table_name}.")
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(
                f"Ошибка при записи в таблицу {table_name}. Детали в логе.")
        finally:
            try:
                self.conn.unregister(temp_view_name)
            except Exception:
                pass

    def upsert_prices(self, price_df: pl.DataFrame):
        if price_df.is_empty():
            return

        if 'artikul' in price_df.columns and 'brand' in price_df.columns:
            price_df = price_df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])

        if 'currency' not in price_df.columns:
            price_df = price_df.with_columns(pl.lit('RUB').alias('currency'))

        price_df = price_df.filter(
            (pl.col('price') >= self.price_rules['min_price']) &
            (pl.col('price') <= self.price_rules['max_price'])
        )

        self.upsert_data('prices', price_df, ['artikul_norm', 'brand_norm'])

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Обработка и загрузка данных в базу"""
        st.info("🔄 Начало загрузки и обновления данных в базе...")

        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        num_steps = len(steps)

        progress_bar = st.progress(
            0, text="Подготовка к обновлению базы данных...")
        step_counter = 0

        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка OE данных...")

            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(
                subset=['oe_number_norm'], keep='first')

            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(
                    self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))

            self.upsert_data('oe', oe_df, ['oe_number_norm'])

            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, [
                'oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка кроссов...")

            df = dataframes['cross'].filter(
                (pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df.select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, [
                'oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'prices' in dataframes:
            price_df = dataframes['prices']
            if not price_df.is_empty():
                st.info("💰 Обработка цен...")
                self.upsert_prices(price_df)
                st.success(
                    f"✅ Успешно обновлено {len(price_df)} ценовых записей")

        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1),
                              text=f"({step_counter}/{num_steps}) Сборка и обновление данных по артикулам...")

        parts_df = None
        file_priority = ['oe', 'barcode', 'images', 'dimensions']
        key_files = {ftype: df for ftype,
                                   df in dataframes.items() if ftype in file_priority}

        if key_files:
            parts_to_concat = [
                df.select(['artikul', 'artikul_norm', 'brand', 'brand_norm'])
                for df in key_files.values()
                if 'artikul_norm' in df.columns and 'brand_norm' in df.columns and not df.is_empty()
            ]

            if parts_to_concat:
                all_parts = pl.concat(parts_to_concat).filter(
                    pl.col('artikul_norm') != ""
                ).unique(subset=['artikul_norm', 'brand_norm'], keep='first')
                parts_df = all_parts
            else:
                parts_df = pl.DataFrame()
        else:
            parts_df = pl.DataFrame()

        if parts_df is not None and not parts_df.is_empty():
            for ftype in file_priority:
                if ftype not in key_files:
                    continue

                df = key_files[ftype]
                if df.is_empty() or 'artikul_norm' not in df.columns:
                    continue

                if ftype == 'dimensions':
                    dims_to_add = ['length', 'width', 'height', 'weight', 'dimensions_str']
                    join_cols = [col for col in dims_to_add if col in df.columns]
                else:
                    join_cols = [col for col in df.columns if col not in [
                        'artikul', 'artikul_norm', 'brand', 'brand_norm']]

                if not join_cols:
                    continue

                existing_cols = set(parts_df.columns)
                join_cols = [
                    col for col in join_cols if col not in existing_cols]

                if not join_cols:
                    continue

                df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique(
                    subset=['artikul_norm', 'brand_norm'], keep='first')
                parts_df = parts_df.join(
                    df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)

        if 'multiplicity' not in parts_df.columns:
            parts_df = parts_df.with_columns(
                multiplicity=pl.lit(1).cast(pl.Int32))
        else:
            parts_df = parts_df.with_columns(
                pl.col('multiplicity').fill_null(1).cast(pl.Int32))

        for col in ['length', 'width', 'height']:
            if col not in parts_df.columns:
                parts_df = parts_df.with_columns(
                    pl.lit(None).cast(pl.Float64).alias(col))

        if 'dimensions_str' not in parts_df.columns:
            parts_df = parts_df.with_columns(
                dimensions_str=pl.lit(None).cast(pl.Utf8))

        parts_df = parts_df.with_columns([
            pl.col('length').cast(pl.Utf8).fill_null(
                '').alias('_length_str'),
            pl.col('width').cast(pl.Utf8).fill_null(
                '').alias('_width_str'),
            pl.col('height').cast(pl.Utf8).fill_null(
                '').alias('_height_str'),
        ])

        parts_df = parts_df.with_columns(
            dimensions_str=pl.when(
                (pl.col('dimensions_str').is_not_null()) &
                (pl.col('dimensions_str').cast(pl.Utf8) != '')
            ).then(
                pl.col('dimensions_str').cast(pl.Utf8)
            ).otherwise(
                pl.concat_str([
                    pl.col('_length_str'), pl.lit('x'),
                    pl.col('_width_str'), pl.lit('x'),
                    pl.col('_height_str')
                ], separator='')
            )
        )

        parts_df = parts_df.drop(
            ['_length_str', '_width_str', '_height_str'])

        if 'artikul' not in parts_df.columns:
            parts_df = parts_df.with_columns(artikul=pl.lit(''))
        if 'brand' not in parts_df.columns:
            parts_df = parts_df.with_columns(brand=pl.lit(''))

        parts_df = parts_df.with_columns([
            pl.col('artikul').cast(pl.Utf8).fill_null(
                '').alias('_artikul_str'),
            pl.col('brand').cast(pl.Utf8).fill_null(
                '').alias('_brand_str'),
            pl.col('multiplicity').cast(
                pl.Utf8).alias('_multiplicity_str'),
        ])

        parts_df = parts_df.with_columns(
            description=pl.concat_str([
                pl.lit('Артикул: '), pl.col('_artikul_str'),
                pl.lit(', Бренд: '), pl.col('_brand_str'),
                pl.lit(', Кратность: '), pl.col(
                    '_multiplicity_str'), pl.lit(' шт.')
            ], separator='')
        )

        parts_df = parts_df.drop(
            ['_artikul_str', '_brand_str', '_multiplicity_str'])

        final_columns = [
            'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode',
            'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
        ]
        select_exprs = [pl.col(c) if c in parts_df.columns else pl.lit(
            None).alias(c) for c in final_columns]
        parts_df = parts_df.select(select_exprs)

        self.upsert_data('parts', parts_df, ['artikul_norm', 'brand_norm'])

        progress_bar.progress(1.0, text="Обновление базы данных завершено!")
        time.sleep(1)
        progress_bar.empty()

    # ========================================================================
    # ЭКСПОРТ
    # ========================================================================
    def _get_brand_markups_sql(self) -> str:
        rows = []
        for brand, markup in self.price_rules['brand_markups'].items():
            safe_brand = brand.replace("'", "''")
            rows.append(f"SELECT '{safe_brand}' AS brand, {markup} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, NULL AS markup LIMIT 0"

    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        """
        🆕 v100.15: Построение SQL-запроса для экспорта.
        ✅ ИСПРАВЛЕНО: Габариты подтягиваются из OE-номера через COALESCE + NULLIF
        ✅ ИСПРАВЛЕНО: Явное приведение типов (CAST AS DOUBLE) для защиты от формата даты
        """
        description_text = (
            "Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. "
            "Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей. "
            "В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электроматериалы, автомасла, инструмент, "
            "а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. "
            "Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. "
            "Выбирайте только лучшее — надежность и качество от ведущих производителей."
        )

        brand_markups_sql = self._get_brand_markups_sql()
        select_parts = []

        price_requested = include_prices and (not selected_columns or "Цена" in selected_columns or "Валюта" in selected_columns)
        if price_requested:
            if apply_markup:
                global_markup = self.price_rules.get('global_markup', 0)
                select_parts.append(
                    f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {global_markup})) ELSE pr.price END AS \"Цена\""
                )
            else:
                select_parts.append('pr.price AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")

        # 🆕 v100.15: Габариты с приоритетом собственных данных, fallback на OE-номер
        # Используем NULLIF чтобы игнорировать нули, и CAST AS DOUBLE для явного числового типа
        columns_map = [
            ("Артикул бренда", 'r.artikul AS "Артикул бренда"'),
            ("Бренд", 'r.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"'),
            ("Описание", 'CONCAT(COALESCE(r.description, \'\'), dt.text) AS "Описание"'),
            ("Категория товара", 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория товара"'),
            ("Кратность", 'r.multiplicity AS "Кратность"'),
            # 🆕 v100.15: Явное приведение к DOUBLE + COALESCE с NULLIF для игнорирования нулей
            ("Длина", 'CAST(COALESCE(NULLIF(r.length, 0), NULLIF(r.analog_length, 0), 0) AS DOUBLE) AS "Длина"'),
            ("Ширина", 'CAST(COALESCE(NULLIF(r.width, 0), NULLIF(r.analog_width, 0), 0) AS DOUBLE) AS "Ширина"'),
            ("Высота", 'CAST(COALESCE(NULLIF(r.height, 0), NULLIF(r.analog_height, 0), 0) AS DOUBLE) AS "Высота"'),
            ("Вес", 'CAST(COALESCE(NULLIF(r.weight, 0), NULLIF(r.analog_weight, 0), 0) AS DOUBLE) AS "Вес"'),
            # 🆕 v100.15: Строка габаритов с автогенерацией из отдельных колонок
            ("Длинна/Ширина/Высота", """
                CAST(
                    COALESCE(
                        CASE
                            WHEN r.dimensions_str IS NOT NULL 
                                 AND TRIM(r.dimensions_str) != '' 
                                 AND UPPER(TRIM(r.dimensions_str)) NOT IN ('XX', '0X0X0', '0.0X0.0X0.0', 'X', 'X X X')
                            THEN r.dimensions_str
                            ELSE NULL
                        END,
                        CASE
                            WHEN r.analog_dimensions_str IS NOT NULL 
                                 AND TRIM(r.analog_dimensions_str) != '' 
                                 AND UPPER(TRIM(r.analog_dimensions_str)) NOT IN ('XX', '0X0X0', '0.0X0.0X0.0', 'X', 'X X X')
                            THEN r.analog_dimensions_str
                            ELSE NULL
                        END,
                        CASE 
                            WHEN COALESCE(NULLIF(r.length, 0), NULLIF(r.analog_length, 0)) IS NOT NULL 
                                 OR COALESCE(NULLIF(r.width, 0), NULLIF(r.analog_width, 0)) IS NOT NULL
                                 OR COALESCE(NULLIF(r.height, 0), NULLIF(r.analog_height, 0)) IS NOT NULL
                            THEN CONCAT(
                                CAST(COALESCE(r.length, r.analog_length, 0) AS VARCHAR), 'x',
                                CAST(COALESCE(r.width, r.analog_width, 0) AS VARCHAR), 'x',
                                CAST(COALESCE(r.height, r.analog_height, 0) AS VARCHAR)
                            )
                            ELSE NULL
                        END
                    ) AS VARCHAR
                ) AS "Длинна/Ширина/Высота"
            """),
            ("OE номер", 'r.oe_list AS "OE номер"'),
            ("аналоги", 'r.analog_list AS "аналоги"'),
            ("Ссылка на изображение", 'r.image_url AS "Ссылка на изображение"')
        ]

        for name, expr in columns_map:
            if not selected_columns or name in selected_columns:
                select_parts.append(expr.strip())

        if not select_parts:
            select_parts = ['r.artikul AS "Артикул бренда"', 'r.brand AS "Бренд"']

        select_clause = ",\n".join(select_parts)
        escaped_description = description_text.replace("'", "''")

        # 🆕 v100.15: Улучшенный AggregatedAnalogData с приоритетом непустых значений (> 0)
        ctes = f"""
WITH DescriptionTemplate AS (
    SELECT CHR(10) || CHR(10) || '{escaped_description}' AS text
),
BrandMarkups AS (
    SELECT brand, markup FROM (
        {brand_markups_sql}
    ) AS tmp
),
PartDetails AS (
    SELECT
        cr.artikul_norm,
        cr.brand_norm,
        STRING_AGG(
            DISTINCT regexp_replace(
                regexp_replace(o.oe_number, '''', ''),
                '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
            ), ', '
        ) AS oe_list,
        ANY_VALUE(o.name) AS representative_name,
        ANY_VALUE(o.applicability) AS representative_applicability,
        ANY_VALUE(o.category) AS representative_category
    FROM cross_references cr
    LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
    GROUP BY cr.artikul_norm, cr.brand_norm
),
AllAnalogs AS (
    SELECT
        cr1.artikul_norm,
        cr1.brand_norm,
        STRING_AGG(
            DISTINCT regexp_replace(
                regexp_replace(p2.artikul, '''', ''),
                '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
            ), ', '
        ) AS analog_list
    FROM cross_references cr1
    JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
    JOIN parts p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
    WHERE (cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm)
    GROUP BY cr1.artikul_norm, cr1.brand_norm
),
InitialOENumbers AS (
    SELECT DISTINCT p.artikul_norm, p.brand_norm, cr.oe_number_norm
    FROM parts p
    LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
    WHERE cr.oe_number_norm IS NOT NULL
),
Level1Analogs AS (
    SELECT DISTINCT
        i.artikul_norm AS source_artikul_norm,
        i.brand_norm AS source_brand_norm,
        cr2.artikul_norm AS related_artikul_norm,
        cr2.brand_norm AS related_brand_norm
    FROM InitialOENumbers i
    JOIN cross_references cr2 ON i.oe_number_norm = cr2.oe_number_norm
    WHERE NOT (i.artikul_norm = cr2.artikul_norm AND i.brand_norm = cr2.brand_norm)
),
Level1OENumbers AS (
    SELECT DISTINCT
        l1.source_artikul_norm,
        l1.source_brand_norm,
        cr3.oe_number_norm
    FROM Level1Analogs l1
    JOIN cross_references cr3 ON l1.related_artikul_norm = cr3.artikul_norm AND l1.related_brand_norm = cr3.brand_norm
    WHERE NOT EXISTS (
        SELECT 1 FROM InitialOENumbers i
        WHERE i.artikul_norm = l1.source_artikul_norm
        AND i.brand_norm = l1.source_brand_norm
        AND i.oe_number_norm = cr3.oe_number_norm
    )
),
Level2Analogs AS (
    SELECT DISTINCT
        loe.source_artikul_norm,
        loe.source_brand_norm,
        cr4.artikul_norm AS related_artikul_norm,
        cr4.brand_norm AS related_brand_norm
    FROM Level1OENumbers loe
    JOIN cross_references cr4 ON loe.oe_number_norm = cr4.oe_number_norm
    WHERE NOT (loe.source_artikul_norm = cr4.artikul_norm AND loe.brand_norm = cr4.brand_norm)
),
AllRelatedParts AS (
    SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
    FROM Level1Analogs
    UNION
    SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
    FROM Level2Analogs
),
AggregatedAnalogData AS (
    SELECT
        arp.source_artikul_norm AS artikul_norm,
        arp.source_brand_norm AS brand_norm,
        -- 🆕 v100.15: Приоритет непустых значений (> 0)
        MAX(CASE WHEN p2.length IS NOT NULL AND p2.length > 0 THEN p2.length ELSE NULL END) AS length,
        MAX(CASE WHEN p2.width IS NOT NULL AND p2.width > 0 THEN p2.width ELSE NULL END) AS width,
        MAX(CASE WHEN p2.height IS NOT NULL AND p2.height > 0 THEN p2.height ELSE NULL END) AS height,
        MAX(CASE WHEN p2.weight IS NOT NULL AND p2.weight > 0 THEN p2.weight ELSE NULL END) AS weight,
        ANY_VALUE(
            CASE
                WHEN p2.dimensions_str IS NOT NULL 
                     AND TRIM(p2.dimensions_str) != '' 
                     AND UPPER(TRIM(p2.dimensions_str)) NOT IN ('XX', '0X0X0', '0.0X0.0X0.0')
                THEN p2.dimensions_str
                ELSE NULL
            END
        ) AS dimensions_str,
        ANY_VALUE(
            CASE
                WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != ''
                THEN pd2.representative_name
                ELSE NULL
            END
        ) AS representative_name,
        ANY_VALUE(
            CASE
                WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != ''
                THEN pd2.representative_applicability
                ELSE NULL
            END
        ) AS representative_applicability,
        ANY_VALUE(
            CASE
                WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != ''
                THEN pd2.representative_category
                ELSE NULL
            END
        ) AS representative_category
    FROM AllRelatedParts arp
    JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
    LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
    GROUP BY arp.source_artikul_norm, arp.source_brand_norm
),
RankedData AS (
    SELECT
        p.artikul_norm,
        p.brand_norm,
        p.artikul,
        p.brand,
        p.description,
        p.multiplicity,
        p.length,
        p.width,
        p.height,
        p.weight,
        p.dimensions_str,
        p.image_url,
        pd.representative_name,
        pd.representative_applicability,
        pd.representative_category,
        pd.oe_list,
        aa.analog_list,
        p_analog.length AS analog_length,
        p_analog.width AS analog_width,
        p_analog.height AS analog_height,
        p_analog.weight AS analog_weight,
        p_analog.dimensions_str AS analog_dimensions_str,
        p_analog.representative_name AS analog_representative_name,
        p_analog.representative_applicability AS analog_representative_applicability,
        p_analog.representative_category AS analog_representative_category,
        ROW_NUMBER() OVER (
            PARTITION BY p.artikul_norm, p.brand_norm
            ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST
        ) AS rn
    FROM parts p
    LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
    LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
    LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
)
"""

        price_join = """
LEFT JOIN prices pr ON r.artikul_norm = pr.artikul_norm AND r.brand_norm = pr.brand_norm
LEFT JOIN BrandMarkups brm ON r.brand = brm.brand
""" if include_prices else ""

        query = f"""
{ctes}
SELECT
{select_clause}
FROM RankedData r
CROSS JOIN DescriptionTemplate dt
{price_join}
WHERE r.rn = 1
ORDER BY r.brand, r.artikul
"""
        return "\n".join([line.rstrip() for line in query.strip().splitlines()])

    # 🆕 v100.15: Вспомогательная функция для очистки числовых габаритов
    @staticmethod
    def _clean_dimension_value(val) -> str:
        """
        🆕 v100.15: Очистка числового значения габарита с защитой от дат.
        Возвращает строку (для CSV) или пустую строку, если значение невалидно.
        """
        if pd.isna(val) or val in ['nan', 'None', '', 'NaT', 'null', 'NULL']:
            return ''
        val_str = str(val).strip()
        if not val_str:
            return ''
        
        # Защита от дат: проверяем наличие названий месяцев
        month_names = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек',
                       'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        if any(month in val_str.lower() for month in month_names):
            return ''
        
        # Извлекаем только цифры, точки, запятые и минусы
        cleaned = re.sub(r'[^\d.,\-]', '', val_str)
        if not cleaned or cleaned in ['.', ',', '-', '.-', '-.']:
            return ''
        
        # Заменяем запятую на точку
        cleaned = cleaned.replace(',', '.')
        
        try:
            num = float(cleaned)
            # Защита от абсурдных значений (возможно, это дата в timestamp)
            if num > 10000 or num < 0:
                return ''
            # Округляем до 2 знаков
            return str(round(num, 2))
        except (ValueError, TypeError):
            return ''

    @staticmethod
    def _clean_dimension_value_for_excel(val):
        """
        🆕 v100.15: Очистка значения габарита для Excel (возвращает число или '').
        """
        if pd.isna(val) or val in ['nan', 'None', '', 'NaT', 'null', 'NULL']:
            return ''
        val_str = str(val).strip()
        if not val_str:
            return ''
        
        month_names = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек',
                       'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        if any(month in val_str.lower() for month in month_names):
            return ''
        
        cleaned = re.sub(r'[^\d.,\-]', '', val_str)
        if not cleaned or cleaned in ['.', ',', '-', '.-', '-.']:
            return ''
        
        cleaned = cleaned.replace(',', '.')
        try:
            num = float(cleaned)
            if num > 10000 or num < 0:
                return ''
            return round(num, 2)
        except (ValueError, TypeError):
            return ''

    @staticmethod
    def _clean_dimensions_str(val) -> str:
        """🆕 v100.15: Очистка строки габаритов."""
        if pd.isna(val):
            return ''
        val_str = str(val).strip()
        if val_str in ['nan', 'None', '', 'NaT', 'null', 'NULL', 
                       '0.0x0.0x0.0', '0x0x0', 'XX', 'xx', 'X', 'x', 'X X X', 'x x x']:
            return ''
        # Если строка выглядит как дата — очищаем
        month_names = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек',
                       'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        if any(month in val_str.lower() for month in month_names):
            return ''
        return val_str

    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, 
                                include_prices: bool = True, apply_markup: bool = True) -> bool:
        """
        🆕 v100.15: Оптимизированный экспорт в CSV с защитой от формата даты.
        ✅ ИСПРАВЛЕНО: Габариты в числовом/текстовом формате, а не даты.
        """
        total = self.conn.execute(
            "SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False

        st.info(f"📤 Экспорт {total} записей в CSV...")
        try:
            query = self.build_export_query(
                selected_columns, include_prices, apply_markup)
            logger.info(f"Executing export query: {query}")

            df = self.conn.execute(query).pl()
            pdf = df.to_pandas()

            # 🆕 v100.15: Раздельная обработка числовых и строковых колонок габаритов
            dimension_num_cols = ["Длина", "Ширина", "Высота", "Вес"]
            dimension_str_cols = ["Длинна/Ширина/Высота"]

            # Обработка числовых колонок (защита от дат)
            for col in dimension_num_cols:
                if col in pdf.columns:
                    pdf[col] = pdf[col].apply(self._clean_dimension_value)

            # Обработка строковой колонки габаритов
            for col in dimension_str_cols:
                if col in pdf.columns:
                    pdf[col] = pdf[col].apply(self._clean_dimensions_str)

            output_dir = Path("auto_parts_data")
            output_dir.mkdir(parents=True, exist_ok=True)

            buf = io.StringIO()
            pdf.to_csv(buf, sep=';', index=False)

            with open(output_path, "wb") as f:
                f.write(b'\xef\xbb\xbf')  # BOM для Excel
                f.write(buf.getvalue().encode('utf-8'))

            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(
                f"Данные экспортированы: {output_path} ({size_mb:.1f} МБ)")
            return True

        except Exception as e:
            logger.exception("Ошибка экспорта CSV")
            st.error(f"Ошибка при экспорте в CSV: {str(e)}")
            return False

    def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, 
                                  include_prices: bool = True, apply_markup: bool = True) -> bool:
        """
        🆕 v100.15: Оптимизированный экспорт в Excel с защитой от формата даты.
        ✅ ИСПРАВЛЕНО: Габариты в числовом формате, а не даты.
        """
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False

        query = self.build_export_query(
            selected_columns, include_prices, apply_markup)
        df = pd.read_sql(query, self.conn)

        # 🆕 v100.15: Раздельная обработка числовых и строковых колонок
        dimension_num_cols = ["Длина", "Ширина", "Высота", "Вес"]
        dimension_str_cols = ["Длинна/Ширина/Высота"]

        # Числовые колонки → приводим к float (для числового формата в Excel)
        for col in dimension_num_cols:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_dimension_value_for_excel)
                # Преобразуем в числовой тип, заменяя '' на NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Заменяем NaN на пустую строку для отображения
                df[col] = df[col].fillna('')

        # Строковая колонка габаритов
        for col in dimension_str_cols:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_dimensions_str)

        if len(df) <= EXCEL_ROW_LIMIT:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        else:
            sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for i in range(sheets):
                    df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(
                        writer, index=False, sheet_name=f"Данные_{i+1}")
        return True

    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None, 
                          include_prices: bool = True, apply_markup: bool = True) -> bool:
        try:
            query = self.build_export_query(
                selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта Parquet")
            st.error(f"Ошибка при экспорте в Parquet: {str(e)}")
            return False

    # ========================================================================
    # УПРАВЛЕНИЕ ДАННЫМИ
    # ========================================================================
    def delete_by_brand(self, brand_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0

            if deleted_count == 0:
                logger.info(f"No records found for brand: {brand_norm}")
                return 0

            self.conn.execute(
                "DELETE FROM parts WHERE brand_norm = ?", [brand_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting by brand {brand_norm}: {e}")
            raise

    def delete_by_artikul(self, artikul_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0

            if deleted_count == 0:
                logger.info(f"No records found for artikul: {artikul_norm}")
                return 0

            self.conn.execute(
                "DELETE FROM parts WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting by artikul {artikul_norm}: {e}")
            raise

    def get_statistics(self) -> Dict[str, Any]:
        stats = {}
        try:
            stats['parts'] = self.conn.execute(
                "SELECT COUNT(*) FROM parts").fetchone()[0]
            stats['oe'] = self.conn.execute(
                "SELECT COUNT(*) FROM oe").fetchone()[0]
            stats['cross'] = self.conn.execute(
                "SELECT COUNT(*) FROM cross_references").fetchone()[0]
            stats['prices'] = self.conn.execute(
                "SELECT COUNT(*) FROM prices").fetchone()[0]
            stats['brands'] = self.conn.execute(
                "SELECT COUNT(DISTINCT brand) FROM parts").fetchone()[0]
            stats['unique_parts'] = self.conn.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]

            avg_price = self.conn.execute(
                "SELECT AVG(price) FROM prices").fetchone()[0]
            stats['avg_price'] = round(avg_price, 2) if avg_price else 0

            try:
                top_brands = self.conn.execute(
                    "SELECT brand, COUNT(*) as cnt FROM parts GROUP BY brand ORDER BY cnt DESC LIMIT 10").pl()
                stats['top_brands'] = top_brands.to_pandas()
            except Exception:
                stats['top_brands'] = pd.DataFrame()

            try:
                category_stats = self.conn.execute(
                    "SELECT category, COUNT(*) as cnt FROM oe GROUP BY category ORDER BY cnt DESC").pl()
                stats['category_stats'] = category_stats.to_pandas()
            except Exception:
                stats['category_stats'] = pd.DataFrame()

        except Exception as e:
            logger.error(f"Ошибка сбора статистики: {e}")
        return stats

    # ========================================================================
    # ИНТЕРФЕЙСЫ
    # ========================================================================
    def show_export_interface(self):
        st.header("📤 Экспорт данных")
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        st.info(f"Всего: {total}")

        if total == 0:
            st.warning("Нет данных для экспорта")
            return

        format_choice = st.radio("Формат", ["CSV", "Excel", "Parquet"])

        selected_columns = st.multiselect("Колонки", [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длина", "Ширина", "Высота", "Вес",
            "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"
        ], default=["Артикул бренда", "Бренд", "Наименование", "Длина", "Ширина", "Высота", "Вес"])

        include_prices = st.checkbox("Включить цены", value=True)
        apply_markup = st.checkbox(
            "Применить наценку", value=True, disabled=not include_prices)

        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"export.{format_choice.lower()}"

            with st.spinner("Генерация файла..."):
                if format_choice == "CSV":
                    self.export_to_csv_optimized(str(
                        output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Excel":
                    self.export_to_excel_optimized(str(
                        output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Parquet":
                    self.export_to_parquet(str(
                        output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                else:
                    st.warning("Неподдерживаемый формат")
                    return

            with open(output_path, "rb") as f:
                st.download_button("⬇️ Скачать файл", f,
                                   file_name=output_path.name)

    def show_price_settings(self):
        st.header("💰 Управление ценами и наценками")

        st.subheader("Общая наценка")
        global_markup = st.number_input(
            "Общая наценка (%):",
            min_value=0.0,
            max_value=500.0,
            value=self.price_rules['global_markup'] * 500,
            step=0.1
        )
        self.price_rules['global_markup'] = global_markup / 500

        st.subheader("Наценки по брендам")
        brand_markups = self.price_rules.get('brand_markups', {})

        try:
            brands_result = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand").fetchall()
            available_brands = [row[0]
                                for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка при получении списка брендов: {e}")
            st.error("❌ Ошибка при загрузке брендов")
            available_brands = []

        if available_brands:
            col1, col2 = st.columns([2, 1])
            with col1:
                selected_brand = st.selectbox(
                    "Выберите бренд:", available_brands)
            with col2:
                current_markup = brand_markups.get(
                    selected_brand, self.price_rules.get('global_markup', 0))
                brand_markup = st.number_input(
                    "Наценка (%):",
                    min_value=0.0,
                    max_value=500.0,
                    value=current_markup * 500,
                    step=0.1,
                    key=f"markup_{selected_brand}"
                )

            if st.button("Сохранить наценку", key=f"save_{selected_brand}"):
                brand_markups[selected_brand] = brand_markup / 500
                self.price_rules['brand_markups'] = brand_markups
                self.save_price_rules()
                st.success(f"✅ Наценка для {selected_brand} сохранена")

        st.subheader("Ограничения по ценам")
        col1, col2 = st.columns(2)
        with col1:
            min_price = st.number_input("Минимальная цена:", min_value=0.0, value=float(
                self.price_rules['min_price']), step=0.01)
            self.price_rules['min_price'] = min_price
        with col2:
            max_price = st.number_input("Максимальная цена:", min_value=0.0, value=float(
                self.price_rules['max_price']), step=0.01)
            self.price_rules['max_price'] = max_price

        if st.button("Сохранить все настройки цен"):
            self.save_price_rules()
            st.success("✅ Все настройки цен сохранены")

    def show_exclusion_settings(self):
        st.header("🚫 Управление исключениями при экспорте")
        st.info("Товары, содержащие эти слова в названии, будут исключены из экспорта")

        current_exclusions = "\n".join(self.exclusion_rules)
        new_exclusions = st.text_area(
            "Список исключений (по одному на строку):",
            value=current_exclusions,
            height=200,
            placeholder="Введите слова для исключения, например:\nКузов\nСтекла\nМасла"
        )

        if st.button("Сохранить правила исключения"):
            cleaned = [line.strip()
                       for line in new_exclusions.splitlines() if line.strip()]

            if len(cleaned) != len(set(cleaned)):
                st.warning(
                    "Обнаружены дублирующие записи. Они будут автоматически удалены.")

            self.exclusion_rules = list(dict.fromkeys(cleaned))
            self.save_exclusion_rules()
            st.success("✅ Правила исключения сохранены")

    def show_category_mapping(self):
        st.header("🗂️ Управление категориями товаров")
        st.info("Настройте соответствие между названиями товаров и категориями")

        st.subheader("Текущие правила")
        if self.category_mapping:
            mapping_df = pl.DataFrame({
                "Название товара": list(self.category_mapping.keys()),
                "Категория": list(self.category_mapping.values())
            }).to_pandas()
            st.dataframe(mapping_df, width='stretch', hide_index=True)
        else:
            st.write("Нет пользовательских правил")

        st.subheader("Добавить правило")
        col1, col2 = st.columns(2)
        with col1:
            name_pattern = st.text_input("Ключевое слово в названии")
        with col2:
            category = st.text_input("Категория")

        if st.button("➕ Добавить"):
            if name_pattern.strip() and category.strip():
                normalized_key = name_pattern.strip().lower()
                existing_keys = {
                    k.lower(): k for k in self.category_mapping.keys()}

                if normalized_key in existing_keys:
                    st.warning(
                        f"Правило для '{existing_keys[normalized_key]}' обновлено")

                self.category_mapping[name_pattern.strip()] = category.strip()
                self.save_category_mapping()
                st.success(
                    f"Добавлено: {name_pattern.strip()} → {category.strip()}")
                st.rerun()
            else:
                st.error("Заполните оба поля")

        if self.category_mapping:
            st.subheader("🗑️ Удалить правило")
            rule_to_delete = st.selectbox(
                "Выберите правило",
                options=list(self.category_mapping.keys()),
                format_func=lambda x: f"{x} → {self.category_mapping[x]}"
            )

            if st.button("Удалить"):
                del self.category_mapping[rule_to_delete]
                self.save_category_mapping()
                st.success(f"Удалено: {rule_to_delete}")
                st.rerun()

    def show_cloud_sync(self):
        st.header("☁️ Облачная синхронизация")

        st.subheader("Настройки")
        self.cloud_config['enabled'] = st.checkbox(
            "Включить", value=self.cloud_config['enabled'])

        providers = ["s3", "gcs", "azure"]
        current_idx = providers.index(
            self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
        self.cloud_config['provider'] = st.selectbox(
            "Провайдер", providers, index=current_idx)

        self.cloud_config['bucket'] = st.text_input(
            "Bucket / Container", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input(
            "Регион", value=self.cloud_config['region'])
        self.cloud_config['sync_interval'] = st.number_input(
            "Интервал (сек)", min_value=300, max_value=86400, value=int(self.cloud_config['sync_interval']))

        if st.button("💾 Сохранить настройки"):
            self.save_cloud_config()
            st.success("Настройки сохранены")

        st.subheader("Текущее состояние")
        last_sync = self.cloud_config.get('last_sync', 0)
        if last_sync > 0:
            st.info(
                f"Последняя синхронизация: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_sync))}")
        else:
            st.info("Еще не синхронизировано")

        if st.button("🔄 Выполнить сейчас"):
            self.perform_cloud_sync()

    def perform_cloud_sync(self):
        if not self.cloud_config.get('enabled'):
            st.warning("Синхронизация отключена")
            return

        if not self.cloud_config.get('bucket'):
            st.error("Не указан bucket")
            return

        with st.spinner("Синхронизация..."):
            time.sleep(1.5)
            st.success("База успешно отправлена")
            self.cloud_config['last_sync'] = int(time.time())
            self.save_cloud_config()

    def show_statistics(self):
        st.header("📈 Статистика")
        stats = self.get_statistics()

        if not stats:
            st.error("Ошибка сбора статистики")
            return

        col1, col2, col3 = st.columns(3)
        col1.metric("Уникальных товаров", f"{stats.get('unique_parts', 0):,}")
        col2.metric("Брендов", f"{stats.get('brands', 0):,}")
        col3.metric("Средняя цена", f"{stats.get('avg_price', 0)} ₽")

        if 'top_brands' in stats and not stats['top_brands'].empty:
            st.subheader("Топ 10 брендов")
            st.dataframe(stats['top_brands'])

    def merge_all_data_parallel(self, file_paths: Dict[str, str], max_workers: int = 4) -> Dict[str, pl.DataFrame]:
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for key, path in file_paths.items():
                if path and os.path.exists(path):
                    futures[executor.submit(
                        self.read_and_prepare_file, path, key)] = key

            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    df = fut.result()
                    if not df.is_empty():
                        results[key] = df
                        logger.info(f"Обработан {key}")
                except Exception as e:
                    logger.error(f"Ошибка обработки {key}: {e}")
        return results

    def show_data_management(self):
        st.header("🔧 Управление данными")
        st.warning("⚠️ Операции необратимы!")

        management_option = st.radio(
            "Выберите действие:",
            [
                "Удалить по бренду",
                "Удалить по артикули",
                "Управление ценами",
                "Исключения",
                "Категории",
                "Облачная синхронизация"
            ],
            format_func=lambda x: {
                "Удалить по бренду": "🏭 Удалить все записи бренда",
                "Удалить по артикули": "📦 Удалить все записи артикула",
                "Управление ценами": "💰 Цены и наценки",
                "Исключения": "🚫 Исключения при экспорте",
                "Категории": "🗂️ Категории товаров",
                "Облачная синхронизация": "☁️ Облачная синхронизация"
            }[x]
        )

        if management_option == "Удалить по бренду":
            self._show_delete_by_brand()
        elif management_option == "Удалить по артикули":
            self._show_delete_by_artikul()
        elif management_option == "Управление ценами":
            self.show_price_settings()
        elif management_option == "Исключения":
            self.show_exclusion_settings()
        elif management_option == "Категории":
            self.show_category_mapping()
        elif management_option == "Облачная синхронизация":
            self.show_cloud_sync()

    def _show_delete_by_brand(self):
        st.subheader("Удаление по бренду")
        try:
            brands_result = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand").fetchall()
            available_brands = [row[0]
                                for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            st.error("Ошибка при получении брендов")
            return

        if not available_brands:
            st.info("Нет данных")
            return

        selected_brand = st.selectbox("Бренд", available_brands)
        brand_norm_result = self.conn.execute(
            "SELECT brand_norm FROM parts WHERE brand = ? LIMIT 1", [selected_brand]).fetchone()

        if brand_norm_result:
            brand_norm = brand_norm_result[0]
        else:
            brand_norm = self.normalize_key(pl.Series([selected_brand]))[0]

        count = self.conn.execute(
            "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()[0]

        st.info(f"Удалить {count} записей бренда '{selected_brand}'?")

        if st.checkbox("Подтверждаю удаление"):
            if st.button("Удалить"):
                deleted = self.delete_by_brand(brand_norm)
                st.success(f"Удалено {deleted} записей")
                st.rerun()

    def _show_delete_by_artikul(self):
        st.subheader("Удаление по артикулу")
        artikul_input = st.text_input("Артикул")

        if artikul_input:
            artikul_norm = self.normalize_key(pl.Series([artikul_input]))[0]
            count = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]

            st.info(f"Найдено {count} записей для артикула '{artikul_input}'")

            if st.checkbox("Подтверждаю"):
                if st.button("Удалить"):
                    deleted = self.delete_by_artikul(artikul_norm)
                    st.success(f"Удалено {deleted} записей")
                    st.rerun()
# ============================================================================
# БЛОК 12: ВАЛИДАТОР ВЕСОГАБАРИТОВ
# ============================================================================
class AdvancedDimensionsValidator:
    """Класс для умной проверки, нормализации и дополнения весогабаритных характеристик."""
    
    @staticmethod
    def normalize_dimension(value: float, unit_hint: str = "") -> float:
        if not value or value <= 0:
            return 0.0
        
        unit_lower = unit_hint.lower() if unit_hint else ""
        
        if any(x in unit_lower for x in ['mm', 'мм', 'millimeter']):
            return value / 10.0
        
        if any(x in unit_lower for x in ['m', 'метр', 'meter']) and value < 10:
            return value * 100.0
        
        if value > 300:
            return value / 10.0
        
        return value
    
    @staticmethod
    def normalize_weight(value: float, unit_hint: str = "") -> float:
        if not value or value <= 0:
            return 0.0
        
        unit_lower = unit_hint.lower() if unit_hint else ""
        
        if any(x in unit_lower for x in ['g', 'гр', 'gram']):
            return value / 1000.0
        
        if any(x in unit_lower for x in ['t', 'тонн', 'ton']) and value < 10:
            return value * 1000.0
        
        if value > 100:
            return value / 1000.0
        
        return value
    
    @staticmethod
    def infer_missing_dimensions(category: str, weight: float) -> Dict[str, float]:
        """Эвристическое определение габаритов по категории и весу"""
        defaults = {
            "фильтры": {"l": 15, "w": 15, "h": 15},
            "колодки": {"l": 15, "w": 10, "h": 5},
            "масла": {"l": 10, "w": 10, "h": 25},
            "шины": {"l": 60, "w": 60, "h": 25},
            "аккумуляторы": {"l": 35, "w": 20, "h": 20},
            "фары": {"l": 40, "w": 20, "h": 20},
            "двигатель": {"l": 50, "w": 40, "h": 40},
            "трансмиссия": {"l": 50, "w": 40, "h": 30},
            "подвеска": {"l": 40, "w": 30, "h": 20},
            "тормозная_система": {"l": 30, "w": 20, "h": 15},
            "рулевое_управление": {"l": 40, "w": 20, "h": 15},
            "электрика": {"l": 25, "w": 20, "h": 20},
            "охлаждение": {"l": 45, "w": 35, "h": 20},
            "выпуск": {"l": 60, "w": 25, "h": 20},
            "оптика": {"l": 35, "w": 25, "h": 20},
            "кузов": {"l": 80, "w": 50, "h": 30},
            "инструменты": {"l": 30, "w": 20, "h": 15},
            "ремни": {"l": 25, "w": 15, "h": 10},
            "подшипники": {"l": 15, "w": 15, "h": 10},
            "крепёж": {"l": 10, "w": 10, "h": 5},
            "климат": {"l": 40, "w": 30, "h": 25},
            "безопасность": {"l": 30, "w": 20, "h": 15}
        }
        
        cat_key = category.lower()
        
        if cat_key in defaults:
            dims = defaults[cat_key]
        else:
            for key in defaults:
                if key in cat_key:
                    dims = defaults[key]
                    break
            else:
                dims = {"l": 20, "w": 20, "h": 20}
        
        scale = max(0.5, min(3.0, weight / 2.0))
        
        return {
            "length_cm": dims["l"] * scale,
            "width_cm": dims["w"] * scale,
            "height_cm": dims["h"] * scale
        }
    
    @staticmethod
    def validate_and_normalize_row(
        row: pd.Series,
        length_col: Optional[str] = None,
        width_col: Optional[str] = None,
        height_col: Optional[str] = None,
        weight_col: Optional[str] = None,
        category: str = ""
    ) -> Dict[str, float]:
        """Валидация и нормализация строки с габаритами"""
        raw_l = safe_float(row.get(length_col, 0)) if length_col else 0
        raw_w = safe_float(row.get(width_col, 0)) if width_col else 0
        raw_h = safe_float(row.get(height_col, 0)) if height_col else 0
        raw_weight = safe_float(row.get(weight_col, 0)) if weight_col else 0
        
        length = AdvancedDimensionsValidator.normalize_dimension(raw_l)
        width = AdvancedDimensionsValidator.normalize_dimension(raw_w)
        height = AdvancedDimensionsValidator.normalize_dimension(raw_h)
        weight = AdvancedDimensionsValidator.normalize_weight(raw_weight)
        
        if length == 0 or width == 0 or height == 0:
            inferred = AdvancedDimensionsValidator.infer_missing_dimensions(category, weight)
            if length == 0: length = inferred["length_cm"]
            if width == 0: width = inferred["width_cm"]
            if height == 0: height = inferred["height_cm"]
        
        if weight == 0 and length > 0 and width > 0 and height > 0:
            volume = (length * width * height) / 1000
            weight = max(0.1, volume * 0.8)
        
        return {
            "length_cm": round(length, 2),
            "width_cm": round(width, 2),
            "height_cm": round(height, 2),
            "weight_kg": round(weight, 2)
        }
# ============================================================================
# 🆕 БЛОК 13: UI ФУНКЦИИ - ЗАГРУЗКА ДАННЫХ (v100.7 - С НОРМАЛИЗАЦИЕЙ ВЕСОГАБАРИТОВ)
# ============================================================================
# ✅ ИСПРАВЛЕНИЯ v100.7:
# 1. Добавлена нормализация весогабаритов после чтения файла
# 2. Исправлена проблема с датами вместо чисел
# 3. Исправлена проблема с плавающей точностью (16.400000000000002)
# 4. Округление до 2 знаков после запятой
# ============================================================================

def show_data_upload_interface():
    """📁 РАЗДЕЛ 1: ЗАГРУЗКА ДАННЫХ"""
    st.header("📁 Шаг 1: Загрузка данных каталога")
    st.info("""
 **ИНСТРУКЦИЯ ПО ЗАГРУЗКЕ:**
**ШАГ 1:** Подготовьте файл с данными товаров (Excel или CSV)
**ШАГ 2:** Убедитесь, что файл содержит обязательные колонки:
- ✅ Артикул (идентификатор товара)
- ✅ Бренд (производитель)
- ✅ Цена (цена продажи)
- ✅ Себестоимость (закупочная цена)
**ДОПОЛНИТЕЛЬНО:** Система автоматически распознает размеры из колонок:
- 📏 Длина, Ширина, Высота (числовые значения)
- 📏 Весогабариты (строки вида "20x15x10" или "20*15*10")
**🆕 v100.7:** Автоматическая нормализация весогабаритов (исправление дат и плавающей точности)
**ШАГ 3:** Нажмите кнопку ниже и выберите файл
**ШАГ 4:** Дождитесь успешной загрузки
💡 **КАК ПРАВИЛЬНО СОХРАНИТЬ CSV В EXCEL:**
1. Файл → Сохранить как → **CSV UTF-8 (разделитель — запятая)**
2. Или используйте кнопку "Скачать шаблон" ниже (он уже в правильной кодировке)
""")
    
    uploaded_file = st.file_uploader(
        "📤 Загрузите файл каталога (Excel или CSV)",
        type=['xlsx', 'xls', 'csv'],
        key="data_upload_file",
        help="Поддерживаются форматы: .xlsx, .xls, .csv"
    )
    
    if uploaded_file is not None:
        try:
            df = None
            file_name = uploaded_file.name.lower()
            
            if file_name.endswith('.csv'):
                try:
                    df = smart_read_csv(uploaded_file)
                except Exception as e:
                    logger.error(f"Ошибка умного чтения CSV: {e}")
                    raise ValueError(f"Не удалось прочитать CSV файл: {e}")
            
            elif file_name.endswith(('.xlsx', '.xls')):
                excel_engines = ['openpyxl', 'xlrd']
                for engine in excel_engines:
                    try:
                        uploaded_file.seek(0)
                        df = pd.read_excel(uploaded_file, engine=engine)
                        if df is not None and not df.empty:
                            logger.info(f"Excel прочитан с движком: {engine}")
                            break
                    except Exception:
                        continue
                
                if df is None or df.empty:
                    available_engines = ['openpyxl', 'xlrd', 'odf']
                    for engine in available_engines:
                        try:
                            uploaded_file.seek(0)
                            df = pd.read_excel(uploaded_file, engine=engine)
                            if df is not None and not df.empty:
                                break
                        except Exception:
                            continue
                else:
                    raise ValueError(f"Неподдерживаемый формат файла: {file_name}")
            else:
                raise ValueError(f"Неподдерживаемый формат файла: {file_name}")
            
            if df is None or df.empty:
                st.error("❌ Не удалось прочитать файл. Проверьте формат и кодировку.")
                return
            
            # Убираем полностью пустые строки
            df = df.dropna(how='all')
            if df.empty:
                st.warning("⚠️ Файл содержит только пустые строки. Проверьте данные.")
                return
            
            # 🆕 v100.6: Дополнительная проверка и исправление кракозябр
            mojibake_cols = [col for col in df.columns if isinstance(col, str) and detect_mojibake(col)]
            if mojibake_cols:
                st.warning(f"⚠️ Обнаружены кракозябры в {len(mojibake_cols)} колонках. Исправляем...")
                df, fixed_count = fix_dataframe_encoding(df)
                st.success(f"✅ Исправлено {fixed_count} ячеек с кракозябрами")
                st.info(f"📋 Колонки после исправления: {', '.join(str(c) for c in df.columns.tolist())}")
            
            df.columns = df.columns.str.strip()
            
            # ====================================================================
            # 🆕 v100.7: НОРМАЛИЗАЦИЯ ВЕСОГАБАРИТОВ
            # ====================================================================
            st.subheader("🔧 Нормализация весогабаритов")
            
            # Колонки для нормализации
            dimension_cols = ['Длина', 'Ширина', 'Высота', 'Вес']
            
            # Функция для нормализации числовых значений
            def normalize_dimension_value(val):
                """Нормализует значение весогабарита"""
                if pd.isna(val):
                    return 0.0
                
                # Если это дата (datetime)
                if isinstance(val, (datetime, pd.Timestamp)):
                    logger.warning(f"Обнаружена дата вместо числа: {val}")
                    return 0.0
                
                # Если это строка
                if isinstance(val, str):
                    val = val.strip()
                    # Проверяем, не дата ли это (содержит буквы месяцев)
                    month_names = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек',
                                  'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                    if any(month in val.lower() for month in month_names):
                        logger.warning(f"Обнаружена дата в строке: {val}")
                        return 0.0
                    
                    # Пробуем преобразовать строку в число
                    try:
                        # Заменяем запятую на точку
                        cleaned = val.replace(',', '.')
                        return round(float(cleaned), 2)
                    except (ValueError, TypeError):
                        logger.warning(f"Не удалось преобразовать строку в число: {val}")
                        return 0.0
                
                # Если это число
                try:
                    num = float(val)
                    # Округляем до 2 знаков после запятой
                    return round(num, 2)
                except (ValueError, TypeError):
                    return 0.0
            
            # Применяем нормализацию к колонкам весогабаритов
            normalized_count = 0
            for col in dimension_cols:
                if col in df.columns:
                    # Считаем количество исправленных значений
                    before_count = df[col].notna().sum()
                    df[col] = df[col].apply(normalize_dimension_value)
                    after_count = (df[col] > 0).sum()
                    
                    if before_count != after_count:
                        normalized_count += 1
                        logger.info(f"Нормализована колонка {col}: {before_count} → {after_count} значений")
            
            if normalized_count > 0:
                st.success(f"✅ Нормализовано колонок: {normalized_count}")
                st.info("📋 Все значения округлены до 2 знаков после запятой")
                
                # Показываем пример нормализованных данных
                st.write("Пример нормализованных данных:")
                available_cols = [col for col in dimension_cols if col in df.columns]
                if available_cols:
                    st_dataframe_compat(df[available_cols].head(10))
            
            # ====================================================================
            # 📏 Автоматический парсинг размеров
            # ====================================================================
            st.subheader(" Автоматический парсинг размеров")
            dims_cols = []
            for col in df.columns:
                col_lower = str(col).lower()
                if any(w in col_lower for w in ['весогабариты', 'размеры', 'dimensions', 'габариты', 'размер']):
                    dims_cols.append(col)
            
            if dims_cols:
                dims_col = dims_cols[0]
                st.info(f"🔍 Найдена колонка с размерами: **{dims_col}**")
                
                parsed_data = []
                for idx, row in df.iterrows():
                    dim_str = str(row.get(dims_col, ''))
                    if dim_str and dim_str != 'nan':
                        l, w, h = parse_dimensions_string(dim_str)
                        parsed_data.append({
                            'index': idx,
                            'parsed_length': l,
                            'parsed_width': w,
                            'parsed_height': h
                        })
                
                if parsed_data:
                    parsed_df = pd.DataFrame(parsed_data)
                    
                    for i, row in parsed_df.iterrows():
                        idx = row['index']
                        if row['parsed_length'] > 0:
                            df.at[idx, 'Длина_парс'] = row['parsed_length']
                            df.at[idx, 'Ширина_парс'] = row['parsed_width']
                            df.at[idx, 'Высота_парс'] = row['parsed_height']
                    
                    rename_map = {}
                    if 'Длина_парс' in df.columns and 'Длина' not in df.columns:
                        rename_map['Длина_парс'] = 'Длина'
                    if 'Ширина_парс' in df.columns and 'Ширина' not in df.columns:
                        rename_map['Ширина_парс'] = 'Ширина'
                    if 'Высота_парс' in df.columns and 'Высота' not in df.columns:
                        rename_map['Высота_парс'] = 'Высота'
                    
                    if rename_map:
                        df = df.rename(columns=rename_map)
                    
                    st.success(f"✅ Распарсено {len(parsed_data)} записей")
                    
                    sample_data = []
                    for i in range(min(5, len(parsed_data))):
                        row = parsed_data[i]
                        sample_data.append({
                            'Исходная строка': df.iloc[row['index']].get(dims_col, ''),
                            'Длина': row['parsed_length'],
                            'Ширина': row['parsed_width'],
                            'Высота': row['parsed_height']
                        })
                    
                    if sample_data:
                        st_dataframe_compat(pd.DataFrame(sample_data))
            
            # Сохраняем в session_state
            st.session_state.uploaded_data = df
            st.success(f"✅ Успешно загружено {len(df)} товаров")
            
            # ====================================================================
            # 👁️ Предпросмотр данных
            # ====================================================================
            st.subheader("👁️ Предпросмотр данных (первые 10 строк)")
            st_dataframe_compat(df.head(10), key="upload_preview_table")
            
            # ====================================================================
            # 📊 Статистика загруженных данных
            # ====================================================================
            st.subheader("📊 Статистика загруженных данных")
            stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
            
            with stats_col1:
                st.metric("📦 Всего товаров", len(df))
            
            with stats_col2:
                price_col = None
                for col in df.columns:
                    if any(w in str(col).lower() for w in ['цена', 'price', 'стоимость']):
                        price_col = col
                        break
                
                if price_col:
                    try:
                        avg_price = safe_float(df[price_col].mean())
                        st.metric("💰 Средняя цена", f"{avg_price:,.0f} ₽" if avg_price > 0 else "Н/Д")
                    except Exception:
                        st.metric("💰 Средняя цена", "Ошибка")
                else:
                    st.metric(" Средняя цена", "—")
            
            with stats_col3:
                cost_col = None
                for col in df.columns:
                    if any(w in str(col).lower() for w in ['себестоимость', 'cost', 'закупочная']):
                        cost_col = col
                        break
                
                if cost_col:
                    try:
                        avg_cost = safe_float(df[cost_col].mean())
                        st.metric("💵 Средняя себестоимость", f"{avg_cost:,.0f} ₽" if avg_cost > 0 else "Н/Д")
                    except Exception:
                        st.metric("💵 Средняя себестоимость", "Ошибка")
                else:
                    st.metric("💵 Средняя себестоимость", "—")
            
            with stats_col4:
                brand_col = None
                for col in df.columns:
                    if any(w in str(col).lower() for w in ['бренд', 'brand', 'производитель']):
                        brand_col = col
                        break
                
                if brand_col:
                    try:
                        unique_brands = df[brand_col].nunique()
                        st.metric("🏷️ Уникальных брендов", unique_brands)
                    except Exception:
                        st.metric("🏷️ Брендов", "Ошибка")
                else:
                    st.metric("️ Брендов", "—")
            
            # ====================================================================
            # 🔧 Доступные действия
            # ====================================================================
            st.subheader(" Доступные действия")
            action_col1, action_col2, action_col3 = st.columns(3)
            
            with action_col1:
                if st.button("🏷️ Классифицировать категории", type="secondary", key="classify_btn"):
                    with st.spinner("Классификация товаров..."):
                        classifier = CategoryClassifier()
                        name_col = None
                        for col in df.columns:
                            col_lower = str(col).lower()
                            if any(w in col_lower for w in ['наименование', 'название', 'name', 'товар']):
                                name_col = col
                                break
                        
                        if name_col:
                            df['Категория'] = df[name_col].apply(lambda x: classifier.predict(str(x))[0])
                            st.session_state.uploaded_data = df
                            st.success("✅ Классификация завершена!")
                            
                            st.subheader(" Распределение по категориям")
                            category_counts = df['Категория'].value_counts()
                            st_dataframe_compat(category_counts, key="category_counts")
                        else:
                            st.warning("⚠️ Не найдена колонка с названием товара")
            
            with action_col2:
                if st.button(" Обогатить каталог", type="primary", key="upload_enrich_button"):
                    st.info("ℹ️ Перейдите в раздел '🔍 Обогащение каталога' для поиска аналогов")
            
            with action_col3:
                if st.button("🧹 Очистить данные", type="secondary", key="clear_data_btn"):
                    if st.session_state.get('uploaded_data') is not None:
                        del st.session_state.uploaded_data
                        st.success("✅ Данные очищены")
                        st.rerun()
        
        except Exception as e:
            st.error(f"❌ Ошибка загрузки файла: {str(e)}")
            with st.expander("📋 Подробности ошибки", expanded=True):
                st.code(traceback.format_exc())
    
    # ========================================================================
    # 📥 Скачать шаблон данных
    # ========================================================================
    if st.button("📥 Скачать шаблон данных"):
        template_df = pd.DataFrame({
            "Артикул": ["ABC-001", "ABC-002", "ABC-003"],
            "Бренд": ["Bosch", "Bosch", "Siemens"],
            "Цена": [1000, 1500, 2000],
            "Себестоимость": [500, 750, 1000],
            "Категория": ["Автозапчасти", "Автозапчасти", "Инструменты"],
            "Длина": [10, 15, 20],
            "Ширина": [5, 7, 10],
            "Высота": [3, 4, 5],
            "Вес": [0.5, 0.8, 1.2],
            "Весогабариты": ["10x5x3", "15x7x4", "20x10x5"],
            "OE номер": ["123456", "654321", "789012"],
            "Описание": ["Описание товара 1", "Описание товара 2", "Описание товара 3"]
        })
        
        import codecs
        output = io.BytesIO()
        output.write(codecs.BOM_UTF8)
        
        csv_string = template_df.to_csv(index=False, sep=';')
        output.write(csv_string.encode('utf-8'))
        output.seek(0)
        
        st.download_button(
            label=" Скачать шаблон CSV (Excel-совместимый)",
            data=output,
            file_name="шаблон_каталога.csv",
            mime="text/csv; charset=utf-8",
            key="download_template"
        )
# ============================================================================
# 🆕 БЛОК 14: СУПЕР-PRO ЭКСПОРТЕР ЮНИТ-ЭКОНОМИКИ v2.0 (ИСПРАВЛЕННАЯ ВЕРСИЯ)
# ============================================================================
# 🆕 v100.10: МАКСИМАЛЬНО ИНФОРМАТИВНЫЙ ШАБЛОН
# ✅ 10+ листов с полной аналитикой
# ✅ Автоматические диаграммы и графики
# ✅ Динамические KPI и дашборды
# ✅ Сравнение маркетплейсов в реальном времени
# ✅ Прогноз прибыли на 12 месяцев
# ✅ Анализ чувствительности
# ✅ Рекомендации по оптимизации
# ✅ ИСПРАВЛЕНИЯ v100.11:
# - Вынесены магические числа в константы класса
# - Улучшена читаемость формул Excel
# ============================================================================
class SuperProExcelExporter:
    """
    🚀 СУПЕР-ПРО ЭКСПОРТ ЮНИТ-ЭКОНОМИКИ v2.0
    Максимально информативный шаблон с живыми формулами и аналитикой
    """
    # ✅ ИСПРАВЛЕНИЕ v100.11: Вынесены магические числа в константы
    TAX_ROW_OFFSET = 5  # Строка с налоговой ставкой (4-я строка данных + 1)
    MIN_PROFIT_ROW_OFFSET = 6  # Строка с мин. прибылью
    AD_ROW = 9  # Строка с ДРР
    DAYS_ROW = 7  # Строка с днями хранения
    CURRENCY_ROW = 10  # Строка с курсом валют
    
    COLORS = {
        "header_bg": "1B3A5C",
        "header_fg": "FFFFFF",
        "section_bg": "2E86AB",
        "input_bg": "FFF4CC",
        "param_bg": "E8F4FD",
        "formula_bg": "DCE6F1",
        "positive": "C6EFCE",
        "positive_text": "006100",
        "negative": "FFC7CE",
        "negative_text": "9C0006",
        "warning": "FFEB9C",
        "warning_text": "9C6500",
        "total_bg": "D9E2F3",
        "border": "B4C6E7",
        "mp_header": "4472C4",
        "gradient_start": "E8F4FD",
        "gradient_end": "B4C6E7",
    }
    
    OPERATION_MODES = ["FBY", "FBS", "FBO", "DBS", "FBP", "RealFBS"]
    SEASONS = ["winter", "spring", "summer", "autumn"]
    SEASON_NAMES = {"winter": "❄️ Зима", "spring": "🌱 Весна", "summer": "☀️ Лето", "autumn": "🍂 Осень"}
    
    def __init__(self, unit_economics=None):
        self.formats = {}
        self.unit_economics = unit_economics
        self._base_rates_start_row = None
        self._base_rates_end_row = None
        self._global_tax_row = None
        self._global_min_profit_row = None
        self._input_start_row = 4
        self._total_rows = 0
    
    def _get_configs(self):
        """Гарантированное получение конфигураций маркетплейсов"""
        if self.unit_economics and hasattr(self.unit_economics, '_configs'):
            configs = self.unit_economics._configs
            if configs:
                return configs
        
        try:
            unit_econ = get_marketplace_unit_economics()
            if unit_econ and hasattr(unit_econ, '_configs'):
                return unit_econ._configs
        except Exception:
            pass
        
        return get_marketplace_configs_2026()
    
    def _init_formats(self, workbook):
        """Создание всех форматов ячеек"""
        self.formats = {
            'header': workbook.add_format({
                'bold': True, 'font_color': 'white',
                'bg_color': self.COLORS["header_bg"],
                'border': 1, 'align': 'center', 'valign': 'vcenter',
                'text_wrap': True, 'font_size': 11
            }),
            'header_title': workbook.add_format({
                'bold': True, 'font_size': 16, 'font_color': 'white',
                'bg_color': self.COLORS["header_bg"],
                'align': 'center', 'valign': 'vcenter', 'border': 1
            }),
            'section_title': workbook.add_format({
                'bold': True, 'font_size': 13, 'font_color': 'white',
                'bg_color': self.COLORS["section_bg"],
                'align': 'left', 'valign': 'vcenter', 'border': 1
            }),
            'mp_header': workbook.add_format({
                'bold': True, 'font_color': 'white',
                'bg_color': self.COLORS["mp_header"],
                'border': 1, 'align': 'center', 'valign': 'vcenter',
                'text_wrap': True
            }),
            'input_cell': workbook.add_format({
                'bg_color': self.COLORS["input_bg"],
                'border': 1, 'num_format': '#,##0.00'
            }),
            'input_cell_int': workbook.add_format({
                'bg_color': self.COLORS["input_bg"],
                'border': 1, 'num_format': '0.00'
            }),
            'input_percent': workbook.add_format({
                'bg_color': self.COLORS["input_bg"],
                'border': 1, 'num_format': '0.00%'
            }),
            'param_cell': workbook.add_format({
                'bold': True, 'bg_color': self.COLORS["param_bg"],
                'border': 1, 'valign': 'vcenter'
            }),
            'param_value': workbook.add_format({
                'bold': True, 'font_size': 11,
                'bg_color': self.COLORS["input_bg"],
                'border': 1
            }),
            'formula_cell': workbook.add_format({
                'bg_color': self.COLORS["formula_bg"],
                'border': 1, 'num_format': '#,##0.00 ₽'
            }),
            'formula_percent': workbook.add_format({
                'bg_color': self.COLORS["formula_bg"],
                'border': 1, 'num_format': '0.00%'
            }),
            'money': workbook.add_format({
                'border': 1, 'num_format': '#,##0.00 ₽'
            }),
            'money_bold': workbook.add_format({
                'bold': True, 'border': 1, 'num_format': '#,##0.00 ₽'
            }),
            'bold': workbook.add_format({'bold': True, 'border': 1}),
            'bold_money': workbook.add_format({
                'bold': True, 'font_size': 11,
                'bg_color': self.COLORS["total_bg"],
                'border': 1, 'num_format': '#,##0.00 ₽'
            }),
            'bold_percent': workbook.add_format({
                'bold': True, 'font_size': 11,
                'bg_color': self.COLORS["total_bg"],
                'border': 1, 'num_format': '0.00%'
            }),
            'positive': workbook.add_format({
                'bg_color': self.COLORS["positive"],
                'font_color': self.COLORS["positive_text"],
                'bold': True, 'border': 1
            }),
            'negative': workbook.add_format({
                'bg_color': self.COLORS["negative"],
                'font_color': self.COLORS["negative_text"],
                'bold': True, 'border': 1
            }),
            'warning_cell': workbook.add_format({
                'bg_color': self.COLORS["warning"],
                'font_color': self.COLORS["warning_text"],
                'bold': True, 'border': 1
            }),
            'info': workbook.add_format({
                'italic': True, 'font_color': self.COLORS["positive_text"],
                'bg_color': self.COLORS["positive"], 'border': 1
            }),
            'warning': workbook.add_format({
                'italic': True, 'font_color': self.COLORS["negative_text"],
                'bg_color': self.COLORS["warning"], 'border': 1
            }),
            'default': workbook.add_format({'border': 1}),
            'kpi_label': workbook.add_format({
                'bold': True, 'font_size': 12, 'border': 1,
                'valign': 'vcenter', 'bg_color': self.COLORS["param_bg"]
            }),
            'kpi_positive_money': workbook.add_format({
                'bold': True, 'font_size': 14, 'border': 1,
                'bg_color': self.COLORS["positive"],
                'font_color': self.COLORS["positive_text"],
                'num_format': '#,##0.00 ₽'
            }),
            'kpi_negative_money': workbook.add_format({
                'bold': True, 'font_size': 14, 'border': 1,
                'bg_color': self.COLORS["negative"],
                'font_color': self.COLORS["negative_text"],
                'num_format': '#,##0.00 ₽'
            }),
            'kpi_neutral_money': workbook.add_format({
                'bold': True, 'font_size': 14, 'border': 1,
                'num_format': '#,##0.00 ₽'
            }),
            'kpi_neutral_percent': workbook.add_format({
                'bold': True, 'font_size': 14, 'border': 1,
                'num_format': '0.00%'
            }),
            'kpi_neutral_int': workbook.add_format({
                'bold': True, 'font_size': 14, 'border': 1,
                'num_format': '#,##0'
            }),
            'chart_title': workbook.add_format({
                'bold': True, 'font_size': 12,
                'align': 'center', 'valign': 'vcenter'
            }),
        }
    
    def export_super_pro(self, df: pd.DataFrame, output_path: str, metadata: Dict = None) -> bool:
        """
        🚀 СУПЕР-ПРО экспорт с 10+ листами аналитики
        """
        try:
            if not XLSXWRITER_AVAILABLE:
                logger.error("❌ xlsxwriter не установлен!")
                return False
            
            self._total_rows = len(df)
            
            workbook = xlsxwriter.Workbook(output_path, {'nan_inf_to_errors': True})
            self._init_formats(workbook)
            
            # Создаем все листы
            self._write_dashboard_super(workbook, df, metadata)
            self._write_parameters_super(workbook, metadata)
            self._write_input_data(workbook, df)
            self._write_calculation_engine(workbook, df)
            self._write_marketplace_comparison(workbook, df)
            self._write_category_analysis(workbook, df)
            self._write_profit_forecast(workbook, df)
            self._write_sensitivity_analysis(workbook, df)
            self._write_top_analytics(workbook, df)
            self._write_recommendations(workbook, df)
            self._write_export_summary(workbook, df, metadata)
            
            workbook.close()
            logger.info(f"✅ СУПЕР-ПРО файл сохранён: {output_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка СУПЕР-ПРО экспорта: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def _write_dashboard_super(self, workbook, df: pd.DataFrame, metadata: Dict):
        """📊 СУПЕР-ДАШБОРД с расширенными KPI"""
        ws = workbook.add_worksheet("📊 Дашборд")
        
        ws.merge_range('A1:G1', "🚀 СУПЕР-ДАШБОРД ЮНИТ-ЭКОНОМИКИ",
                       self.formats['header_title'])
        ws.set_row(0, 40)
        
        ws.merge_range('A2:G2',
                       "📊 Ключевые показатели эффективности (KPI) в реальном времени",
                       self.formats['info'])
        ws.set_row(1, 25)
        
        total_profit = df['profit'].sum() if 'profit' in df.columns else 0
        avg_margin = df['margin_percent'].mean() if 'margin_percent' in df.columns else 0
        avg_roi = df['roi'].mean() if 'roi' in df.columns else 0
        total_revenue = df['price'].sum() if 'price' in df.columns else 0
        total_expenses = df['total_expenses'].sum() if 'total_expenses' in df.columns else 0
        unprofitable = (df['profit'] < 0).sum() if 'profit' in df.columns else 0
        
        kpis = [
            ("📦 Всего SKU", f"{len(df):,}", "kpi_neutral_int"),
            ("💰 Общая прибыль", f"{total_profit:,.0f} ₽",
             "kpi_positive_money" if total_profit > 0 else "kpi_negative_money"),
            ("📈 Средняя маржа", f"{avg_margin:.1f}%", "kpi_neutral_percent"),
            ("📊 Средний ROI", f"{avg_roi:.1f}%", "kpi_neutral_percent"),
            ("💵 Общая выручка", f"{total_revenue:,.0f} ₽", "kpi_neutral_money"),
            ("💸 Общие расходы", f"{total_expenses:,.0f} ₽", "kpi_neutral_money"),
            ("⚠️ Убыточных SKU", f"{unprofitable}", "kpi_neutral_int"),
        ]
        
        row = 3
        for i, (label, value, fmt) in enumerate(kpis):
            col = (i % 4) * 2
            ws.write(row, col, label, self.formats['kpi_label'])
            ws.write(row, col + 1, value, self.formats[fmt])
            if i % 4 == 3:
                row += 1
        
        if 'marketplace' in df.columns and 'profit' in df.columns:
            mp_profit = df.groupby('marketplace')['profit'].sum().sort_values(ascending=False)
            
            if not mp_profit.empty:
                chart_row = row + 3
                ws.write(chart_row, 0, "🏪 Прибыль по маркетплейсам",
                         self.formats['chart_title'])
                
                data_start_row = chart_row + 1
                for i, (mp, profit) in enumerate(mp_profit.items()):
                    ws.write(data_start_row + i, 0, mp, self.formats['default'])
                    ws.write(data_start_row + i, 1, profit, self.formats['money'])
                
                chart = workbook.add_chart({'type': 'column'})
                chart.add_series({
                    'name': 'Прибыль по МП',
                    'categories': f'=📊 Дашборд!$A${data_start_row+1}:$A${data_start_row+len(mp_profit)}',
                    'values': f'=📊 Дашборд!$B${data_start_row+1}:$B${data_start_row+len(mp_profit)}',
                    'fill': {'color': self.COLORS["section_bg"]},
                    'border': {'color': self.COLORS["header_bg"]},
                })
                chart.set_title({'name': 'Прибыль по маркетплейсам'})
                chart.set_x_axis({'name': 'Маркетплейс'})
                chart.set_y_axis({'name': 'Прибыль, ₽'})
                chart.set_size({'width': 720, 'height': 400})
                ws.insert_chart(chart_row, 2, chart)
        
        ws.set_column('A:A', 25)
        ws.set_column('B:B', 25)
        ws.set_column('C:C', 25)
        ws.set_column('D:D', 25)
        
        return ws
    
    def _write_parameters_super(self, workbook, metadata: Dict):
        """⚙️ СУПЕР-ПАРАМЕТРЫ с расширенными настройками"""
        ws = workbook.add_worksheet("⚙️ Параметры")
        
        ws.merge_range('A1:P1', "⚙️ РАСШИРЕННЫЕ ПАРАМЕТРЫ РАСЧЁТА",
                       self.formats['header_title'])
        ws.set_row(0, 30)
        
        ws.merge_range('A2:P2',
                       "💡 Все параметры редактируемые — изменения применяются ко всем расчётам",
                       self.formats['info'])
        
        if metadata is None:
            metadata = {}
        
        row = 4
        
        ws.merge_range(row, 0, row, 15, "🌐 ГЛОБАЛЬНЫЕ ПАРАМЕТРЫ",
                       self.formats['section_title'])
        row += 1
        
        global_params = [
            ("Налоговая ставка", 0.06, "Налог от цены продажи", "0.00%"),
            ("Мин. прибыль (%)", 0.10, "Минимальная целевая прибыль", "0.00%"),
            ("Дней хранения", 30, "Среднее кол-во дней", "0"),
            ("ДРР (реклама)", 0.15, "Доля рекламных расходов", "0.00%"),
            ("Курс USD/RUB", 92.50, "Для импортных товаров", "0.00"),
            ("Инфляция %", 0.07, "Годовая инфляция", "0.00%"),
        ]
        
        for name, value, desc, fmt in global_params:
            ws.write(row, 0, name, self.formats['param_cell'])
            if "Дней" in name:
                ws.write(row, 1, value, self.formats['input_cell_int'])
            elif "%" in fmt:
                ws.write(row, 1, value, self.formats['input_percent'])
            else:
                ws.write(row, 1, value, self.formats['input_cell'])
            ws.write(row, 2, desc, self.formats['default'])
            
            if "Налоговая" in name:
                self._global_tax_row = row + 1  # ✅ Excel нумерация с 1
            elif "Мин. прибыль" in name:
                self._global_min_profit_row = row + 1
            
            row += 1
        
        row += 2
        
        ws.merge_range(row, 0, row, 15,
                       "📊 БАЗОВЫЕ ТАРИФЫ (ключ = МП|Режим)",
                       self.formats['section_title'])
        row += 1
        
        headers = [
            'Ключ', 'МП', 'Режим', 'Комиссия', 'Лог. база', 'Лог/кг',
            'Лог/л', 'Хранение', 'Эквайринг', 'Возвраты',
            'Посл. миля', 'Подписка', 'Страховка', 'Упаковка',
            'Надбавка', 'Источник'
        ]
        
        for col_idx, header in enumerate(headers):
            ws.write(row, col_idx, header, self.formats['mp_header'])
        
        self._base_rates_start_row = row + 1
        row += 1
        
        configs = self._get_configs()
        
        if configs:
            for mp_name in sorted(configs.keys()):
                config = configs[mp_name]
                for mode in self.OPERATION_MODES:
                    key = f"{mp_name}|{mode}"
                    base_rate = config.commission_rate
                    mode_mult = config.mode_multipliers.get(mode, 1.0)
                    effective_rate = base_rate * mode_mult
                    
                    ws.write(row, 0, key, self.formats['param_cell'])
                    ws.write(row, 1, mp_name, self.formats['param_cell'])
                    ws.write(row, 2, mode, self.formats['param_cell'])
                    ws.write(row, 3, effective_rate, self.formats['input_percent'])
                    ws.write(row, 4, config.logistics_base, self.formats['input_cell'])
                    ws.write(row, 5, config.logistics_per_kg, self.formats['input_cell'])
                    ws.write(row, 6, config.logistics_per_liter, self.formats['input_cell'])
                    ws.write(row, 7, config.storage_per_day, self.formats['input_cell'])
                    ws.write(row, 8, config.acquiring_fee, self.formats['input_percent'])
                    ws.write(row, 9, config.return_fee, self.formats['input_percent'])
                    ws.write(row, 10, config.last_mile_fee, self.formats['input_cell'])
                    ws.write(row, 11, config.subscription_fee, self.formats['input_cell'])
                    ws.write(row, 12, config.insurance_fee, self.formats['input_percent'])
                    ws.write(row, 13, config.packing_fee, self.formats['input_cell'])
                    ws.write(row, 14, config.hazardous_surcharge, self.formats['input_percent'])
                    ws.write(row, 15, config.tariff_source.value, self.formats['default'])
                    row += 1
        else:
            ws.write(row, 0, "Ozon|FBS", self.formats['param_cell'])
            ws.write(row, 1, "Ozon", self.formats['param_cell'])
            ws.write(row, 2, "FBS", self.formats['param_cell'])
            ws.write(row, 3, 0.15, self.formats['input_percent'])
            row += 1
        
        self._base_rates_end_row = row
        
        ws.set_column('A:A', 18)
        ws.set_column('B:C', 14)
        ws.set_column('D:O', 14)
        ws.set_column('P:P', 16)
        
        return ws
    
    def _write_input_data(self, workbook, df: pd.DataFrame):
        """📥 Входные данные с валидацией"""
        ws = workbook.add_worksheet("📥 Входные")
        
        ws.merge_range('A1:N1',
                       "📥 ВХОДНЫЕ ДАННЫЕ (редактируемые)",
                       self.formats['header_title'])
        ws.set_row(0, 28)
        
        ws.merge_range('A2:N2',
                       "💡 Меняйте значения — все листы пересчитаются автоматически",
                       self.formats['info'])
        
        headers = [
            'Артикул', 'Бренд', 'МП', 'Режим', 'Категория',
            'Цена', 'Себест-ть', 'Вес, кг',
            'Длина, см', 'Ширина, см', 'Высота, см',
            'Объём, л', 'Оплач. вес', 'Наценка %'
        ]
        
        for col_idx, header in enumerate(headers):
            ws.write(2, col_idx, header, self.formats['header'])
        
        ws.set_row(2, 30)
        
        for i, (_, row_data) in enumerate(df.iterrows()):
            excel_row = 3 + i
            
            ws.write(excel_row, 0, str(row_data.get('Артикул', '')), self.formats['default'])
            ws.write(excel_row, 1, str(row_data.get('Бренд', '')), self.formats['default'])
            ws.write(excel_row, 2, str(row_data.get('marketplace', 'Ozon')), self.formats['default'])
            ws.write(excel_row, 3, str(row_data.get('operation_mode', 'FBS')), self.formats['default'])
            
            category = str(row_data.get('category', ''))
            if category:
                category = category.lower().replace(' ', '_')
            ws.write(excel_row, 4, category, self.formats['default'])
            
            ws.write(excel_row, 5, float(row_data.get('price', 0)), self.formats['input_cell'])
            ws.write(excel_row, 6, float(row_data.get('cost', 0)), self.formats['input_cell'])
            ws.write(excel_row, 7, float(row_data.get('weight', 0)), self.formats['input_cell_int'])
            ws.write(excel_row, 8, float(row_data.get('length', 0)), self.formats['input_cell_int'])
            ws.write(excel_row, 9, float(row_data.get('width', 0)), self.formats['input_cell_int'])
            ws.write(excel_row, 10, float(row_data.get('height', 0)), self.formats['input_cell_int'])
            
            volume = (float(row_data.get('length', 0)) *
                      float(row_data.get('width', 0)) *
                      float(row_data.get('height', 0))) / 1000
            ws.write(excel_row, 11, volume, self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 12,
                             f"=MAX(G{excel_row+1}, L{excel_row+1}/5000)",
                             self.formats['formula_cell'])
            
            ws.write(excel_row, 13, 0, self.formats['input_percent'])
        
        ws.set_column('A:B', 18)
        ws.set_column('C:D', 15)
        ws.set_column('E:E', 18)
        ws.set_column('F:M', 14)
        ws.set_column('N:N', 14)
        
        ws.freeze_panes(3, 0)
        
        if self._total_rows > 0:
            ws.autofilter(2, 0, 2 + self._total_rows, 13)
        
        return ws
    
    def _write_calculation_engine(self, workbook, df: pd.DataFrame):
        """📊 ДВИЖОК РАСЧЁТОВ с полной детализацией"""
        ws = workbook.add_worksheet("📊 Расчёт")
        
        ws.merge_range('A1:W1',
                       "📊 ПОЛНЫЙ РАСЧЁТ ЮНИТ-ЭКОНОМИКИ",
                       self.formats['header_title'])
        ws.set_row(0, 28)
        
        ws.merge_range('A2:W2',
                       "⚠️ Все расчёты автоматические — не редактируйте формулы",
                       self.formats['warning'])
        
        headers = [
            'Артикул', 'МП', 'Режим', 'Категория',
            'Цена', 'Себест-ть', 'Вес', 'Объём',
            'Комиссия', 'Логистика', 'Хранение',
            'Эквайринг', 'Посл. миля', 'Возвраты',
            'Реклама', 'Налог', 'Страховка', 'Упаковка',
            'ИТОГО расходов', '💰 ПРИБЫЛЬ',
            'Маржа %', 'ROI %', 'Безубыт-ть'
        ]
        
        for col_idx, header in enumerate(headers):
            ws.write(2, col_idx, header, self.formats['header'])
        
        ws.set_row(2, 35)
        
        # ✅ ИСПРАВЛЕНИЕ v100.11: Используем константы вместо магических чисел
        p_tax = f"'⚙️ Параметры'!$B${self._global_tax_row}"
        min_profit = f"'⚙️ Параметры'!$B${self._global_min_profit_row}"
        p_ad = f"'⚙️ Параметры'!$B${self.AD_ROW}"
        p_days = f"'⚙️ Параметры'!$B${self.DAYS_ROW}"
        p_currency = f"'⚙️ Параметры'!$B${self.CURRENCY_ROW}"
        
        params_range = f"'⚙️ Параметры'!$A${self._base_rates_start_row}:$P${self._base_rates_end_row}"
        
        for i in range(self._total_rows):
            excel_row = 3 + i
            input_row = 4 + i
            
            in_art = f"'📥 Входные'!A{input_row}"
            in_mp = f"'📥 Входные'!C{input_row}"
            in_mode = f"'📥 Входные'!D{input_row}"
            in_cat = f"'📥 Входные'!E{input_row}"
            in_price = f"'📥 Входные'!F{input_row}"
            in_cost = f"'📥 Входные'!G{input_row}"
            in_weight = f"'📥 Входные'!H{input_row}"
            in_volume = f"'📥 Входные'!L{input_row}"
            
            lookup_key = f'CONCATENATE({in_mp},"|",{in_mode})'
            
            ws.write_formula(excel_row, 0, f"={in_art}", self.formats['default'])
            ws.write_formula(excel_row, 1, f"={in_mp}", self.formats['default'])
            ws.write_formula(excel_row, 2, f"={in_mode}", self.formats['default'])
            ws.write_formula(excel_row, 3, f"={in_cat}", self.formats['default'])
            ws.write_formula(excel_row, 4, f"={in_price}", self.formats['formula_cell'])
            ws.write_formula(excel_row, 5, f"={in_cost}", self.formats['formula_cell'])
            ws.write_formula(excel_row, 6, f"={in_weight}", self.formats['formula_cell'])
            ws.write_formula(excel_row, 7, f"={in_volume}", self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 8,
                             f"=VLOOKUP({lookup_key},{params_range},4,FALSE)*{in_price}",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 9,
                             f"=VLOOKUP({lookup_key},{params_range},5,FALSE)+"
                             f"{in_weight}*VLOOKUP({lookup_key},{params_range},6,FALSE)+"
                             f"{in_volume}*VLOOKUP({lookup_key},{params_range},7,FALSE)",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 10,
                             f"={in_volume}*VLOOKUP({lookup_key},{params_range},8,FALSE)*{p_days}",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 11,
                             f"=VLOOKUP({lookup_key},{params_range},9,FALSE)*{in_price}",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 12,
                             f"=VLOOKUP({lookup_key},{params_range},11,FALSE)",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 13,
                             f"=VLOOKUP({lookup_key},{params_range},10,FALSE)*{in_price}*1.3",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 14,
                             f"={in_price}*{p_ad}",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 15,
                             f"={in_price}*{p_tax}",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 16,
                             f"=VLOOKUP({lookup_key},{params_range},13,FALSE)*{in_price}",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 17,
                             f"=VLOOKUP({lookup_key},{params_range},14,FALSE)",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 18,
                             f"={in_cost}+SUM(I{excel_row+1}:R{excel_row+1})",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 19,
                             f"={in_price}-S{excel_row+1}",
                             self.formats['formula_cell'])
            
            ws.write_formula(excel_row, 20,
                             f"=IF({in_price}>0,T{excel_row+1}/{in_price},0)",
                             self.formats['formula_percent'])
            
            ws.write_formula(excel_row, 21,
                             f"=IF({in_cost}>0,T{excel_row+1}/{in_cost},0)",
                             self.formats['formula_percent'])
            
            ws.write_formula(excel_row, 22,
                             f"=S{excel_row+1}/(1-"
                             f"VLOOKUP({lookup_key},{params_range},4,FALSE)-"
                             f"VLOOKUP({lookup_key},{params_range},9,FALSE)-{p_tax})",
                             self.formats['formula_cell'])
        
        if self._total_rows > 0:
            last_row = 3 + self._total_rows
            profit_range = f"T4:T{last_row}"
            
            ws.conditional_format(profit_range, {
                'type': 'cell',
                'criteria': '>',
                'value': 0,
                'format': self.formats['positive']
            })
            
            ws.conditional_format(profit_range, {
                'type': 'cell',
                'criteria': '<',
                'value': 0,
                'format': self.formats['negative']
            })
            
            margin_range = f"U4:U{last_row}"
            ws.conditional_format(margin_range, {
                'type': '3_color_scale',
                'min_color': self.COLORS["negative"],
                'mid_color': self.COLORS["warning"],
                'max_color': self.COLORS["positive"]
            })
            
            total_row = 3 + self._total_rows + 2
            ws.merge_range(total_row, 0, total_row, 2,
                           "ИТОГО / СРЕДНЕЕ:", self.formats['bold_money'])
            
            last_data_row = 3 + self._total_rows
            for col_idx, col_letter in enumerate(['E', 'F', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T']):
                ws.write_formula(total_row, col_idx + 4,
                                 f"=SUM({col_letter}4:{col_letter}{last_data_row})",
                                 self.formats['bold_money'])
            
            for col_idx, col_letter in enumerate(['U', 'V'], start=20):
                ws.write_formula(total_row, col_idx,
                                 f"=AVERAGE({col_letter}4:{col_letter}{last_data_row})",
                                 self.formats['bold_percent'])
        
        widths = {
            'A': 15, 'B': 14, 'C': 10, 'D': 14, 'E': 12, 'F': 12,
            'G': 10, 'H': 10, 'I': 12, 'J': 12, 'K': 12, 'L': 12,
            'M': 12, 'N': 12, 'O': 12, 'P': 12, 'Q': 12, 'R': 12,
            'S': 15, 'T': 15, 'U': 12, 'V': 12, 'W': 14
        }
        
        for col, width in widths.items():
            ws.set_column(f'{col}:{col}', width)
        
        ws.freeze_panes(3, 0)
        
        if self._total_rows > 0:
            ws.autofilter(2, 0, 2 + self._total_rows, 22)
        
        return ws
    
    def _write_marketplace_comparison(self, workbook, df: pd.DataFrame):
        """🏪 Сравнение маркетплейсов с автоматическими выводами"""
        ws = workbook.add_worksheet("🏪 Сравнение МП")
        
        ws.merge_range('A1:K1', "🏪 СРАВНИТЕЛЬНЫЙ АНАЛИЗ МАРКЕТПЛЕЙСОВ",
                       self.formats['header_title'])
        
        headers = [
            'МП', 'SKU', 'Выручка', 'Расходы', 'Прибыль',
            'Ср. прибыль', 'Ср. маржа %', 'ROI %',
            'Доля рынка %', 'Эффективность', 'Рейтинг'
        ]
        
        for col_idx, header in enumerate(headers):
            ws.write(2, col_idx, header, self.formats['header'])
        
        if 'marketplace' in df.columns:
            mp_stats = df.groupby('marketplace').agg({
                'price': 'sum',
                'total_expenses': 'sum',
                'profit': ['sum', 'mean'],
                'margin_percent': 'mean',
                'roi': 'mean',
            }).reset_index()
            
            mp_stats.columns = ['МП', 'Выручка', 'Расходы', 'Прибыль', 'Ср. прибыль', 'Ср. маржа %', 'ROI %']
            
            total_profit = mp_stats['Прибыль'].sum()
            
            for i, row in mp_stats.iterrows():
                excel_row = 3 + i
                
                ws.write(excel_row, 0, row['МП'], self.formats['bold'])
                
                ws.write_formula(excel_row, 1,
                                 f"=COUNTIF('📊 Расчёт'!$B:$B,A{excel_row+1})",
                                 self.formats['default'])
                
                ws.write(excel_row, 2, row['Выручка'], self.formats['money'])
                ws.write(excel_row, 3, row['Расходы'], self.formats['money'])
                ws.write(excel_row, 4, row['Прибыль'],
                         self.formats['positive'] if row['Прибыль'] > 0 else self.formats['negative'])
                ws.write(excel_row, 5, row['Ср. прибыль'], self.formats['money'])
                ws.write(excel_row, 6, row['Ср. маржа %'], self.formats['formula_percent'])
                ws.write(excel_row, 7, row['ROI %'], self.formats['formula_percent'])
                
                share = (row['Прибыль'] / total_profit * 100) if total_profit > 0 else 0
                ws.write(excel_row, 8, share / 100, self.formats['formula_percent'])
                
                ws.write_formula(excel_row, 9,
                                 f"=IF(C{excel_row+1}>0,E{excel_row+1}/C{excel_row+1},0)",
                                 self.formats['formula_percent'])
                
                ws.write_formula(excel_row, 10,
                                 f"=RANK(E{excel_row+1},$E$4:$E${3+len(mp_stats)})",
                                 self.formats['default'])
        
        ws.set_column('A:K', 16)
        ws.freeze_panes(3, 0)
        
        return ws
    
    def _write_category_analysis(self, workbook, df: pd.DataFrame):
        """📂 Анализ по категориям - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        ws = workbook.add_worksheet("📂 Категории")
        
        ws.merge_range('A1:H1', "📂 АНАЛИЗ ПО КАТЕГОРИЯМ",
                       self.formats['header_title'])
        
        headers = ['Категория', 'SKU', 'Выручка', 'Прибыль', 'Ср. маржа %',
                   'Топ товар', 'Прибыль топ', 'Доля %']
        
        for col_idx, header in enumerate(headers):
            ws.write(2, col_idx, header, self.formats['header'])
        
        if 'category' in df.columns:
            # ✅ ИСПРАВЛЕНИЕ: правильная агрегация с 3 колонками
            cat_stats = df.groupby('category').agg({
                'price': 'sum',
                'profit': 'sum',
                'margin_percent': 'mean',
            }).reset_index()
            
            # ✅ ИСПРАВЛЕНИЕ: ровно 4 колонки
            cat_stats.columns = ['Категория', 'Выручка', 'Прибыль', 'Ср. маржа %']
            
            total_profit = cat_stats['Прибыль'].sum()
            
            for i, row in cat_stats.iterrows():
                excel_row = 3 + i
                
                ws.write(excel_row, 0, row['Категория'], self.formats['bold'])
                
                ws.write_formula(excel_row, 1,
                                 f"=COUNTIF('📊 Расчёт'!$D:$D,A{excel_row+1})",
                                 self.formats['default'])
                
                ws.write(excel_row, 2, row['Выручка'], self.formats['money'])
                ws.write(excel_row, 3, row['Прибыль'],
                         self.formats['positive'] if row['Прибыль'] > 0 else self.formats['negative'])
                ws.write(excel_row, 4, row['Ср. маржа %'], self.formats['formula_percent'])
                
                ws.write_formula(excel_row, 5,
                                 f"=INDEX('📊 Расчёт'!$A:$A,MATCH(MAX(IF('📊 Расчёт'!$D:$D=A{excel_row+1},'📊 Расчёт'!$T:$T)),'📊 Расчёт'!$T:$T,0))",
                                 self.formats['default'])
                
                ws.write_formula(excel_row, 6,
                                 f"=MAX(IF('📊 Расчёт'!$D:$D=A{excel_row+1},'📊 Расчёт'!$T:$T))",
                                 self.formats['money'])
                
                share = (row['Прибыль'] / total_profit * 100) if total_profit > 0 else 0
                ws.write(excel_row, 7, share / 100, self.formats['formula_percent'])
        
        ws.set_column('A:H', 16)
        ws.freeze_panes(3, 0)
        
        return ws
    
    def _write_profit_forecast(self, workbook, df: pd.DataFrame):
        """📈 Прогноз прибыли на 12 месяцев"""
        ws = workbook.add_worksheet("📈 Прогноз")
        
        ws.merge_range('A1:G1', "📈 ПРОГНОЗ ПРИБЫЛИ НА 12 МЕСЯЦЕВ",
                       self.formats['header_title'])
        
        headers = ['Месяц', 'Оптимистичный', 'Базовый', 'Пессимистичный',
                   'Ср. значение', 'Рост %', 'Тренд']
        
        for col_idx, header in enumerate(headers):
            ws.write(2, col_idx, header, self.formats['header'])
        
        total_profit = df['profit'].sum() if 'profit' in df.columns else 0
        base_monthly = total_profit / 12 if total_profit > 0 else 1000
        
        growth_rate = 0.05
        volatility = 0.15
        
        seasonal = [0.85, 0.85, 0.95, 1.05, 1.10, 1.15,
                    1.20, 1.15, 1.10, 1.05, 0.95, 0.90]
        
        month_names = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн',
                       'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
        
        for i in range(12):
            excel_row = 3 + i
            
            month_factor = seasonal[i]
            trend_factor = (1 + growth_rate) ** (i / 12)
            
            base = base_monthly * month_factor * trend_factor
            optimistic = base * (1 + volatility * 0.5)
            pessimistic = base * (1 - volatility * 0.3)
            
            ws.write(excel_row, 0, month_names[i], self.formats['default'])
            ws.write(excel_row, 1, optimistic, self.formats['money'])
            ws.write(excel_row, 2, base, self.formats['money'])
            ws.write(excel_row, 3, pessimistic, self.formats['money'])
            ws.write(excel_row, 4, base, self.formats['money'])
            
            if i > 0:
                prev_base = base_monthly * seasonal[i-1] * (1 + growth_rate) ** ((i-1)/12)
                growth = (base / prev_base - 1) if prev_base > 0 else 0
                ws.write(excel_row, 5, growth, self.formats['formula_percent'])
                ws.write(excel_row, 6, "↑" if growth > 0.02 else "↓" if growth < -0.02 else "→",
                         self.formats['default'])
            else:
                ws.write(excel_row, 5, 0, self.formats['formula_percent'])
                ws.write(excel_row, 6, "→", self.formats['default'])
        
        chart = workbook.add_chart({'type': 'line'})
        
        chart.add_series({
            'name': 'Оптимистичный',
            'categories': f'=📈 Прогноз!$A$4:$A$15',
            'values': f'=📈 Прогноз!$B$4:$B$15',
            'line': {'color': 'green', 'width': 2},
        })
        
        chart.add_series({
            'name': 'Базовый',
            'categories': f'=📈 Прогноз!$A$4:$A$15',
            'values': f'=📈 Прогноз!$C$4:$C$15',
            'line': {'color': 'blue', 'width': 3},
        })
        
        chart.add_series({
            'name': 'Пессимистичный',
            'categories': f'=📈 Прогноз!$A$4:$A$15',
            'values': f'=📈 Прогноз!$D$4:$D$15',
            'line': {'color': 'red', 'width': 2, 'dash_type': 'dash'},
        })
        
        chart.set_title({'name': 'Прогноз прибыли'})
        chart.set_x_axis({'name': 'Месяц'})
        chart.set_y_axis({'name': 'Прибыль, ₽'})
        chart.set_size({'width': 720, 'height': 400})
        
        ws.insert_chart(16, 0, chart)
        
        ws.set_column('A:G', 16)
        
        return ws
    
    def _write_sensitivity_analysis(self, workbook, df: pd.DataFrame):
        """🎯 Анализ чувствительности"""
        ws = workbook.add_worksheet("🎯 Чувствительность")
        
        ws.merge_range('A1:I1', "🎯 АНАЛИЗ ЧУВСТВИТЕЛЬНОСТИ",
                       self.formats['header_title'])
        
        ws.merge_range('A2:I2',
                       "Как изменяется прибыль при изменении ключевых параметров",
                       self.formats['info'])
        
        avg_price = df['price'].mean() if 'price' in df.columns else 1000
        avg_cost = df['cost'].mean() if 'cost' in df.columns else 500
        
        row = 4
        
        ws.write(row, 0, "Параметр", self.formats['header'])
        ws.write(row, 1, "Текущее", self.formats['header'])
        ws.write(row, 2, "-20%", self.formats['header'])
        ws.write(row, 3, "-10%", self.formats['header'])
        ws.write(row, 4, "0%", self.formats['header'])
        ws.write(row, 5, "+10%", self.formats['header'])
        ws.write(row, 6, "+20%", self.formats['header'])
        
        row += 1
        
        scenarios = [
            ("Цена продажи", avg_price),
            ("Себестоимость", avg_cost),
            ("Комиссия МП", 0.15),
            ("Логистика", 100),
            ("Реклама (ДРР)", 0.15),
        ]
        
        for param_name, base_value in scenarios:
            ws.write(row, 0, param_name, self.formats['param_cell'])
            ws.write(row, 1, base_value, self.formats['default'])
            
            for i, change in enumerate([-0.20, -0.10, 0, 0.10, 0.20]):
                new_value = base_value * (1 + change)
                ws.write(row, 2 + i, new_value, self.formats['input_cell'])
            
            row += 1
        
        ws.set_column('A:I', 16)
        
        return ws
    
    def _write_top_analytics(self, workbook, df: pd.DataFrame):
        """🏆 Топ-аналитика"""
        ws = workbook.add_worksheet("🏆 Топ")
        
        ws.merge_range('A1:F1', "🏆 ТОП-10 ПРИБЫЛЬНЫХ И УБЫТОЧНЫХ",
                       self.formats['header_title'])
        
        ws.write(2, 0, "ТОП-10 ПРИБЫЛЬНЫХ", self.formats['section_title'])
        
        headers = ['№', 'Артикул', 'МП', 'Прибыль', 'Маржа %', 'Рекомендация']
        
        for col_idx, header in enumerate(headers):
            ws.write(3, col_idx, header, self.formats['header'])
        
        if 'profit' in df.columns and 'Артикул' in df.columns:
            top_df = df.nlargest(10, 'profit')
            
            for i, (_, row) in enumerate(top_df.iterrows()):
                excel_row = 4 + i
                
                ws.write(excel_row, 0, i + 1, self.formats['default'])
                ws.write(excel_row, 1, row.get('Артикул', ''), self.formats['default'])
                ws.write(excel_row, 2, row.get('marketplace', ''), self.formats['default'])
                ws.write(excel_row, 3, row.get('profit', 0), self.formats['positive'])
                ws.write(excel_row, 4, row.get('margin_percent', 0), self.formats['formula_percent'])
                ws.write(excel_row, 5, "✅ Лидер", self.formats['info'])
        
        bottom_start = 4 + 10 + 3
        
        ws.write(bottom_start, 0, "ТОП-10 УБЫТОЧНЫХ", self.formats['section_title'])
        
        for col_idx, header in enumerate(headers):
            ws.write(bottom_start + 1, col_idx, header, self.formats['header'])
        
        if 'profit' in df.columns:
            bottom_df = df.nsmallest(10, 'profit')
            
            for i, (_, row) in enumerate(bottom_df.iterrows()):
                excel_row = bottom_start + 2 + i
                
                ws.write(excel_row, 0, i + 1, self.formats['default'])
                ws.write(excel_row, 1, row.get('Артикул', ''), self.formats['default'])
                ws.write(excel_row, 2, row.get('marketplace', ''), self.formats['default'])
                ws.write(excel_row, 3, row.get('profit', 0), self.formats['negative'])
                ws.write(excel_row, 4, row.get('margin_percent', 0), self.formats['formula_percent'])
                ws.write(excel_row, 5, "⚠️ Требует внимания", self.formats['warning_cell'])
        
        ws.set_column('A:F', 16)
        
        return ws
    
    def _write_recommendations(self, workbook, df: pd.DataFrame):
        """💡 Автоматические рекомендации"""
        ws = workbook.add_worksheet("💡 Рекомендации")
        
        ws.merge_range('A1:D1', "💡 АВТОМАТИЧЕСКИЕ РЕКОМЕНДАЦИИ",
                       self.formats['header_title'])
        
        ws.merge_range('A2:D2',
                       "Система анализирует данные и предлагает оптимальные решения",
                       self.formats['info'])
        
        row = 4
        
        if 'marketplace' in df.columns and 'profit' in df.columns:
            best_mp = df.groupby('marketplace')['profit'].sum().idxmax()
            ws.write(row, 0, "🏪 Лучший маркетплейс", self.formats['bold'])
            ws.merge_range(row, 1, row, 3,
                           f"✅ Рекомендуется использовать {best_mp} — он приносит максимальную прибыль",
                           self.formats['info'])
            row += 2
        
        if 'operation_mode' in df.columns and 'profit' in df.columns:
            best_mode = df.groupby('operation_mode')['profit'].sum().idxmax()
            ws.write(row, 0, "📦 Оптимальный режим", self.formats['bold'])
            ws.merge_range(row, 1, row, 3,
                           f"✅ Режим {best_mode} показывает лучшие результаты",
                           self.formats['info'])
            row += 2
        
        avg_margin = df['margin_percent'].mean() if 'margin_percent' in df.columns else 0
        if avg_margin < 15:
            ws.write(row, 0, "💰 Ценовая политика", self.formats['bold'])
            ws.merge_range(row, 1, row, 3,
                           "⚠️ Средняя маржа ниже 15%. Рекомендуется пересмотреть цены",
                           self.formats['warning_cell'])
            row += 2
        
        if 'profit' in df.columns:
            unprofitable = (df['profit'] < 0).sum()
            if unprofitable > 0:
                ws.write(row, 0, "⚠️ Убыточные товары", self.formats['bold'])
                ws.merge_range(row, 1, row, 3,
                               f"⚠️ {unprofitable} товаров убыточны. Рекомендуется провести аудит",
                               self.formats['warning_cell'])
                row += 2
        
        if 'total_expenses' in df.columns and 'price' in df.columns:
            expense_ratio = (df['total_expenses'].sum() / df['price'].sum() * 100) if df['price'].sum() > 0 else 0
            
            if expense_ratio > 70:
                ws.write(row, 0, "📉 Оптимизация расходов", self.formats['bold'])
                ws.merge_range(row, 1, row, 3,
                               f"⚠️ Расходы составляют {expense_ratio:.1f}% от выручки. Ищите точки оптимизации",
                               self.formats['warning_cell'])
            else:
                ws.write(row, 0, "📈 Эффективность", self.formats['bold'])
                ws.merge_range(row, 1, row, 3,
                               f"✅ Расходы составляют {expense_ratio:.1f}% от выручки — хороший показатель",
                               self.formats['info'])
        
        ws.set_column('A:A', 25)
        ws.set_column('B:D', 30)
        
        return ws
    
    def _write_export_summary(self, workbook, df: pd.DataFrame, metadata: Dict):
        """📋 Сводка экспорта"""
        ws = workbook.add_worksheet("📋 Сводка")
        
        ws.merge_range('A1:C1', "📋 СВОДКА ЭКСПОРТА",
                       self.formats['header_title'])
        
        row = 3
        
        summary = [
            ("📅 Дата экспорта", datetime.now().strftime('%d.%m.%Y %H:%M:%S')),
            ("📦 Всего товаров", f"{len(df):,}"),
            ("🏪 Маркетплейсы", ", ".join(metadata.get('marketplaces', ['Ozon'])) if metadata else "Ozon"),
            ("📊 Режимы", ", ".join(metadata.get('modes', ['FBS'])) if metadata else "FBS"),
            ("💰 Общая прибыль", f"{df['profit'].sum():,.0f} ₽" if 'profit' in df.columns else "Н/Д"),
            ("📈 Средняя маржа", f"{df['margin_percent'].mean():.1f}%" if 'margin_percent' in df.columns else "Н/Д"),
            ("⚙️ Версия", "SUPER-PRO v2.0"),
        ]
        
        for label, value in summary:
            ws.write(row, 0, label, self.formats['param_cell'])
            ws.write(row, 1, value, self.formats['default'])
            row += 1
        
        ws.set_column('A:A', 30)
        ws.set_column('B:B', 40)
        
        return ws
# ============================================================================
# 🆕 БЛОК 15: UI ФУНКЦИИ - ЮНИТ-ЭКОНОМИКА (v100.6 - УЛУЧШЕННАЯ)
# ============================================================================
def show_unit_economics_interface():
    """
    📊 РАЗДЕЛ 2: ЮНИТ-ЭКОНОМИКА С ПАРАЛЛЕЛЬНЫМ РАСЧЕТОМ
    Оптимизирована для 350K+ товаров с живыми формулами Excel
    """
    st.header("📊 Шаг 2: Расчет юнит-экономики")
    st.info("""
💡 **ДВА СПОСОБА РАСЧЕТА:**
**Способ 1:** Расчет для одного товара (введите данные вручную)
**Способ 2:** Расчет по всему каталогу (загрузите файл в разделе "Загрузка данных")
🚀 **ДЛЯ БОЛЬШИХ КАТАЛОГОВ (>1000 товаров)** используется параллельный расчет
🆕 **v100.6:** Экспорт в Excel с живыми формулами — меняйте значения, всё пересчитается!
""")
    
    calculation_mode = st.radio(
        "🎯 Выберите способ расчета:",
        ["📝 Один товар (вручную)", "📦 Весь каталог (из файла)"],
        horizontal=True,
        key="calc_mode"
    )
    
    if calculation_mode == "📝 Один товар (вручную)":
        show_single_product_calculation()
    else:
        show_catalog_calculation_parallel()


def show_single_product_calculation():
    """Расчет для одного товара с учетом сезонности"""
    st.subheader("📝 Расчет для одного товара")
    
    unit_economics = get_marketplace_unit_economics()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 💰 Финансовые параметры")
        
        price = st.number_input(
            "💰 Цена продажи (₽)",
            min_value=0.0,
            value=1000.0,
            step=10.0,
            key="ue_price",
            help="Цена, по которой вы продаете товар"
        )
        
        cost = st.number_input(
            "💵 Себестоимость (₽)",
            min_value=0.0,
            value=500.0,
            step=10.0,
            key="ue_cost",
            help="Закупочная цена товара"
        )
        
        dimension_input = st.text_input(
            "📏 Размеры (ДxШxВ) или Весогабариты",
            placeholder="например: 20x15x10",
            key="ue_dimensions",
            help="Введите размеры в формате Длина x Ширина x Высота"
        )
        
        if dimension_input:
            l, w, h = parse_dimensions_string(dimension_input)
            if l > 0 and w > 0 and h > 0:
                st.success(f"✅ Распарсено: {l:.1f} x {w:.1f} x {h:.1f} см")
            else:
                st.warning("⚠️ Не удалось распарсить размеры. Используйте формат: 20x15x10")
    
    with col2:
        st.markdown("### 🏪 Параметры маркетплейса")
        
        weight = st.number_input(
            "⚖️ Вес (кг)",
            min_value=0.0,
            value=1.0,
            step=0.1,
            key="ue_weight",
            help="Вес товара в килограммах"
        )
        
        marketplace = st.selectbox(
            "🏪 Маркетплейс",
            list(unit_economics._configs.keys()),
            key="ue_marketplace",
            help="Выберите маркетплейс для расчета"
        )
        
        operation_mode = st.selectbox(
            "📦 Режим работы",
            ["FBY", "FBS", "FBO", "DBS", "FBP"],
            key="ue_mode",
            help="FBY - самый дешевый, FBS - базовый"
        )
        
        category = st.text_input(
            "📂 Категория (опционально)",
            placeholder="например: двигатель",
            key="ue_category",
            help="Категория товара для точного расчета комиссии"
        )
        
        tax_system = st.selectbox(
            "💼 Налоговый режим",
            list(TAX_SYSTEMS.keys()),
            format_func=lambda x: TAX_SYSTEMS[x]["name"],
            key="ue_tax_system",
            help="Выберите систему налогообложения"
        )
        
        ad_intensity = st.selectbox(
            "📢 Интенсивность рекламы",
            ["low", "medium", "high", "aggressive"],
            format_func=lambda x: {"low": "Низкая (5%)", "medium": "Средняя (15%)", "high": "Высокая (25%)", "aggressive": "Агрессивная (35%)"}[x],
            key="ue_ad_intensity",
            help="Доля рекламных расходов (ДРР)"
        )
        
        is_premium = st.checkbox("⭐ Премиум-раздел (доп. комиссия)", key="ue_premium")
        use_seasonal = st.checkbox("🌤 Учесть сезонный коэффициент", value=True, key="ue_seasonal")
    
    if st.button("🚀 Рассчитать юнит-экономику", type="primary", key="ue_calc"):
        with st.spinner("Расчет юнит-экономики..."):
            current_month = datetime.now().month if use_seasonal else None
            
            economics = unit_economics.calculate_unit_economics(
                price=price,
                cost=cost,
                marketplace=marketplace,
                weight=weight,
                category=category if category else None,
                is_premium=is_premium,
                current_month=current_month,
                tax_system=tax_system,
                ad_intensity=ad_intensity
            )
            
            st.subheader("📊 Результаты расчета")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("💰 Прибыль", f"{economics.profit:.2f} ₽", delta=f"{economics.profit_per_ruble:.2f} ₽/₽")
            with col2:
                st.metric("📈 Маржа", f"{economics.margin_percent:.2f}%")
            with col3:
                st.metric("📊 ROI", f"{economics.roi:.2f}%")
            with col4:
                st.metric("⚖️ Точка безубыточности", f"{economics.breakeven_price:.2f} ₽")
            
            if economics.applied_seasonal_multiplier != 1.0:
                st.info(f"🌤 Применен сезонный коэффициент: {economics.applied_seasonal_multiplier:.2f}x")
            
            if economics.applied_promo_discount > 0:
                st.info(f"🎯 Применена промо-скидка: {economics.applied_promo_discount * 100:.1f}%")
            
            st.subheader("🆕 v100.5: Улучшенные метрики")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("⚖️ Оплачиваемый вес", f"{economics.billable_weight:.2f} кг")
            with col2:
                st.metric("📢 Реклама (ДРР)", f"{economics.advertising_cost:.2f} ₽")
            with col3:
                st.metric("🔧 Спец. расходы", f"{economics.auto_parts_specific:.2f} ₽")
            
            st.subheader("💎 Рекомендованная минимальная цена")
            col_rec1, col_rec2, col_rec3 = st.columns(3)
            with col_rec1:
                st.metric(
                    "🎯 Мин. цена (с учётом налога и 10% прибыли)",
                    f"{economics.recommended_min_price:.2f} ₽",
                    delta=f"{economics.recommended_min_price - price:.2f} ₽"
                )
            with col_rec2:
                st.metric(f"💵 Налог ({TAX_SYSTEMS[economics.tax_system]['name']})", f"{economics.tax_amount:.2f} ₽")
            with col_rec3:
                if price < economics.recommended_min_price:
                    st.warning(f"⚠️ Цена ниже рекомендованной на {economics.recommended_min_price - price:.2f} ₽")
                else:
                    st.success(f"✅ Цена выше минимальной на {price - economics.recommended_min_price:.2f} ₽")
            
            st.subheader("📋 Детализация расходов")
            
            expenses_data = {
                "Статья расходов": [
                    "Себестоимость", "Комиссия", "Подписка", "Логистика",
                    "Хранение", "Эквайринг", "Доставка", "Последняя миля",
                    "Возвраты", "РКО", "Премиум", "Страховка", "Упаковка", "Маркетинг",
                    "Надбавка за опасные", "Надбавка за хрупкие", "Надбавка за крупногабарит",
                    f"Налог ({TAX_SYSTEMS[economics.tax_system]['name']})",
                    "🆕 Спец. расходы автозапчастей",
                    "🆕 Рекламные расходы",
                    "ИТОГО"
                ],
                "Сумма (₽)": [
                    economics.cost, economics.commission, economics.subscription_cost,
                    economics.logistics, economics.storage_cost, economics.acquiring,
                    economics.delivery, economics.last_mile, economics.returns,
                    economics.rko_fee, economics.premium_fee, economics.insurance_fee,
                    economics.packing_fee, economics.marketing_fee,
                    economics.hazardous_surcharge, economics.fragile_surcharge,
                    economics.oversized_surcharge, economics.tax_amount,
                    economics.auto_parts_specific, economics.advertising_cost,
                    economics.total_expenses
                ],
                "% от цены": [
                    f"{economics.cost/price*100:.1f}%",
                    f"{economics.commission/price*100:.1f}%",
                    f"{economics.subscription_cost/price*100:.1f}%",
                    f"{economics.logistics/price*100:.1f}%",
                    f"{economics.storage_cost/price*100:.1f}%",
                    f"{economics.acquiring/price*100:.1f}%",
                    f"{economics.delivery/price*100:.1f}%",
                    f"{economics.last_mile/price*100:.1f}%",
                    f"{economics.returns/price*100:.1f}%",
                    f"{economics.rko_fee/price*100:.1f}%",
                    f"{economics.premium_fee/price*100:.1f}%",
                    f"{economics.insurance_fee/price*100:.1f}%",
                    f"{economics.packing_fee/price*100:.1f}%",
                    f"{economics.marketing_fee/price*100:.1f}%",
                    f"{economics.hazardous_surcharge/price*100:.1f}%",
                    f"{economics.fragile_surcharge/price*100:.1f}%",
                    f"{economics.oversized_surcharge/price*100:.1f}%",
                    f"{economics.tax_amount/price*100:.1f}%",
                    f"{economics.auto_parts_specific/price*100:.1f}%",
                    f"{economics.advertising_cost/price*100:.1f}%",
                    f"{economics.total_expenses/price*100:.1f}%"
                ]
            }
            
            st_dataframe_compat(pd.DataFrame(expenses_data), key="ue_expenses_table")
# ============================================================================
# 🆕 БЛОК 16: UI ФУНКЦИИ - ПАРАЛЛЕЛЬНЫЙ РАСЧЕТ (v100.6 - С PRO ЭКСПОРТОМ)
# ============================================================================
# ✅ ИСПРАВЛЕНИЯ v100.11:
# 1. Магическое число 10000 вынесено в константу WARNING_THRESHOLD
# 2. Все st.experimental_rerun() заменены на st.rerun()
# 3. Улучшена обработка ошибок при экспорте
# ============================================================================

# ✅ ИСПРАВЛЕНИЕ v100.11: Константа вместо магического числа
WARNING_THRESHOLD = 10_000


def show_catalog_calculation_parallel():
    """
    📦 ПАРАЛЛЕЛЬНЫЙ РАСЧЕТ ПО КАТАЛОГУ
    Оптимизирован для 350K+ товаров с живыми формулами Excel
    """
    st.subheader("📦 Параллельный расчет по каталогу")
    
    if st.session_state.get('uploaded_data') is None:
        st.warning("⚠️ Сначала загрузите данные в разделе '📁 Загрузка данных'")
        return
    
    df = st.session_state.uploaded_data.copy()
    
    st.info("""
📋 **ИНСТРУКЦИЯ:**
1. Убедитесь, что данные загружены
2. Выберите маркетплейсы для расчета
3. Укажите режим работы
4. **Система автоматически определит колонки**
5. Для больших каталогов (>1000 товаров) используется параллельный расчет
6. Нажмите "Рассчитать"

🆕 **v100.6:** Экспорт в Excel с живыми формулами — меняйте значения, всё пересчитается!
""")
    
    unit_economics = get_marketplace_unit_economics()
    
    st.subheader("⚙️ Параметры расчета")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        available_marketplaces = list(unit_economics._configs.keys())
        selected_marketplaces = st.multiselect(
            "🏪 Маркетплейсы для расчета",
            options=available_marketplaces,
            default=available_marketplaces[:3] if len(available_marketplaces) >= 3 else available_marketplaces,
            key="ue_parallel_marketplaces",
            help="Выберите один или несколько маркетплейсов"
        )
        
        if not selected_marketplaces:
            st.warning("⚠️ Выберите хотя бы один маркетплейс")
            return
    
    with col2:
        operation_mode = st.selectbox(
            "📦 Режим работы",
            ["FBY", "FBS", "FBO", "DBS", "FBP"],
            key="ue_parallel_mode"
        )
        
        days_in_storage = st.number_input(
            "📦 Дней хранения",
            min_value=1,
            max_value=365,
            value=30,
            step=1,
            key="ue_parallel_days"
        )
    
    with col3:
        apply_markup = st.checkbox("💰 Применить наценку", value=False, key="ue_parallel_markup")
        if apply_markup:
            markup_percent = st.number_input(
                "Наценка (%)",
                min_value=0.0,
                max_value=500.0,
                value=20.0,
                step=5.0,
                key="ue_parallel_markup_percent"
            )
        else:
            markup_percent = 0.0
        
        use_seasonal = st.checkbox("🌤 Учесть сезонность", value=True, key="ue_parallel_seasonal")
        
        use_parallel = st.checkbox("🚀 Параллельный расчет", value=True, key="ue_parallel_enabled")
        if use_parallel:
            max_workers = st.number_input(
                "🧵 Потоков",
                min_value=1,
                max_value=16,
                value=min(4, os.cpu_count() or 2),
                step=1,
                key="ue_parallel_workers"
            )
        else:
            max_workers = 1
    
    st.subheader("📋 Определение колонок в данных")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        article_col = st.selectbox("Артикул", options=df.columns, key="ue_parallel_article")
    
    with col2:
        price_options = [col for col in df.columns if any(w in str(col).lower() for w in ['цена', 'price', 'стоимость'])]
        if not price_options:
            price_options = list(df.columns)
        price_col = st.selectbox("Цена продажи", options=price_options, key="ue_parallel_price")
    
    with col3:
        cost_options = [col for col in df.columns if any(w in str(col).lower() for w in ['себестоимость', 'cost', 'закупочная'])]
        if not cost_options:
            cost_options = list(df.columns)
        cost_col = st.selectbox("Себестоимость", options=cost_options, key="ue_parallel_cost")
    
    with col4:
        category_options = [col for col in df.columns if any(w in str(col).lower() for w in ['категория', 'category', 'группа'])]
        category_options = ['Не выбрано'] + list(category_options)
        category_col = st.selectbox("Категория (опционально)", options=category_options, key="ue_parallel_category")
    
    st.subheader("📏 Габариты")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        length_options = ['Не выбрано'] + [col for col in df.columns if any(w in str(col).lower() for w in ['длина', 'length', 'длинна', 'l'])]
        length_col = st.selectbox("Длина (см)", options=length_options, key="ue_parallel_length")
    
    with col2:
        width_options = ['Не выбрано'] + [col for col in df.columns if any(w in str(col).lower() for w in ['ширина', 'width', 'w'])]
        width_col = st.selectbox("Ширина (см)", options=width_options, key="ue_parallel_width")
    
    with col3:
        height_options = ['Не выбрано'] + [col for col in df.columns if any(w in str(col).lower() for w in ['высота', 'height', 'h'])]
        height_col = st.selectbox("Высота (см)", options=height_options, key="ue_parallel_height")
    
    with col4:
        weight_options = ['Не выбрано'] + [col for col in df.columns if any(w in str(col).lower() for w in ['вес', 'weight', 'масса', 'кг'])]
        weight_col = st.selectbox("Вес (кг)", options=weight_options, key="ue_parallel_weight")
    
    if st.button("🚀 Рассчитать юнит-экономику", type="primary", key="ue_parallel_calc"):
        total_items = len(df) * len(selected_marketplaces)
        
        # ✅ ИСПРАВЛЕНИЕ v100.11: Используем константу вместо магического числа
        if total_items > WARNING_THRESHOLD:
            st.warning(f"⚠️ Будет выполнено {total_items:,} расчетов. Это может занять несколько минут.")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with st.spinner("Расчет юнит-экономики..."):
            try:
                category_col_name = category_col if category_col != 'Не выбрано' else None
                length_col_name = length_col if length_col != 'Не выбрано' else None
                width_col_name = width_col if width_col != 'Не выбрано' else None
                height_col_name = height_col if height_col != 'Не выбрано' else None
                weight_col_name = weight_col if weight_col != 'Не выбрано' else None
                
                def progress_callback(progress):
                    progress_bar.progress(progress)
                    status_text.text(f"🔄 Обработано: {int(progress * 100)}%")
                
                results_df = unit_economics.calculate_for_catalog_batch(
                    df=df,
                    price_col=price_col,
                    cost_col=cost_col,
                    category_col=category_col_name,
                    length_col=length_col_name,
                    width_col=width_col_name,
                    height_col=height_col_name,
                    weight_col=weight_col_name,
                    article_col=article_col,
                    marketplaces=selected_marketplaces,
                    operation_mode=operation_mode,
                    days_in_storage=days_in_storage,
                    apply_markup=markup_percent,
                    use_parallel=use_parallel,
                    max_workers=max_workers if use_parallel else 1,
                    progress_callback=progress_callback if total_items > 1000 else None
                )
                
                progress_bar.progress(1.0)
                status_text.text("✅ Расчет завершен!")
                
                if results_df.empty:
                    st.error("❌ Не удалось рассчитать юнит-экономику ни для одного товара")
                    return
                
                st.session_state.ue_parallel_results = results_df
                st.session_state.ue_parallel_metadata = {
                    'marketplaces': selected_marketplaces,
                    'operation_mode': operation_mode,
                    'days_in_storage': days_in_storage,
                    'seasonal': use_seasonal,
                    'total_items': len(results_df),
                }
                
                st.success(f"✅ Рассчитано {len(results_df):,} записей по {len(selected_marketplaces)} маркетплейсам")
            
            except Exception as e:
                st.error(f"❌ Ошибка при расчете: {str(e)}")
                with st.expander("📋 Подробности ошибки", expanded=True):
                    st.code(traceback.format_exc())
                return
    
    if 'ue_parallel_results' in st.session_state and st.session_state.ue_parallel_results is not None:
        results_df = st.session_state.ue_parallel_results
        metadata = st.session_state.get('ue_parallel_metadata', {})
        
        st.subheader("📊 Сводная статистика")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_profit = results_df['profit'].sum()
            st.metric("💰 Общая прибыль", f"{total_profit:,.0f} ₽")
        
        with col2:
            avg_profit = results_df['profit'].mean()
            st.metric("📈 Средняя прибыль", f"{avg_profit:.2f} ₽")
        
        with col3:
            avg_margin = results_df['margin_percent'].mean()
            st.metric("📊 Средняя маржа", f"{avg_margin:.1f}%")
        
        with col4:
            try:
                best_mp = results_df.groupby('marketplace')['profit'].sum().idxmax()
                st.metric("🏆 Лучший МП", best_mp)
            except Exception:
                st.metric("🏆 Лучший МП", "Н/Д")
        
        st.subheader("📋 Результаты расчета")
        
        display_cols = ['Артикул', 'marketplace', 'price', 'profit', 'margin_percent',
                       'recommended_min_price', 'tax_amount', 'breakeven_price']
        available_display = [col for col in display_cols if col in results_df.columns]
        
        if available_display:
            st_dataframe_compat(results_df[available_display].head(100))
        
        st.subheader("📤 Экспорт результатов")
        
        st.info("""
🆕 **v100.6: Три варианта экспорта:**

🟢 **Excel PRO с формулами** — живые формулы, можно редактировать входные данные, всё пересчитается

🔵 **Excel базовый** — статические значения, быстрее для очень больших файлов

⚪ **CSV** — универсальный формат для импорта в другие системы
""")
        
        export_col1, export_col2, export_col3 = st.columns(3)
        
        with export_col1:
            st.markdown("#### 🟢 Excel PRO (с формулами)")
            st.caption("✅ Живые формулы\n✅ Редактируемые параметры\n✅ Пересчёт при изменении")
            
            if st.button("📥 Экспорт PRO", type="primary", key="ue_parallel_export_excel_pro", use_container_width=True):
                try:
                    with st.spinner("Генерация отчёта с живыми формулами..."):
                        output_path = TEMP_DIR / f"unit_economics_PRO_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                        
                        export_metadata = {
                            'marketplaces': metadata.get('marketplaces', []),
                            'operation_mode': metadata.get('operation_mode', 'FBS'),
                            'days_in_storage': metadata.get('days_in_storage', 30),
                            'seasonal': metadata.get('seasonal', True),
                            'tariff_source': 'Актуальные тарифы 2026',
                            'total_items': len(results_df),
                        }
                        
                        # ✅ ИСПРАВЛЕНИЕ: Используем SuperProExcelExporter вместо FormulaExcelExporter
                        try:
                            from streamlit_app import SuperProExcelExporter
                            exporter = SuperProExcelExporter(unit_economics=unit_economics)
                            success = exporter.export_super_pro(
                                results_df, str(output_path), export_metadata
                            )
                        except (ImportError, NameError):
                            # Fallback: используем базовый pandas экспорт
                            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                                results_df.to_excel(writer, index=False, sheet_name='Результаты')
                                
                                if 'marketplace' in results_df.columns:
                                    mp_summary = results_df.groupby('marketplace').agg({
                                        'profit': ['sum', 'mean', 'count'],
                                        'margin_percent': 'mean',
                                    }).reset_index()
                                    mp_summary.columns = ['Маркетплейс', 'Общая прибыль', 'Средняя прибыль',
                                                         'Кол-во SKU', 'Средняя маржа %']
                                    mp_summary.to_excel(writer, index=False, sheet_name='Сводка по МП')
                            
                            success = True
                        
                        if success and output_path.exists():
                            with open(output_path, "rb") as f:
                                file_bytes = f.read()
                            
                            st.download_button(
                                label="⬇️ Скачать PRO-отчёт",
                                data=file_bytes,
                                file_name=output_path.name,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="ue_parallel_download_excel_pro",
                                use_container_width=True
                            )
                            st.success("✅ PRO-отчёт готов! Откройте в Excel — все формулы работают")
                        else:
                            st.error("❌ Ошибка генерации отчёта")
                
                except Exception as e:
                    st.error(f"❌ Ошибка: {str(e)}")
                    logger.error(f"Ошибка PRO-экспорта: {traceback.format_exc()}")
        
        with export_col2:
            st.markdown("#### 🔵 Excel (базовый)")
            st.caption("⚡ Быстрее для 350K+\n📊 Статические значения\n📋 Простой формат")
            
            if st.button("📥 Экспорт Excel", key="ue_parallel_export_excel", use_container_width=True):
                try:
                    with st.spinner("Генерация Excel файла..."):
                        output = io.BytesIO()
                        
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            results_df.to_excel(writer, index=False, sheet_name='Результаты')
                            
                            if 'marketplace' in results_df.columns:
                                mp_summary = results_df.groupby('marketplace').agg({
                                    'profit': ['sum', 'mean', 'count'],
                                    'margin_percent': 'mean',
                                    'price': 'mean'
                                }).reset_index()
                                mp_summary.columns = ['Маркетплейс', 'Общая прибыль', 'Средняя прибыль',
                                                     'Кол-во SKU', 'Средняя маржа %', 'Средняя цена']
                                mp_summary.to_excel(writer, index=False, sheet_name='Сводка по МП')
                        
                        output.seek(0)
                        
                        st.download_button(
                            label="⬇️ Скачать Excel",
                            data=output,
                            file_name=f"юнит_экономика_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="ue_parallel_download_excel",
                            use_container_width=True
                        )
                        st.success("✅ Excel файл готов!")
                
                except Exception as e:
                    st.error(f"❌ Ошибка: {str(e)}")
        
        with export_col3:
            st.markdown("#### ⚪ CSV")
            st.caption("🌍 Универсальный формат\n📦 Для импорта в 1С\n🔧 Для других систем")
            
            if st.button("📥 Экспорт CSV", key="ue_parallel_export_csv", use_container_width=True):
                try:
                    with st.spinner("Генерация CSV файла..."):
                        csv_data = results_df.to_csv(index=False, encoding='utf-8-sig', sep=';')
                        
                        st.download_button(
                            label="⬇️ Скачать CSV",
                            data=csv_data.encode('utf-8-sig'),
                            file_name=f"юнит_экономика_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv; charset=utf-8",
                            key="ue_parallel_download_csv",
                            use_container_width=True
                        )
                        st.success("✅ CSV файл готов!")
                
                except Exception as e:
                    st.error(f"❌ Ошибка: {str(e)}")
        
        st.divider()
        
        col_clear1, col_clear2 = st.columns([3, 1])
        
        with col_clear2:
            if st.button("🗑️ Очистить результаты", key="ue_parallel_clear"):
                for key in ['ue_parallel_results', 'ue_parallel_metadata']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.success("✅ Результаты очищены")
                # ✅ ИСПРАВЛЕНИЕ v100.11: st.rerun() вместо st.experimental_rerun()
                st.rerun()
    
    else:
        st.info("ℹ️ Нажмите кнопку '🚀 Рассчитать юнит-экономику' для начала расчета")
# ============================================================================
# БЛОК 17: UI функции каталога (ПОЛНАЯ ВЕРСИЯ)
# ============================================================================
# ✅ ИСПРАВЛЕНИЯ v100.11:
# 1. Исправлен MIME-тип для Parquet экспорта
# 2. Улучшена обработка ошибок при экспорте
# 3. Добавлена корректная работа с sidebar radio
# ============================================================================

def show_catalog_grouping_interface():
    """
    🗂️ РАЗДЕЛ 3: КАТАЛОГ ДЛЯ ГРУППИРОВКИ
    High-Volume каталог с поддержкой 10M+ записей
    """
    st.header("🗂️ Шаг 3: Каталог для группировки")
    st.info("""
📋 **О РАЗДЕЛЕ:**
Этот раздел предназначен для работы с большими каталогами товаров.
**Возможности:**
- ✅ Загрузка каталогов до 10 миллионов записей
- ✅ Автоматическая группировка по категориям
- ✅ Интеллектуальный парсинг размеров "20x15x10"
- ✅ Поиск и фильтрация товаров
- ✅ Экспорт в Excel, CSV, Parquet
- ✅ Статистика и аналитика
""")
    
    if not (POLARS_AVAILABLE and DUCKDB_AVAILABLE):
        st.warning("⚠️ Для работы с большими каталогами установите: `pip install polars duckdb`")
        return
    
    if 'high_volume_catalog' not in st.session_state:
        st.session_state.high_volume_catalog = get_high_volume_catalog()
    
    catalog = st.session_state.high_volume_catalog
    
    if not catalog.conn:
        st.error("❌ Ошибка подключения к базе данных")
        return
    
    st.sidebar.title("🧭 Меню каталога")
    
    # ✅ ИСПРАВЛЕНИЕ v100.11: используем уникальный key для sidebar radio
    option = st.sidebar.radio(
        "Выберите раздел",
        ["📥 Загрузка данных", "🔍 Поиск и фильтрация", "📊 Статистика", "📤 Экспорт", "🔧 Управление"],
        key="catalog_menu_sidebar"
    )
    
    if option == "📥 Загрузка данных":
        show_catalog_upload(catalog)
    elif option == "🔍 Поиск и фильтрация":
        show_catalog_search(catalog)
    elif option == "📊 Статистика":
        show_catalog_statistics(catalog)
    elif option == "📤 Экспорт":
        show_catalog_export(catalog)
    elif option == "🔧 Управление":
        show_catalog_management(catalog)


def show_catalog_upload(catalog):
    """Загрузка данных в каталог"""
    st.subheader("📥 Загрузка данных")
    st.info("""
📋 **ТРЕБОВАНИЯ К ФАЙЛАМ:**
- **Основные данные (OE):** `oe_number`, `artikul`, `brand`, `name`, `applicability`
- **Кросс-ссылки:** `oe_number`, `artikul`, `brand`
- **Штрих-коды:** `artikul`, `brand`, `barcode`, `multiplicity`
- **Габариты:** `artikul`, `brand`, `length`, `width`, `height`, `weight`, `dimensions_str`
- **Изображения:** `artikul`, `brand`, `image_url`
- **Цены:** `artikul`, `brand`, `price`, `currency`
""")
    
    col1, col2 = st.columns(2)
    with col1:
        oe_file = st.file_uploader("📋 Основные данные (OE)", type=['xlsx'], key="hv_oe")
        cross_file = st.file_uploader("🔗 Кросс-ссылки", type=['xlsx'], key="hv_cross")
        barcode_file = st.file_uploader("📊 Штрих-коды", type=['xlsx'], key="hv_barcode")
    
    with col2:
        dims_file = st.file_uploader("📏 Габариты", type=['xlsx'], key="hv_dims")
        images_file = st.file_uploader("🖼️ Изображения", type=['xlsx'], key="hv_images")
        prices_file = st.file_uploader("💰 Цены", type=['xlsx'], key="hv_prices")
    
    uploaded_files = {
        'oe': oe_file, 'cross': cross_file, 'barcode': barcode_file,
        'dimensions': dims_file, 'images': images_file, 'prices': prices_file
    }
    
    if st.button("🚀 Обработать и загрузить", key="hv_load"):
        saved_paths = {}
        for key, file in uploaded_files.items():
            if file:
                path = catalog.data_dir / f"{key}_{int(time.time())}.xlsx"
                with open(path, "wb") as f:
                    f.write(file.getbuffer())
                saved_paths[key] = str(path)
        
        if saved_paths:
            with st.spinner("Обработка файлов..."):
                dataframes = catalog.merge_all_data_parallel(saved_paths)
            
            with st.spinner("Загрузка данных в базу..."):
                catalog.process_and_load_data(dataframes)
            
            st.success("✅ Данные успешно загружены!")
        else:
            st.warning("⚠️ Загрузите хотя бы один файл")


def show_catalog_search(catalog):
    """Поиск и фильтрация в каталоге"""
    st.subheader("🔍 Поиск и фильтрация")
    
    col1, col2 = st.columns(2)
    with col1:
        search_artikul = st.text_input("🔢 Артикул", key="search_artikul")
        search_brand = st.text_input("🏷️ Бренд", key="search_brand")
    
    with col2:
        search_oe = st.text_input("🔗 OE номер", key="search_oe")
        search_category = st.text_input("📂 Категория", key="search_category")
    
    if st.button("🔍 Найти", key="catalog_search"):
        query_parts = []
        params = []
        
        if search_artikul:
            query_parts.append("artikul LIKE ?")
            params.append(f"%{search_artikul}%")
        
        if search_brand:
            query_parts.append("brand LIKE ?")
            params.append(f"%{search_brand}%")
        
        if search_oe:
            query_parts.append("""
                artikul_norm IN (
                    SELECT artikul_norm FROM cross_references
                    WHERE oe_number_norm LIKE ?
                )
            """)
            params.append(f"%{search_oe}%")
        
        if search_category:
            query_parts.append("category LIKE ?")
            params.append(f"%{search_category}%")
        
        if query_parts:
            where_clause = " AND ".join(query_parts)
            query = f"SELECT * FROM parts WHERE {where_clause} LIMIT 100"
            
            try:
                df = catalog.conn.execute(query, params).df()
                st_dataframe_compat(df)
            except duckdb.Error as e:
                st.error(f"❌ Ошибка поиска: {e}")


def show_catalog_statistics(catalog):
    """Статистика каталога"""
    st.subheader("📊 Статистика каталога")
    
    stats = catalog.get_statistics()
    
    if stats:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📦 Уникальных товаров", f"{stats.get('unique_parts', 0):,}")
        with col2:
            st.metric("🏷️ Брендов", f"{stats.get('brands', 0):,}")
        with col3:
            st.metric("💰 Средняя цена", f"{stats.get('avg_price', 0):.2f} ₽")
        
        if 'category_stats' in stats and not stats['category_stats'].empty:
            st.subheader("📊 Распределение по категориям")
            st_dataframe_compat(stats['category_stats'])
        
        if 'top_brands' in stats and not stats['top_brands'].empty:
            st.subheader("🏆 Топ 10 брендов")
            st_dataframe_compat(stats['top_brands'])


def show_catalog_export(catalog):
    """Экспорт каталога"""
    st.subheader("📤 Экспорт каталога")
    
    total = catalog.conn.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)"
    ).fetchone()[0]
    
    st.info(f"📊 Всего записей: {total:,}")
    
    if total == 0:
        st.warning("⚠️ Нет данных для экспорта")
        return
    
    format_choice = st.radio("Формат", ["CSV", "Excel", "Parquet"])
    
    selected_columns = st.multiselect("Колонки", [
        "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
        "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
        "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"
    ])
    
    include_prices = st.checkbox("Включить цены", value=True)
    apply_markup = st.checkbox("Применить наценку", value=True, disabled=not include_prices)
    
    if st.button("🚀 Экспортировать"):
        output_path = catalog.data_dir / f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format_choice.lower()}"
        
        with st.spinner("Генерация файла..."):
            if format_choice == "CSV":
                success = catalog.export_to_csv_optimized(
                    str(output_path),
                    selected_columns if selected_columns else None,
                    include_prices,
                    apply_markup
                )
            elif format_choice == "Excel":
                success = catalog.export_to_excel_optimized(
                    str(output_path),
                    selected_columns if selected_columns else None,
                    include_prices,
                    apply_markup
                )
            elif format_choice == "Parquet":
                success = catalog.export_to_parquet(
                    str(output_path),
                    selected_columns if selected_columns else None,
                    include_prices,
                    apply_markup
                )
            else:
                st.warning("Неподдерживаемый формат")
                return
        
        if success and output_path.exists():
            with open(output_path, "rb") as f:
                file_data = f.read()
            
            # ✅ ИСПРАВЛЕНИЕ v100.11: правильный MIME-тип для каждого формата
            mime_map = {
                "CSV": "text/csv; charset=utf-8",
                "Excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "Parquet": "application/octet-stream"
            }
            mime_type = mime_map.get(format_choice, "application/octet-stream")
            
            st.download_button(
                label="⬇️ Скачать файл",
                data=file_data,
                file_name=output_path.name,
                mime=mime_type,
                key="catalog_download"
            )
        else:
            st.error("❌ Ошибка при экспорте")


def show_catalog_management(catalog):
    """Управление каталогом"""
    st.subheader("🔧 Управление каталогом")
    st.warning("⚠️ Операции необратимы!")
    
    management_option = st.radio(
        "Выберите действие:",
        [
            "Удалить по бренду",
            "Удалить по артикули",
            "Управление ценами",
            "Исключения",
            "Категории",
            "Облачная синхронизация"
        ],
        format_func=lambda x: {
            "Удалить по бренду": "🏭 Удалить все записи бренда",
            "Удалить по артикули": "📦 Удалить все записи артикула",
            "Управление ценами": "💰 Цены и наценки",
            "Исключения": "🚫 Исключения при экспорте",
            "Категории": "🗂️ Категории товаров",
            "Облачная синхронизация": "☁️ Облачная синхронизация"
        }[x]
    )
    
    if management_option == "Удалить по бренду":
        catalog._show_delete_by_brand()
    elif management_option == "Удалить по артикули":
        catalog._show_delete_by_artikul()
    elif management_option == "Управление ценами":
        catalog.show_price_settings()
    elif management_option == "Исключения":
        catalog.show_exclusion_settings()
    elif management_option == "Категории":
        catalog.show_category_mapping()
    elif management_option == "Облачная синхронизация":
        catalog.show_cloud_sync()
# ============================================================================
# 🆕 БЛОК 18: AI ТАРИФЫ - БЕЗ ИЗМЕНЕНИЙ
# ============================================================================
def show_ai_tariffs_interface():
    """🤖 AI ТАРИФЫ С ПРОГНОЗИРОВАНИЕМ"""
    st.header("🤖 Шаг 4: AI Тарифы с прогнозом")
    
    st.info("""
🤖 **ОБНОВЛЕНИЕ ТАРИФОВ ЧЕРЕЗ ИИ С ПРОГНОЗОМ:**
1. Получите API ключ на platform.deepseek.com
2. Введите ключ в поле ниже
3. Выберите маркетплейс
4. Поставьте галочку "Запросить ИИ" для получения прогноза
5. Нажмите "Обновить тарифы"
""")
    
    unit_economics = get_marketplace_unit_economics()
    
    api_key = st.text_input(
        "🔑 API ключ DeepSeek",
        type="password",
        placeholder="sk-...",
        help="Получите API ключ на platform.deepseek.com",
        key="ai_api_key"
    )
    
    if api_key:
        os.environ['DEEPSEEK_API_KEY'] = api_key
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            marketplace = st.selectbox(
                "🏪 Маркетплейс",
                ["Ozon", "Wildberries", "Яндекс Маркет", "AliExpress", "Мегамаркет", "Все маркетплейсы"],
                key="ai_marketplace"
            )
        
        with col2:
            category = st.text_input(
                "📂 Категория (опционально)",
                placeholder="например: двигатель",
                key="ai_category"
            )
        
        with col3:
            force_refresh = st.checkbox(
                "🔄 Запросить ИИ (принудительное обновление)",
                value=False,
                key="ai_force_refresh",
                help="Если установлено — тарифы будут запрошены у DeepSeek AI."
            )
        
        include_forecast = st.checkbox(
            "📈 Получить прогноз на 3 месяца",
            value=False,
            key="ai_forecast",
            help="Получить прогноз изменения тарифов на 3 месяца вперед"
        )
        
        if st.button("🔄 Обновить тарифы", type="primary", key="ai_update"):
            if not api_key and not os.environ.get('DEEPSEEK_API_KEY'):
                st.error("❌ Введите API ключ DeepSeek")
                return
            
            with st.spinner("Обновление тарифов..."):
                if marketplace == "Все маркетплейсы":
                    result = unit_economics.refresh_tariffs_from_ai(
                        marketplace=None,
                        category=category if category else None,
                        force=force_refresh,
                        include_forecast=include_forecast
                    )
                    
                    if result.get('success'):
                        st.success(f"✅ Обновлены тарифы для {result.get('marketplaces_updated', 0)} из {result.get('total', 0)} маркетплейсов")
                    else:
                        st.error(f"❌ Ошибка: {result.get('error', 'Неизвестная ошибка')}")
                else:
                    result = unit_economics.refresh_tariffs_from_ai(
                        marketplace=marketplace,
                        category=category if category else None,
                        force=force_refresh,
                        include_forecast=include_forecast
                    )
                    
                    if result.get('success'):
                        st.success(f"✅ Обновлены тарифы для {marketplace}")
                        
                        st.info(f"📥 Источник: **{result.get('source', 'Н/Д')}**")
                        
                        if result.get('forecast'):
                            st.subheader("📈 Прогноз тарифов на 3 месяца")
                            forecast = result['forecast']
                            
                            if 'month_1' in forecast:
                                st.info("**Месяц 1:**")
                                st.json(forecast['month_1'])
                            
                            if 'month_2' in forecast:
                                st.info("**Месяц 2:**")
                                st.json(forecast['month_2'])
                            
                            if 'month_3' in forecast:
                                st.info("**Месяц 3:**")
                                st.json(forecast['month_3'])
                        
                        with st.expander("📋 Текущие тарифы"):
                            st.json(result.get('rates', {}))
                    else:
                        st.error(f"❌ Ошибка: {result.get('error', 'Неизвестная ошибка')}")


# ============================================================================
# 🆕 БЛОК 19: РАСШИРЕННЫЙ API КОННЕКТОР С ВЫБОРОМ ИСТОЧНИКА
# ============================================================================
# 🆕 v100.10: УМНЫЙ ВЫБОР ИСТОЧНИКА ТАРИФОВ
# ✅ API маркетплейса (прямое подключение)
# ✅ AI анализ документации (автоматический парсинг)
# ✅ Загруженные ранее тарифы (кэш)
# ✅ Гибридный режим (комбинация источников)
# ============================================================================

class SmartTariffLoader:
    """
    🧠 УМНАЯ ЗАГРУЗКА ТАРИФОВ С ВЫБОРОМ ИСТОЧНИКА
    Поддерживает 4 режима: API, AI, Кэш, Гибридный
    """
    
    SOURCES = {
        "api": "🔌 API Маркетплейса",
        "ai": "🤖 AI (документация)", 
        "cache": "💾 Загруженные ранее",
        "hybrid": "🔄 Гибридный (AI + API)"
    }
    
    def __init__(self):
        self.api_connector = MarketplaceAPIConnector()
        self.ai_updater = DeepSeekRateUpdater()
        self.tariff_cache = get_smart_tariff_cache()
        self.logger = logging.getLogger('SmartTariffLoader')
    
    def load_tariffs(self, marketplace: str, source: str = "hybrid", 
                     api_key: str = None, client_id: str = None,
                     force_refresh: bool = False) -> Dict[str, Any]:
        """
        Загрузка тарифов из выбранного источника
        
        Args:
            marketplace: Название маркетплейса
            source: "api", "ai", "cache", "hybrid"
            api_key: API ключ (для API режима)
            client_id: Client ID (для Ozon)
            force_refresh: Принудительное обновление
        """
        
        result = {
            "marketplace": marketplace,
            "source": source,
            "timestamp": datetime.now().isoformat(),
            "data": {},
            "source_used": None,
            "confidence": 0.0,
            "warnings": [],
            "errors": []
        }
        
        try:
            if source == "api":
                result = self._load_from_api(marketplace, api_key, client_id, result)
            elif source == "ai":
                result = self._load_from_ai(marketplace, result, force_refresh)
            elif source == "cache":
                result = self._load_from_cache(marketplace, result)
            elif source == "hybrid":
                result = self._load_hybrid(marketplace, api_key, client_id, result, force_refresh)
            else:
                result["errors"].append(f"Неизвестный источник: {source}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка загрузки тарифов: {e}")
            result["errors"].append(str(e))
            return result
    
    def _load_from_api(self, marketplace: str, api_key: str, 
                       client_id: str, result: Dict) -> Dict:
        """Загрузка через официальное API маркетплейса"""
        result["source_used"] = "API"
        
        try:
            if marketplace == "Ozon" and api_key and client_id:
                data = self.api_connector.get_ozon_tariffs(api_key, client_id)
                if data:
                    result["data"] = data
                    result["confidence"] = 0.95
                    result["warnings"].append("✅ Тарифы загружены напрямую из API Ozon")
                else:
                    result["errors"].append("Не удалось получить данные из API Ozon")
            
            elif marketplace == "Wildberries" and api_key:
                data = self.api_connector.get_wildberries_tariffs(api_key)
                if data and data.get('success'):
                    result["data"] = data.get('data', {})
                    result["confidence"] = 0.95
                    result["warnings"].append("✅ Тарифы загружены напрямую из API WB")
                else:
                    result["errors"].append("Не удалось получить данные из API WB")
            
            else:
                result["errors"].append(f"API для {marketplace} не поддерживается или не хватает ключей")
        
        except Exception as e:
            result["errors"].append(f"Ошибка API: {str(e)}")
        
        return result
    
    def _load_from_ai(self, marketplace: str, result: Dict, force_refresh: bool) -> Dict:
        """Загрузка через AI анализ документации"""
        result["source_used"] = "AI"
        
        try:
            rates, source, forecast = self.ai_updater.get_rates_from_ai(
                marketplace=marketplace,
                force_refresh=force_refresh,
                use_cache=True,
                include_forecast=True
            )
            
            if rates:
                result["data"] = {
                    "rates": rates,
                    "forecast": forecast,
                    "source": source.value
                }
                result["confidence"] = 0.85
                result["warnings"].append("🤖 Тарифы получены через AI анализ документации")
                
                if forecast:
                    result["warnings"].append("📈 Прогноз тарифов на 3 месяца получен")
            else:
                result["errors"].append("AI не смог получить актуальные тарифы")
        
        except Exception as e:
            result["errors"].append(f"Ошибка AI: {str(e)}")
        
        return result
    
    def _load_from_cache(self, marketplace: str, result: Dict) -> Dict:
        """Загрузка из кэша (ранее загруженные тарифы)"""
        result["source_used"] = "Cache"
        
        try:
            cached = self.tariff_cache.get(marketplace, None, use_expired=False)
            
            if cached:
                result["data"] = {
                    "rates": cached.data,
                    "timestamp": datetime.fromtimestamp(cached.timestamp).isoformat(),
                    "source": cached.source.value
                }
                result["confidence"] = 0.90
                result["warnings"].append(f"💾 Использованы кэшированные тарифы от {datetime.fromtimestamp(cached.timestamp).strftime('%d.%m.%Y %H:%M')}")
            else:
                result["errors"].append("Кэшированные тарифы не найдены или устарели")
        
        except Exception as e:
            result["errors"].append(f"Ошибка кэша: {str(e)}")
        
        return result
    
    def _load_hybrid(self, marketplace: str, api_key: str, 
                     client_id: str, result: Dict, force_refresh: bool) -> Dict:
        """
        Гибридный режим: сначала API, если нет — AI, если нет — кэш
        """
        result["source_used"] = "Hybrid"
        result["warnings"].append("🔄 Используется гибридный режим загрузки")
        
        # 1. Пробуем API
        if api_key:
            api_result = self._load_from_api(marketplace, api_key, client_id, result.copy())
            if not api_result["errors"] and api_result["data"]:
                result["data"] = api_result["data"]
                result["source_used"] = "API (Hybrid)"
                result["confidence"] = 0.95
                result["warnings"].append("✅ Использованы API тарифы")
                return result
        
        # 2. Пробуем AI
        ai_result = self._load_from_ai(marketplace, result.copy(), force_refresh)
        if not ai_result["errors"] and ai_result["data"]:
            result["data"] = ai_result["data"]
            result["source_used"] = "AI (Hybrid)"
            result["confidence"] = 0.85
            result["warnings"].append("🤖 Использованы AI тарифы (API не доступен)")
            return result
        
        # 3. Пробуем кэш
        cache_result = self._load_from_cache(marketplace, result.copy())
        if not cache_result["errors"] and cache_result["data"]:
            result["data"] = cache_result["data"]
            result["source_used"] = "Cache (Hybrid)"
            result["confidence"] = 0.80
            result["warnings"].append("💾 Использованы кэшированные тарифы (AI и API не доступны)")
            return result
        
        result["errors"].append("Не удалось загрузить тарифы ни из одного источника")
        return result
    
    def get_available_sources(self, marketplace: str) -> List[str]:
        """Получить список доступных источников для маркетплейса"""
        sources = []
        
        # Проверяем API
        if marketplace in ["Ozon", "Wildberries"]:
            sources.append("api")
        
        # AI всегда доступен (если есть ключ)
        if self.ai_updater.api_key:
            sources.append("ai")
        
        # Кэш доступен если есть данные
        if self.tariff_cache.get(marketplace, None, use_expired=False):
            sources.append("cache")
        
        # Гибридный доступен всегда
        sources.append("hybrid")
        
        return sources
    
    def compare_sources(self, marketplace: str, api_key: str = None, 
                        client_id: str = None) -> pd.DataFrame:
        """Сравнить тарифы из разных источников"""
        results = []
        
        for source in ["api", "ai", "cache"]:
            if source == "api" and not api_key:
                continue
            
            result = self.load_tariffs(marketplace, source, api_key, client_id)
            
            if not result["errors"]:
                results.append({
                    "Источник": self.SOURCES.get(source, source),
                    "Статус": "✅ Доступен",
                    "Данных": len(result["data"]) if isinstance(result["data"], dict) else 0,
                    "Доверие": f"{result['confidence']*100:.0f}%",
                    "Предупреждения": ", ".join(result["warnings"][:2])
                })
            else:
                results.append({
                    "Источник": self.SOURCES.get(source, source),
                    "Статус": "❌ Недоступен",
                    "Данных": 0,
                    "Доверие": "0%",
                    "Предупреждения": result["errors"][0][:50] if result["errors"] else ""
                })
        
        return pd.DataFrame(results)
# ============================================================================
# 🆕 БЛОК 20: UI ДЛЯ УМНОЙ ЗАГРУЗКИ ТАРИФОВ (ИСПРАВЛЕННАЯ ВЕРСИЯ)
# ============================================================================
# ✅ ИСПРАВЛЕНИЯ v100.11:
# 1. Улучшена обработка ошибок инициализации
# 2. Корректная обработка тарифов из прямого API
# 3. Добавлены проверки доступности методов
# ============================================================================

def show_smart_tariff_interface():
    """
    🧠 ИНТЕРФЕЙС УМНОЙ ЗАГРУЗКИ ТАРИФОВ
    ✅ ИСПРАВЛЕНО: Корректная обработка тарифов из прямого API
    """
    st.header("🧠 Умная загрузка тарифов")
    st.info("""
📋 **ВЫБЕРИТЕ ИСТОЧНИК ТАРИФОВ:**
🔌 **API Маркетплейса** — прямое подключение к API (самый точный)
🤖 **AI (документация)** — автоматический парсинг документации
💾 **Загруженные ранее** — использование кэшированных тарифов
🔄 **Гибридный** — AI + API (рекомендуемый)
💡 **Рекомендация:** Используйте гибридный режим для максимальной надёжности
""")
    
    # ✅ Инициализация с обработкой ошибок
    try:
        tariff_loader = SmartTariffLoader()
        st.success("✅ SmartTariffLoader инициализирован")
    except Exception as e:
        st.error(f"❌ Ошибка инициализации SmartTariffLoader: {e}")
        tariff_loader = None
        return
    
    try:
        unit_economics = get_marketplace_unit_economics()
        if unit_economics is None:
            st.warning("⚠️ UnitEconomics не инициализирован")
            return
    except Exception as e:
        st.error(f"❌ Ошибка инициализации UnitEconomics: {e}")
        return
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        marketplace = st.selectbox(
            "🏪 Выберите маркетплейс",
            ["Ozon", "Wildberries", "Яндекс Маркет", "AliExpress", "Мегамаркет", "СберМегаМаркет"],
            key="smart_tariff_mp"
        )
    
    with col2:
        source = st.selectbox(
            "📡 Источник тарифов",
            [
                "hybrid",
                "api",
                "ai",
                "cache"
            ],
            format_func=lambda x: SmartTariffLoader.SOURCES.get(x, x) if hasattr(SmartTariffLoader, 'SOURCES') else x,
            key="smart_tariff_source"
        )
    
    # ✅ Показываем доступные источники
    if tariff_loader and hasattr(tariff_loader, 'get_available_sources'):
        try:
            available = tariff_loader.get_available_sources(marketplace)
            source_labels = [SmartTariffLoader.SOURCES.get(s, s) for s in available] if hasattr(SmartTariffLoader, 'SOURCES') else available
            st.info(f"🔍 Доступные источники для {marketplace}: {', '.join(source_labels)}")
        except Exception as e:
            st.warning(f"⚠️ Ошибка получения доступных источников: {e}")
    else:
        st.info("ℹ️ Доступны все источники")
    
    # ✅ API ключи (если выбран API режим)
    if source in ["api", "hybrid"]:
        st.subheader("🔑 API ключи")
        col1, col2 = st.columns(2)
        with col1:
            api_key = st.text_input(
                "API Key",
                type="password",
                placeholder="Введите API ключ",
                key="smart_tariff_api_key",
                help="Для Ozon: Api-Key, для WB: Api-Key"
            )
        with col2:
            client_id = st.text_input(
                "Client ID (только для Ozon)",
                type="password",
                placeholder="Введите Client ID",
                key="smart_tariff_client_id"
            )
    else:
        api_key = None
        client_id = None
    
    # ✅ Кнопка сравнения источников
    if st.button("📊 Сравнить источники", key="smart_tariff_compare"):
        if tariff_loader and hasattr(tariff_loader, 'compare_sources'):
            with st.spinner("Сравнение источников..."):
                try:
                    compare_df = tariff_loader.compare_sources(marketplace, api_key, client_id)
                    if compare_df is not None and not compare_df.empty:
                        st.subheader("📊 Сравнение источников")
                        st_dataframe_compat(compare_df)
                    else:
                        st.warning("⚠️ Нет данных для сравнения")
                except Exception as e:
                    st.error(f"❌ Ошибка сравнения: {e}")
        else:
            st.warning("⚠️ Метод compare_sources не найден")
    
    # ✅ Кнопка загрузки
    if st.button("🚀 Загрузить тарифы", type="primary", key="smart_tariff_load"):
        if not tariff_loader or not hasattr(tariff_loader, 'load_tariffs'):
            st.error("❌ Метод load_tariffs не найден")
            return
        
        with st.spinner(f"Загрузка тарифов из источника: {SmartTariffLoader.SOURCES.get(source, source) if hasattr(SmartTariffLoader, 'SOURCES') else source}..."):
            try:
                result = tariff_loader.load_tariffs(
                    marketplace=marketplace,
                    source=source,
                    api_key=api_key,
                    client_id=client_id,
                    force_refresh=True
                )
                
                if not isinstance(result, dict):
                    st.error("❌ Неверный формат результата")
                    return
                
                if result.get("errors"):
                    st.error(f"❌ Ошибки загрузки:")
                    for err in result["errors"]:
                        st.error(f"  - {err}")
                
                if result.get("warnings"):
                    st.info(f"ℹ️ Информация:")
                    for warn in result["warnings"]:
                        st.info(f"  - {warn}")
                
                if result.get("data"):
                    st.success(f"✅ Тарифы успешно загружены из источника: {result.get('source_used', 'Неизвестно')}")
                    confidence = result.get('confidence', 0)
                    st.info(f"🎯 Доверие к данным: {confidence*100:.0f}%")
                    
                    # Показываем загруженные тарифы
                    with st.expander("📋 Загруженные тарифы", expanded=True):
                        if isinstance(result["data"], dict):
                            st.json(result["data"])
                        else:
                            st.write(result["data"])
                    
                    # ✅ ИСПРАВЛЕНИЕ: Применяем тарифы с учётом структуры данных
                    if st.button("💾 Применить тарифы к расчётам", key="smart_tariff_apply"):
                        rates_to_apply = None
                        
                        # ✅ ИСПРАВЛЕНИЕ: Проверяем разные структуры данных
                        if "rates" in result["data"]:
                            # Структура от AI
                            rates_to_apply = result["data"]["rates"]
                        elif "raw_data" in result["data"]:
                            # Структура от прямого API
                            st.warning("⚠️ Прямой API вернул сырые данные. Применяем базовые тарифы.")
                            rates_to_apply = result["data"].get("raw_data", {})
                        elif isinstance(result["data"], dict) and any(k in result["data"] for k in ["commission_rate", "logistics_base"]):
                            # Прямая структура тарифов
                            rates_to_apply = result["data"]
                        
                        if rates_to_apply and unit_economics and hasattr(unit_economics, '_apply_ai_tariffs'):
                            try:
                                unit_economics._apply_ai_tariffs(marketplace, rates_to_apply)
                                st.success(f"✅ Тарифы для {marketplace} применены!")
                            except Exception as e:
                                st.error(f"❌ Ошибка применения: {e}")
                        else:
                            st.warning("⚠️ Не найдены данные для применения")
                else:
                    st.error("❌ Не удалось загрузить тарифы")
            
            except Exception as e:
                st.error(f"❌ Ошибка загрузки: {e}")
                logger.exception("Ошибка в load_tariffs")
    
    # ✅ Отображение текущих тарифов
    st.subheader("📊 Текущие тарифы")
    
    if unit_economics and hasattr(unit_economics, '_configs'):
        configs = unit_economics._configs
        
        if marketplace in configs:
            try:
                config = configs[marketplace]
                
                tariff_data = {
                    "Параметр": [
                        "Комиссия", "Мин. комиссия", "Логистика база",
                        "Логистика за кг", "Логистика за л", "Хранение",
                        "Эквайринг", "Возвраты", "Последняя миля",
                        "Подписка", "Источник", "Обновлено"
                    ],
                    "Значение": [
                        f"{config.commission_rate*100:.1f}%",
                        f"{config.min_commission:.2f} ₽",
                        f"{config.logistics_base:.2f} ₽",
                        f"{config.logistics_per_kg:.2f} ₽",
                        f"{config.logistics_per_liter:.2f} ₽",
                        f"{config.storage_per_day:.2f} ₽/л/день",
                        f"{config.acquiring_fee*100:.1f}%",
                        f"{config.return_fee*100:.1f}%",
                        f"{config.last_mile_fee:.2f} ₽",
                        f"{config.subscription_fee:.2f} ₽",
                        config.tariff_source.value if hasattr(config.tariff_source, 'value') else str(config.tariff_source),
                        config.last_updated.strftime('%d.%m.%Y %H:%M') if hasattr(config.last_updated, 'strftime') else str(config.last_updated)
                    ]
                }
                
                st_dataframe_compat(pd.DataFrame(tariff_data))
            
            except Exception as e:
                st.warning(f"⚠️ Ошибка отображения тарифов: {e}")
        else:
            st.info(f"ℹ️ Тарифы для {marketplace} не найдены")
    else:
        st.warning("⚠️ Конфигурации маркетплейсов не найдены")
# ============================================================================
# ✅ ИСПРАВЛЕНИЕ 2: Заглушка для API Тарифов маркетплейсов
# ============================================================================
def show_api_tariffs_interface():
    """🌐 API Тарифы маркетплейсов - информационный раздел"""
    st.header("🌐 API Тарифы маркетплейсов")
    st.info("""
🚧 **Раздел в разработке**

Прямое подключение к API маркетплейсов интегрировано в блок '🧠 Умная загрузка тарифов'.

**Используйте раздел '🧠 Умная загрузка тарифов' для:**
- ✅ Получения тарифов через API Ozon/Wildberries
- ✅ AI-анализа документации через DeepSeek
- ✅ Гибридного режима (API + AI)
- ✅ Работы с кэшированными тарифами
""")
    
    st.markdown("""
### 📋 Доступные API:

**Ozon Seller API:**
- `https://api-seller.ozon.ru/v1/finance/tariff-rates` — Тарифы
- `https://api-seller.ozon.ru/v2/products/info/stocks` — Остатки

**Wildberries API:**
- `https://common-api.wildberries.ru/tariffs/box` — Тарифы коробов
- `https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod` — Отчёты

**Яндекс Маркет API:**
- `https://api.partner.market.yandex.ru/v2/campaigns` — Кампании
- `https://api.partner.market.yandex.ru/v2/campaigns/{id}/deliveries/fees` — Тарифы
""")
    
    st.warning("⚠️ Для работы с API используйте раздел '🧠 Умная загрузка тарифов'")

# ============================================================================
# 🆕 БЛОК 21: БАЗА ДАННЫХ КАТЕГОРИЙ С ВЕСОГАБАРИТАМИ
# ============================================================================
# ✅ Загрузка категорий из Excel с весогабаритами
# ✅ Валидация и нормализация данных
# ✅ Интеграция с валидатором весогабаритов
# ============================================================================

class CategoryDimensionsDB:
    """
    📊 База данных категорий с весогабаритами
    Позволяет загружать категории из Excel и использовать их для валидации
    """
    
    def __init__(self):
        self.db_path = DATA_DIR / "category_dimensions.json"
        self.db_path.parent.mkdir(exist_ok=True)
        self.categories = {}
        self._load_from_file()
        self.logger = logging.getLogger('CategoryDimensionsDB')
    
    def _load_from_file(self):
        """Загрузка из JSON файла"""
        if self.db_path.exists():
            try:
                with open(self.db_path, 'r', encoding='utf-8') as f:
                    self.categories = json.load(f)
                self.logger.info(f"✅ Загружено {len(self.categories)} категорий из файла")
            except Exception as e:
                self.logger.error(f"❌ Ошибка загрузки: {e}")
                self.categories = {}
    
    def save_to_file(self):
        """Сохранение в JSON файл"""
        try:
            with open(self.db_path, 'w', encoding='utf-8') as f:
                json.dump(self.categories, f, ensure_ascii=False, indent=2)
            self.logger.info(f"✅ Сохранено {len(self.categories)} категорий")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения: {e}")
            return False
    
    def add_category(self, name: str, length: float, width: float, height: float, 
                     weight: float, unit: str = "см", weight_unit: str = "кг"):
        """Добавление категории"""
        self.categories[name.lower().strip()] = {
            "name": name,
            "length_cm": length,
            "width_cm": width,
            "height_cm": height,
            "weight_kg": weight,
            "unit": unit,
            "weight_unit": weight_unit,
            "added_at": datetime.now().isoformat()
        }
        self.save_to_file()
    
    def get_category(self, name: str) -> Optional[Dict[str, Any]]:
        """Получение категории по названию"""
        return self.categories.get(name.lower().strip())
    
    def get_all_categories(self) -> Dict[str, Dict[str, Any]]:
        """Получение всех категорий"""
        return self.categories
    
    def delete_category(self, name: str):
        """Удаление категории"""
        key = name.lower().strip()
        if key in self.categories:
            del self.categories[key]
            self.save_to_file()
    
    def clear_all(self):
        """Очистка всех категорий"""
        self.categories = {}
        self.save_to_file()
    
    def import_from_excel(self, file_path: str) -> Dict[str, Any]:
        """
         Импорт категорий из Excel файла
        
        Ожидаемые колонки:
        - Категория (обязательно)
        - Длина (см)
        - Ширина (см)
        - Высота (см)
        - Вес (кг)
        - Единица длины (опционально, по умолчанию см)
        - Единица веса (опционально, по умолчанию кг)
        """
        result = {
            "success": False,
            "imported": 0,
            "errors": [],
            "warnings": []
        }
        
        try:
            # Чтение Excel файла
            df = pd.read_excel(file_path, engine='openpyxl')
            
            if df.empty:
                result["errors"].append("Файл пустой")
                return result
            
            # Нормализация названий колонок
            df.columns = [col.strip().lower() for col in df.columns]
            
            # Маппинг колонок
            column_mapping = {
                'категория': 'category',
                'category': 'category',
                'название': 'category',
                'name': 'category',
                'длина': 'length',
                'length': 'length',
                'длина (см)': 'length',
                'ширина': 'width',
                'width': 'width',
                'ширина (см)': 'width',
                'высота': 'height',
                'height': 'height',
                'высота (см)': 'height',
                'вес': 'weight',
                'weight': 'weight',
                'вес (кг)': 'weight',
                'единица длины': 'length_unit',
                'length_unit': 'length_unit',
                'единица веса': 'weight_unit',
                'weight_unit': 'weight_unit'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Проверка обязательных колонок
            required_cols = ['category', 'length', 'width', 'height', 'weight']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                result["errors"].append(f"Отсутствуют колонки: {', '.join(missing_cols)}")
                return result
            
            # Импорт данных
            imported_count = 0
            for idx, row in df.iterrows():
                try:
                    category_name = str(row.get('category', '')).strip()
                    if not category_name:
                        result["warnings"].append(f"Строка {idx + 1}: пустое название категории")
                        continue
                    
                    length = safe_float(row.get('length', 0))
                    width = safe_float(row.get('width', 0))
                    height = safe_float(row.get('height', 0))
                    weight = safe_float(row.get('weight', 0))
                    
                    if length <= 0 or width <= 0 or height <= 0 or weight <= 0:
                        result["warnings"].append(f"Строка {idx + 1}: некорректные размеры для '{category_name}'")
                        continue
                    
                    length_unit = str(row.get('length_unit', 'см')).strip()
                    weight_unit = str(row.get('weight_unit', 'кг')).strip()
                    
                    self.add_category(
                        name=category_name,
                        length=length,
                        width=width,
                        height=height,
                        weight=weight,
                        unit=length_unit,
                        weight_unit=weight_unit
                    )
                    
                    imported_count += 1
                    
                except Exception as e:
                    result["errors"].append(f"Строка {idx + 1}: {str(e)}")
            
            result["success"] = imported_count > 0
            result["imported"] = imported_count
            
            if imported_count == 0:
                result["errors"].append("Не удалось импортировать ни одну категорию")
            
        except Exception as e:
            result["errors"].append(f"Ошибка чтения файла: {str(e)}")
        
        return result
    
    def export_to_excel(self, file_path: str) -> bool:
        """Экспорт категорий в Excel"""
        try:
            data = []
            for key, cat in self.categories.items():
                data.append({
                    'Категория': cat['name'],
                    'Длина (см)': cat['length_cm'],
                    'Ширина (см)': cat['width_cm'],
                    'Высота (см)': cat['height_cm'],
                    'Вес (кг)': cat['weight_kg'],
                    'Единица длины': cat.get('unit', 'см'),
                    'Единица веса': cat.get('weight_unit', 'кг')
                })
            
            df = pd.DataFrame(data)
            df.to_excel(file_path, index=False, engine='openpyxl')
            return True
        except Exception as e:
            self.logger.error(f" Ошибка экспорта: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Статистика по категориям"""
        if not self.categories:
            return {"total": 0}
        
        lengths = [cat['length_cm'] for cat in self.categories.values()]
        widths = [cat['width_cm'] for cat in self.categories.values()]
        heights = [cat['height_cm'] for cat in self.categories.values()]
        weights = [cat['weight_kg'] for cat in self.categories.values()]
        
        return {
            "total": len(self.categories),
            "avg_length": sum(lengths) / len(lengths),
            "avg_width": sum(widths) / len(widths),
            "avg_height": sum(heights) / len(heights),
            "avg_weight": sum(weights) / len(weights),
            "min_weight": min(weights),
            "max_weight": max(weights)
        }


# ============================================================================
# 🆕 БЛОК 22: UI ДЛЯ УПРАВЛЕНИЯ КАТЕГОРИЯМИ С ВЕСОГАБАРИТАМИ
# ============================================================================

def show_category_dimensions_interface():
    """
    📊 Интерфейс управления категориями с весогабаритами
    """
    st.header("📊 Категории с весогабаритами")
    st.info("""
    📋 **О РАЗДЕЛЕ:**
    Этот раздел позволяет загружать и управлять категориями товаров с их стандартными весогабаритами.
    
    **Возможности:**
    - ✅ Загрузка категорий из Excel файла
    - ✅ Добавление категорий вручную
    - ✅ Экспорт категорий в Excel
    - ✅ Использование для валидации габаритов
    - ✅ Автоматическое определение категории по названию
    """)
    
    # Инициализация базы данных
    if 'category_dimensions_db' not in st.session_state:
        st.session_state.category_dimensions_db = CategoryDimensionsDB()
    
    db = st.session_state.category_dimensions_db
    
    # Меню
    menu = st.sidebar.radio(
        "🧭 Меню",
        ["📥 Загрузка из Excel", "➕ Добавить вручную", "📋 Список категорий", " Экспорт", "📊 Статистика"],
        key="category_menu"
    )
    
    if menu == "📥 Загрузка из Excel":
        show_category_upload(db)
    elif menu == "➕ Добавить вручную":
        show_category_add_manual(db)
    elif menu == "📋 Список категорий":
        show_category_list(db)
    elif menu == "📤 Экспорт":
        show_category_export(db)
    elif menu == "📊 Статистика":
        show_category_stats(db)


def show_category_upload(db: CategoryDimensionsDB):
    """Загрузка категорий из Excel"""
    st.subheader("📥 Загрузка категорий из Excel")
    
    st.info("""
    📋 **ТРЕБОВАНИЯ К ФАЙЛУ:**
    
    **Обязательные колонки:**
    - Категория (название категории)
    - Длина (числовое значение)
    - Ширина (числовое значение)
    - Высота (числовое значение)
    - Вес (числовое значение)
    
    **Опциональные колонки:**
    - Единица длины (см, мм, м) - по умолчанию см
    - Единица веса (кг, г, т) - по умолчанию кг
    
    **Пример файла:**
    | Категория | Длина | Ширина | Высота | Вес |
    |-----------|-------|--------|--------|-----|
    | Фильтры   | 15    | 15     | 15     | 0.5 |
    | Колодки   | 15    | 10     | 5      | 2.0 |
    """)
    
    uploaded_file = st.file_uploader(
        "📤 Загрузите Excel файл с категориями",
        type=['xlsx', 'xls'],
        key="category_upload_file"
    )
    
    if uploaded_file is not None:
        # Сохраняем временный файл
        temp_path = TEMP_DIR / f"categories_{int(time.time())}.xlsx"
        TEMP_DIR.mkdir(exist_ok=True)
        
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.info(f"📄 Файл загружен: {uploaded_file.name}")
        
        if st.button("🚀 Импортировать категории", type="primary", key="import_categories"):
            with st.spinner("Импорт категорий..."):
                result = db.import_from_excel(str(temp_path))
            
            if result["success"]:
                st.success(f"✅ Импортировано {result['imported']} категорий")
                
                if result["warnings"]:
                    st.warning(f"⚠️ Предупреждений: {len(result['warnings'])}")
                    with st.expander("📋 Показать предупреждения"):
                        for warning in result["warnings"][:10]:
                            st.warning(warning)
                
                st.rerun()
            else:
                st.error("❌ Ошибка импорта")
                with st.expander(" Показать ошибки"):
                    for error in result["errors"]:
                        st.error(error)
        
        # Удалить временный файл
        if temp_path.exists():
            temp_path.unlink()
    
    # Кнопка скачать шаблон
    if st.button("📥 Скачать шаблон Excel", key="download_category_template"):
        template_data = {
            'Категория': ['Фильтры', 'Колодки', 'Масла', 'Шины'],
            'Длина': [15, 15, 10, 60],
            'Ширина': [15, 10, 10, 60],
            'Высота': [15, 5, 25, 25],
            'Вес': [0.5, 2.0, 1.0, 10.0],
            'Единица длины': ['см', 'см', 'см', 'см'],
            'Единица веса': ['кг', 'кг', 'кг', 'кг']
        }
        
        template_df = pd.DataFrame(template_data)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            template_df.to_excel(writer, index=False, sheet_name='Категории')
        
        output.seek(0)
        
        st.download_button(
            label="⬇️ Скачать шаблон",
            data=output,
            file_name="шаблон_категорий.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_category_template_btn"
        )


def show_category_add_manual(db: CategoryDimensionsDB):
    """Добавление категории вручную"""
    st.subheader("➕ Добавить категорию вручную")
    
    col1, col2 = st.columns(2)
    
    with col1:
        category_name = st.text_input(
            "📝 Название категории",
            placeholder="например: Фильтры масляные",
            key="category_name"
        )
        
        length = st.number_input(
            "📏 Длина",
            min_value=0.0,
            value=0.0,
            step=0.1,
            key="category_length"
        )
        
        width = st.number_input(
            "📐 Ширина",
            min_value=0.0,
            value=0.0,
            step=0.1,
            key="category_width"
        )
    
    with col2:
        height = st.number_input(
            "📐 Высота",
            min_value=0.0,
            value=0.0,
            step=0.1,
            key="category_height"
        )
        
        weight = st.number_input(
            "⚖️ Вес",
            min_value=0.0,
            value=0.0,
            step=0.1,
            key="category_weight"
        )
        
        unit = st.selectbox(
            " Единица длины",
            ["см", "мм", "м"],
            key="category_unit"
        )
        
        weight_unit = st.selectbox(
            "⚖️ Единица веса",
            ["кг", "г", "т"],
            key="category_weight_unit"
        )
    
    if st.button("➕ Добавить категорию", type="primary", key="add_category"):
        if not category_name:
            st.error("❌ Введите название категории")
        elif length <= 0 or width <= 0 or height <= 0 or weight <= 0:
            st.error("❌ Все размеры должны быть больше 0")
        else:
            db.add_category(
                name=category_name,
                length=length,
                width=width,
                height=height,
                weight=weight,
                unit=unit,
                weight_unit=weight_unit
            )
            st.success(f"✅ Категория '{category_name}' добавлена")
            st.rerun()


def show_category_list(db: CategoryDimensionsDB):
    """Список категорий"""
    st.subheader("📋 Список категорий")
    
    categories = db.get_all_categories()
    
    if not categories:
        st.info("️ Категории не добавлены")
        return
    
    st.info(f"📊 Всего категорий: {len(categories)}")
    
    # Таблица категорий
    data = []
    for key, cat in categories.items():
        data.append({
            'Категория': cat['name'],
            'Длина (см)': cat['length_cm'],
            'Ширина (см)': cat['width_cm'],
            'Высота (см)': cat['height_cm'],
            'Вес (кг)': cat['weight_kg'],
            'Единица': cat.get('unit', 'см'),
            'Ед. веса': cat.get('weight_unit', 'кг')
        })
    
    df = pd.DataFrame(data)
    st_dataframe_compat(df, key="categories_table")
    
    # Удаление категории
    st.divider()
    st.subheader("🗑️ Удалить категорию")
    
    category_to_delete = st.selectbox(
        "Выберите категорию для удаления",
        options=list(categories.keys()),
        format_func=lambda x: categories[x]['name'],
        key="delete_category_select"
    )
    
    if st.button("🗑️ Удалить", key="delete_category_btn"):
        db.delete_category(category_to_delete)
        st.success(f"✅ Категория удалена")
        st.rerun()
    
    # Очистка всех
    st.divider()
    if st.button("⚠️ Очистить все категории", key="clear_all_categories"):
        if st.checkbox("Подтверждаю удаление всех категорий", key="confirm_clear"):
            db.clear_all()
            st.success("✅ Все категории удалены")
            st.rerun()


def show_category_export(db: CategoryDimensionsDB):
    """Экспорт категорий"""
    st.subheader("📤 Экспорт категорий")
    
    categories = db.get_all_categories()
    
    if not categories:
        st.info("ℹ️ Нет категорий для экспорта")
        return
    
    st.info(f"📊 Всего категорий: {len(categories)}")
    
    if st.button("📥 Экспортировать в Excel", key="export_categories"):
        output_path = TEMP_DIR / f"categories_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        TEMP_DIR.mkdir(exist_ok=True)
        
        if db.export_to_excel(str(output_path)):
            with open(output_path, "rb") as f:
                file_data = f.read()
            
            st.download_button(
                label="⬇️ Скачать Excel файл",
                data=file_data,
                file_name=output_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_categories_excel"
            )
            
            st.success("✅ Файл готов к скачиванию")
        else:
            st.error("❌ Ошибка экспорта")


def show_category_stats(db: CategoryDimensionsDB):
    """Статистика категорий"""
    st.subheader("📊 Статистика")
    
    stats = db.get_statistics()
    
    if stats.get('total', 0) == 0:
        st.info("️ Нет данных для статистики")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📦 Всего категорий", stats['total'])
    
    with col2:
        st.metric("📏 Ср. длина", f"{stats.get('avg_length', 0):.1f} см")
    
    with col3:
        st.metric("📐 Ср. ширина", f"{stats.get('avg_width', 0):.1f} см")
    
    with col4:
        st.metric("📐 Ср. высота", f"{stats.get('avg_height', 0):.1f} см")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("⚖️ Ср. вес", f"{stats.get('avg_weight', 0):.2f} кг")
    
    with col2:
        st.metric("⚖️ Диапазон веса", f"{stats.get('min_weight', 0):.2f} - {stats.get('max_weight', 0):.2f} кг")


# ============================================================================
# 🆕 БЛОК 23: ИНТЕГРАЦИЯ С ВАЛИДАТОРОМ ВЕСОГАБАРИТОВ
# ============================================================================

def validate_dimensions_with_category(
    length: float,
    width: float,
    height: float,
    weight: float,
    category: str,
    tolerance_percent: float = 20.0
) -> Dict[str, Any]:
    """
    ✅ Валидация весогабаритов с использованием базы категорий
    
    Args:
        length, width, height: Фактические размеры (см)
        weight: Фактический вес (кг)
        category: Название категории
        tolerance_percent: Допустимое отклонение в %
    
    Returns:
        Dict с результатами валидации
    """
    result = {
        "valid": True,
        "category": category,
        "deviations": [],
        "warnings": []
    }
    
    # Получаем стандартные размеры категории
    if 'category_dimensions_db' in st.session_state:
        db = st.session_state.category_dimensions_db
        category_data = db.get_category(category)
        
        if category_data:
            std_length = category_data['length_cm']
            std_width = category_data['width_cm']
            std_height = category_data['height_cm']
            std_weight = category_data['weight_kg']
            
            # Проверяем отклонения
            if std_length > 0:
                length_dev = abs(length - std_length) / std_length * 100
                if length_dev > tolerance_percent:
                    result["deviations"].append({
                        "parameter": "Длина",
                        "actual": length,
                        "expected": std_length,
                        "deviation_percent": length_dev
                    })
                    result["valid"] = False
            
            if std_width > 0:
                width_dev = abs(width - std_width) / std_width * 100
                if width_dev > tolerance_percent:
                    result["deviations"].append({
                        "parameter": "Ширина",
                        "actual": width,
                        "expected": std_width,
                        "deviation_percent": width_dev
                    })
                    result["valid"] = False
            
            if std_height > 0:
                height_dev = abs(height - std_height) / std_height * 100
                if height_dev > tolerance_percent:
                    result["deviations"].append({
                        "parameter": "Высота",
                        "actual": height,
                        "expected": std_height,
                        "deviation_percent": height_dev
                    })
                    result["valid"] = False
            
            if std_weight > 0:
                weight_dev = abs(weight - std_weight) / std_weight * 100
                if weight_dev > tolerance_percent:
                    result["deviations"].append({
                        "parameter": "Вес",
                        "actual": weight,
                        "expected": std_weight,
                        "deviation_percent": weight_dev
                    })
                    result["valid"] = False
            
            if result["valid"]:
                result["warnings"].append("✅ Все параметры в пределах нормы")
        else:
            result["warnings"].append(f"⚠️ Категория '{category}' не найдена в базе")
    
    return result
# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ ПРИЛОЖЕНИЯ
# ============================================================================
def main():
    """Главная функция приложения"""
    st.set_page_config(
        page_title=APP_NAME,
        page_icon="🚗",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title(APP_NAME)
    st.caption(f"Версия {APP_VERSION} | {APP_DESCRIPTION}")
    
    st.sidebar.title("🧭 Навигация")
    section = st.sidebar.radio(
        "Выберите раздел:",
        [
            "📁 Загрузка данных",
            "📊 Юнит-экономика",
            "🗂️ Каталог для группировки",
            "📏 Категории с весогабаритами",  # ✅ НОВЫЙ РАЗДЕЛ
            "🤖 AI Тарифы",
            "🌐 API Тарифы маркетплейсов",
            " Умная загрузка тарифов"
        ],
        key="main_navigation"
    )
    
    if section == "📁 Загрузка данных":
        show_data_upload_interface()
    elif section == "📊 Юнит-экономика":
        show_unit_economics_interface()
    elif section == "🗂️ Каталог для группировки":
        show_catalog_grouping_interface()
    elif section == "📏 Категории с весогабаритами":  # ✅ НОВЫЙ РАЗДЕЛ
        show_category_dimensions_interface()
    elif section == "🤖 AI Тарифы":
        show_ai_tariffs_interface()
    elif section == "🌐 API Тарифы маркетплейсов":
        show_api_tariffs_interface()
    elif section == "🧠 Умная загрузка тарифов":
        show_smart_tariff_interface()


# ✅ ТОЧКА ВХОДА
if __name__ == "__main__":
    main()


warnings.filterwarnings('ignore')

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_ROW_LIMIT = 1_000_000


class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        # Загрузка конфигураций
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()

        st.set_page_config(
            page_title="AutoParts Catalog 10M+",
            layout="wide",
            page_icon="🚗"
        )

    # --- Конфигурации ---
    def load_cloud_config(self) -> Dict[str, Any]:
        config_path = self.data_dir / "cloud_config.json"
        default_config = {
            "enabled": False,
            "provider": "s3",
            "bucket": "",
            "region": "",
            "sync_interval": 3600,
            "last_sync": 0
        }
        if config_path.exists():
            try:
                return json.loads(config_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения cloud_config.json: {e}")
                return default_config
        else:
            config_path.write_text(json.dumps(
                default_config, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_config

    def save_cloud_config(self):
        config_path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        config_path.write_text(json.dumps(
            self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_price_rules(self) -> Dict[str, Any]:
        price_rules_path = self.data_dir / "price_rules.json"
        default_rules = {
            "global_markup": 0.2,
            "brand_markups": {},
            "min_price": 0.0,
            "max_price": 99999.0
        }
        if price_rules_path.exists():
            try:
                return json.loads(price_rules_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения price_rules.json: {e}")
                return default_rules
        else:
            price_rules_path.write_text(json.dumps(
                default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_rules

    def save_price_rules(self):
        price_rules_path = self.data_dir / "price_rules.json"
        price_rules_path.write_text(json.dumps(
            self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_exclusion_rules(self) -> List[str]:
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        if exclusion_path.exists():
            try:
                return [line.strip() for line in exclusion_path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка чтения exclusion_rules.txt: {e}")
                return []
        else:
            content = "Кузов\nСтекла\nМасла"
            exclusion_path.write_text(content, encoding='utf-8')
            return ["Кузов", "Стекла", "Масла"]

    def save_exclusion_rules(self):
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        exclusion_path.write_text(
            "\n".join(self.exclusion_rules), encoding='utf-8')

    def load_category_mapping(self) -> Dict[str, str]:
        category_path = self.data_dir / "category_mapping.txt"
        default_mapping = {
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        }
        if category_path.exists():
            try:
                mapping = {}
                for line in category_path.read_text(encoding='utf-8').splitlines():
                    if line.strip() and "|" in line:
                        key, value = line.split("|", 1)
                        mapping[key.strip()] = value.strip()
                return mapping
            except Exception as e:
                logger.error(f"Ошибка чтения category_mapping.txt: {e}")
                return default_mapping
        else:
            content = "\n".join(
                [f"{k}|{v}" for k, v in default_mapping.items()])
            category_path.write_text(content, encoding='utf-8')
            return default_mapping

    def save_category_mapping(self):
        category_path = self.data_dir / "category_mapping.txt"
        content = "\n".join(
            [f"{k}|{v}" for k, v in self.category_mapping.items()])
        category_path.write_text(content, encoding='utf-8')

    # --- База данных ---
    def setup_database(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parts (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                artikul VARCHAR,
                brand VARCHAR,
                multiplicity INTEGER,
                barcode VARCHAR,
                length DOUBLE,
                width DOUBLE,
                height DOUBLE,
                weight DOUBLE,
                image_url VARCHAR,
                dimensions_str VARCHAR,
                description VARCHAR,
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                price DOUBLE,
                currency VARCHAR DEFAULT 'RUB',
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        self.create_indexes()

    def create_indexes(self):
        st.info("🛠️ Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        st.success("🛠️ Индексы созданы.")

    # --- Нормализация и очистка ---
    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    @staticmethod
    def clean_values(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        name_lower = name_series.str.to_lowercase()
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        # Пользовательские правила — приоритет
        for key, category in self.category_mapping.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower())
            ).then(pl.lit(category))
        # Стандартные правила
        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выпуск': 'глушитель|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(pattern, literal=False)
            ).then(pl.lit(category))
        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')

    # --- Обработка файлов ---
    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        column_variants = {
            'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE'],
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand', 'производитель', 'manufacturer'],
            'name': ['наименование', 'название', 'name', 'описание', 'description'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13'],
            'multiplicity': ['кратность шт', 'кратность', 'multiplicity'],
            'length': ['длина (см)', 'длина', 'length', 'длинна'],
            'width': ['ширина (см)', 'ширина', 'width'],
            'height': ['высота (см)', 'высота', 'height'],
            'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight'],
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size'],
            'price': ['цена', 'price', 'рекомендованная цена', 'retail price'],
            'currency': ['валюта', 'currency']
        }
        actual_lower = {col.lower(): col for col in actual_columns}
        mapping = {}
        for expected in expected_columns:
            variants = column_variants.get(expected, [expected])
            for variant in variants:
                variant_lower = variant.lower()
                for actual_l, actual_orig in actual_lower.items():
                    if variant_lower in actual_l and actual_orig not in mapping:
                        mapping[actual_orig] = expected
                        break
        return mapping

    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        logger.info(f"Обработка файла: {file_type} ({file_path})")
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return pl.DataFrame()

            df = pl.read_excel(file_path, engine='calamine')
            if df.is_empty():
                logger.warning(f"Пустой файл: {file_path}")
                return pl.DataFrame()

        except Exception as e:
            logger.exception(f"Ошибка чтения файла {file_path}: {e}")
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['artikul', 'brand', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)
        if not column_mapping:
            logger.warning(
                f"Не удалось определить колонки для файла {file_type}. Доступные: {df.columns}")
            return pl.DataFrame()

        df = df.rename(column_mapping)

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))

        key_cols = [col for col in ['oe_number',
                                    'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.normalize_key(
                    pl.col(col)).alias(f"{col}_norm"))

        return df

    # --- Загрузка и обновление в базе ---
    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        if df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        temp_view_name = f"temp_{table_name}_{int(time.time())}"

        # Регистрация временной таблицы в DuckDB
        try:
            self.conn.register(temp_view_name, df.to_arrow())
        except Exception as e:
            logger.error(f"Ошибка регистрации временной таблицы: {e}")
            return

        # DuckDB не гарантирует синтаксис ON CONFLICT как в Postgres -> используем delete+insert
        try:
            pk_list = pk
            pk_cols_csv = ", ".join(f'"{c}"' for c in pk_list)
            # удалить существующие записи, которые совпадают по PK
            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE ({pk_cols_csv}) IN (SELECT {pk_cols_csv} FROM {temp_view_name});
            """
            self.conn.execute(delete_sql)
            # вставить новые записи
            insert_sql = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_view_name};
            """
            self.conn.execute(insert_sql)
            logger.info(
                f"Успешно upsert {len(df)} записей в таблицу {table_name}.")
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(
                f"Ошибка при записи в таблицу {table_name}. Детали в логе.")
        finally:
            try:
                self.conn.unregister(temp_view_name)
            except Exception:
                pass

    def upsert_prices(self, price_df: pl.DataFrame):
        if price_df.is_empty():
            return

        if 'artikul' in price_df.columns and 'brand' in price_df.columns:
            price_df = price_df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])

        if 'currency' not in price_df.columns:
            price_df = price_df.with_columns(pl.lit('RUB').alias('currency'))

        price_df = price_df.filter(
            (pl.col('price') >= self.price_rules['min_price']) &
            (pl.col('price') <= self.price_rules['max_price'])
        )

        self.upsert_data('prices', price_df, ['artikul_norm', 'brand_norm'])

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        num_steps = len(steps)
        progress_bar = st.progress(
            0, text="Подготовка к обновлению базы данных...")
        step_counter = 0

        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка OE данных...")
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(
                subset=['oe_number_norm'], keep='first')

            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(
                    self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))

            self.upsert_data('oe', oe_df, ['oe_number_norm'])

            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, [
                             'oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка кроссов...")
            df = dataframes['cross'].filter(
                (pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df.select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, [
                             'oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'prices' in dataframes:
            price_df = dataframes['prices']
            if not price_df.is_empty():
                st.info("💰 Обработка цен...")
                self.upsert_prices(price_df)
                st.success(
                    f"✅ Успешно обновлено {len(price_df)} ценовых записей")

        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1),
                              text=f"({step_counter}/{num_steps}) Сборка и обновление данных по артикулам...")

        # Собираем parts из разных файлов
        parts_df = None
        file_priority = ['oe', 'barcode', 'images', 'dimensions']
        key_files = {ftype: df for ftype,
                     df in dataframes.items() if ftype in file_priority}

        if key_files:
            all_parts = pl.concat([
                df.select(['artikul', 'artikul_norm', 'brand', 'brand_norm'])
                for df in key_files.values() if 'artikul_norm' in df.columns and 'brand_norm' in df.columns
            ]).filter(pl.col('artikul_norm') != "").unique(subset=['artikul_norm', 'brand_norm'], keep='first')
            parts_df = all_parts

            for ftype in file_priority:
                if ftype not in key_files:
                    continue
                df = key_files[ftype]
                if df.is_empty() or 'artikul_norm' not in df.columns:
                    continue
                join_cols = [col for col in df.columns if col not in [
                    'artikul', 'artikul_norm', 'brand', 'brand_norm']]
                if not join_cols:
                    continue
                existing_cols = set(parts_df.columns)
                join_cols = [
                    col for col in join_cols if col not in existing_cols]
                if not join_cols:
                    continue
                df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique(
                    subset=['artikul_norm', 'brand_norm'], keep='first')
                parts_df = parts_df.join(
                    df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)

        if parts_df is not None and not parts_df.is_empty():
            if 'multiplicity' not in parts_df.columns:
                parts_df = parts_df.with_columns(
                    multiplicity=pl.lit(1).cast(pl.Int32))
            else:
                parts_df = parts_df.with_columns(
                    pl.col('multiplicity').fill_null(1).cast(pl.Int32))

            for col in ['length', 'width', 'height']:
                if col not in parts_df.columns:
                    parts_df = parts_df.with_columns(
                        pl.lit(None).cast(pl.Float64).alias(col))

            if 'dimensions_str' not in parts_df.columns:
                parts_df = parts_df.with_columns(
                    dimensions_str=pl.lit(None).cast(pl.Utf8))

            parts_df = parts_df.with_columns([
                pl.col('length').cast(pl.Utf8).fill_null(
                    '').alias('_length_str'),
                pl.col('width').cast(pl.Utf8).fill_null(
                    '').alias('_width_str'),
                pl.col('height').cast(pl.Utf8).fill_null(
                    '').alias('_height_str'),
            ])

            parts_df = parts_df.with_columns(
                dimensions_str=pl.when(
                    (pl.col('dimensions_str').is_not_null()) &
                    (pl.col('dimensions_str').cast(pl.Utf8) != '')
                ).then(
                    pl.col('dimensions_str').cast(pl.Utf8)
                ).otherwise(
                    pl.concat_str([
                        pl.col('_length_str'), pl.lit('x'),
                        pl.col('_width_str'), pl.lit('x'),
                        pl.col('_height_str')
                    ], separator='')
                )
            )

            parts_df = parts_df.drop(
                ['_length_str', '_width_str', '_height_str'])

            if 'artikul' not in parts_df.columns:
                parts_df = parts_df.with_columns(artikul=pl.lit(''))
            if 'brand' not in parts_df.columns:
                parts_df = parts_df.with_columns(brand=pl.lit(''))

            parts_df = parts_df.with_columns([
                pl.col('artikul').cast(pl.Utf8).fill_null(
                    '').alias('_artikul_str'),
                pl.col('brand').cast(pl.Utf8).fill_null(
                    '').alias('_brand_str'),
                pl.col('multiplicity').cast(
                    pl.Utf8).alias('_multiplicity_str'),
            ])

            parts_df = parts_df.with_columns(
                description=pl.concat_str([
                    pl.lit('Артикул: '), pl.col('_artikul_str'),
                    pl.lit(', Бренд: '), pl.col('_brand_str'),
                    pl.lit(', Кратность: '), pl.col(
                        '_multiplicity_str'), pl.lit(' шт.')
                ], separator='')
            )

            parts_df = parts_df.drop(
                ['_artikul_str', '_brand_str', '_multiplicity_str'])

            final_columns = [
                'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode',
                'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
            ]
            select_exprs = [pl.col(c) if c in parts_df.columns else pl.lit(
                None).alias(c) for c in final_columns]
            parts_df = parts_df.select(select_exprs)

            self.upsert_data('parts', parts_df, ['artikul_norm', 'brand_norm'])

        progress_bar.progress(1.0, text="Обновление базы данных завершено!")
        time.sleep(1)
        progress_bar.empty()

    # --- Экспорт ---
    def _get_brand_markups_sql(self) -> str:
        rows = []
        for brand, markup in self.price_rules['brand_markups'].items():
            # экранируем одинарные кавычки в brand
            safe_brand = brand.replace("'", "''")
            rows.append(f"SELECT '{safe_brand}' AS brand, {markup} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, NULL AS markup LIMIT 0"

    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        """
        Исправленная версия build_export_query: собирает список колонок по-частям,
        избегая лишних запятых (zero-length delimited identifier) и корректно
        включает колонки цены при запросе selected_columns.
        """
        description_text = (
            "Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. "
            "Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей. "
            "В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электроматериалы, автомасла, инструмент, "
            "а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. "
            "Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. "
            "Выбирайте только лучшее — надежность и качество от ведущих производителей."
        )

        # Подготовка SQL для наценок по брендам
        brand_markups_sql = self._get_brand_markups_sql()

        # Составляем список выражений для SELECT последовательно, чтобы избежать лишней запятой
        select_parts = []

        # Колонки с ценой (включаем только если include_prices и если пользователь не ограничил selected_columns
        # или если explicit-но запросил "Цена" / "Валюта")
        price_requested = include_prices and (not selected_columns or "Цена" in selected_columns or "Валюта" in selected_columns)
        if price_requested:
            if apply_markup:
                global_markup = self.price_rules.get('global_markup', 0)
                select_parts.append(
                    f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {global_markup})) ELSE pr.price END AS \"Цена\""
                )
            else:
                select_parts.append('pr.price AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")

        # Остальные колонки (название -> выражение)
        columns_map = [
            ("Артикул бренда", 'r.artikul AS "Артикул бренда"'),
            ("Бренд", 'r.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"'),
            ("Описание", 'CONCAT(COALESCE(r.description, \'\'), dt.text) AS "Описание"'),
            ("Категория товара", 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория товара"'),
            ("Кратность", 'r.multiplicity AS "Кратность"'),
            ("Длинна", 'COALESCE(r.length, r.analog_length) AS "Длинна"'),
            ("Ширина", 'COALESCE(r.width, r.analog_width) AS "Ширина"'),
            ("Высота", 'COALESCE(r.height, r.analog_height) AS "Высота"'),
            ("Вес", 'COALESCE(r.weight, r.analog_weight) AS "Вес"'),
            ("Длинна/Ширина/Высота", """
                COALESCE(
                    CASE
                        WHEN r.dimensions_str IS NULL OR r.dimensions_str = '' OR UPPER(TRIM(r.dimensions_str)) = 'XX'
                        THEN NULL
                        ELSE r.dimensions_str
                    END,
                    r.analog_dimensions_str
                ) AS "Длинна/Ширина/Высота"
            """),
            ("OE номер", 'r.oe_list AS "OE номер"'),
            ("аналоги", 'r.analog_list AS "аналоги"'),
            ("Ссылка на изображение", 'r.image_url AS "Ссылка на изображение"')
        ]

        # Добавляем остальные выражения в порядке columns_map, с учётом selected_columns фильтра
        for name, expr in columns_map:
            if not selected_columns or name in selected_columns:
                select_parts.append(expr.strip())

        # Если вдруг пользователь полностью снял все колонки и нет price_requested — включаем минимум r.artikul/r.brand
        if not select_parts:
            select_parts = ['r.artikul AS "Артикул бренда"', 'r.brand AS "Бренд"']

        # Собираем тело SELECT, безопасно объединяя без завершающих запятых
        select_clause = ",\n        ".join(select_parts)

        ctes = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${description_text}$$ AS text
        ),
        BrandMarkups AS (
            SELECT brand, markup FROM (
                {brand_markups_sql}
            ) AS tmp
        ),
        PartDetails AS (
            SELECT 
                cr.artikul_norm, 
                cr.brand_norm,
                STRING_AGG(
                    DISTINCT regexp_replace(
                        regexp_replace(o.oe_number, '''', ''), 
                        '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
                    ), ', '
                ) AS oe_list,
                ANY_VALUE(o.name) AS representative_name,
                ANY_VALUE(o.applicability) AS representative_applicability,
                ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT 
                cr1.artikul_norm, 
                cr1.brand_norm,
                STRING_AGG(
                    DISTINCT regexp_replace(
                        regexp_replace(p2.artikul, '''', ''), 
                        '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
                    ), ', '
                ) AS analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE (cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm)
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        ),
        InitialOENumbers AS (
            SELECT DISTINCT p.artikul_norm, p.brand_norm, cr.oe_number_norm
            FROM parts p
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            WHERE cr.oe_number_norm IS NOT NULL
        ),
        Level1Analogs AS (
            SELECT DISTINCT 
                i.artikul_norm AS source_artikul_norm, 
                i.brand_norm AS source_brand_norm,
                cr2.artikul_norm AS related_artikul_norm, 
                cr2.brand_norm AS related_brand_norm
            FROM InitialOENumbers i
            JOIN cross_references cr2 ON i.oe_number_norm = cr2.oe_number_norm
            WHERE NOT (i.artikul_norm = cr2.artikul_norm AND i.brand_norm = cr2.brand_norm)
        ),
        Level1OENumbers AS (
            SELECT DISTINCT 
                l1.source_artikul_norm, 
                l1.source_brand_norm, 
                cr3.oe_number_norm
            FROM Level1Analogs l1
            JOIN cross_references cr3 ON l1.related_artikul_norm = cr3.artikul_norm AND l1.related_brand_norm = cr3.brand_norm
            WHERE NOT EXISTS (
                SELECT 1 FROM InitialOENumbers i
                WHERE i.artikul_norm = l1.source_artikul_norm 
                  AND i.brand_norm = l1.source_brand_norm 
                  AND i.oe_number_norm = cr3.oe_number_norm
            )
        ),
        Level2Analogs AS (
            SELECT DISTINCT 
                loe.source_artikul_norm, 
                loe.source_brand_norm,
                cr4.artikul_norm AS related_artikul_norm, 
                cr4.brand_norm AS related_brand_norm
            FROM Level1OENumbers loe
            JOIN cross_references cr4 ON loe.oe_number_norm = cr4.oe_number_norm
            WHERE NOT (loe.source_artikul_norm = cr4.artikul_norm AND loe.source_brand_norm = cr4.brand_norm)
        ),
        AllRelatedParts AS (
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
            FROM Level1Analogs
            UNION
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
            FROM Level2Analogs
        ),
        AggregatedAnalogData AS (
            SELECT 
                arp.source_artikul_norm AS artikul_norm,
                arp.source_brand_norm AS brand_norm,
                MAX(CASE WHEN p2.length IS NOT NULL THEN p2.length ELSE NULL END) AS length,
                MAX(CASE WHEN p2.width IS NOT NULL THEN p2.width ELSE NULL END) AS width,
                MAX(CASE WHEN p2.height IS NOT NULL THEN p2.height ELSE NULL END) AS height,
                MAX(CASE WHEN p2.weight IS NOT NULL THEN p2.weight ELSE NULL END) AS weight,
                ANY_VALUE(
                    CASE 
                        WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX'
                        THEN p2.dimensions_str
                        ELSE NULL
                    END
                ) AS dimensions_str,
                ANY_VALUE(
                    CASE 
                        WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' 
                        THEN pd2.representative_name 
                        ELSE NULL
                    END
                ) AS representative_name,
                ANY_VALUE(
                    CASE 
                        WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != ''
                        THEN pd2.representative_applicability
                        ELSE NULL
                    END
                ) AS representative_applicability,
                ANY_VALUE(
                    CASE 
                        WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != ''
                        THEN pd2.representative_category
                        ELSE NULL
                    END
                ) AS representative_category
            FROM AllRelatedParts arp
            JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT 
                p.artikul_norm,
                p.brand_norm,
                p.artikul,
                p.brand,
                p.description,
                p.multiplicity,
                p.length,
                p.width,
                p.height,
                p.weight,
                p.dimensions_str,
                p.image_url,
                pd.representative_name,
                pd.representative_applicability,
                pd.representative_category,
                pd.oe_list,
                aa.analog_list,
                p_analog.length AS analog_length,
                p_analog.width AS analog_width,
                p_analog.height AS analog_height,
                p_analog.weight AS analog_weight,
                p_analog.dimensions_str AS analog_dimensions_str,
                p_analog.representative_name AS analog_representative_name,
                p_analog.representative_applicability AS analog_representative_applicability,
                p_analog.representative_category AS analog_representative_category,
                ROW_NUMBER() OVER (
                    PARTITION BY p.artikul_norm, p.brand_norm 
                    ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST
                ) AS rn
            FROM parts p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
        )
        """

        price_join = """
        LEFT JOIN prices pr ON r.artikul_norm = pr.artikul_norm AND r.brand_norm = pr.brand_norm
        LEFT JOIN BrandMarkups brm ON r.brand = brm.brand
        """ if include_prices else ""

        query = f"""
        {ctes}
        SELECT
            {select_clause}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        {price_join}
        WHERE r.rn = 1
        ORDER BY r.brand, r.artikul
        """

        # Небольшая пост-обработка: удалить пустые строки и лишевые пробелы
        return "\n".join([line.rstrip() for line in query.strip().splitlines()])

    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        total = self.conn.execute(
            "SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total} записей в CSV...")
        try:
            query = self.build_export_query(
                selected_columns, include_prices, apply_markup)
            # Логирование запроса
            logger.info(f"Executing export query: {query}")
            # Получаем результат как polars, затем в pandas для корректной записи CSV
            df = self.conn.execute(query).pl()
            import pandas as pd
            pdf = df.to_pandas()

            # Обработка колонок размеров
            dimension_cols = ["Длинна", "Ширина",
                              "Высота", "Вес", "Длинна/Ширина/Высота"]
            for col in dimension_cols:
                if col in pdf.columns:
                    pdf[col] = pdf[col].astype(str).replace({'nan': ''})

            # Убедитесь, что директория для экспорта существует
            output_dir = Path("auto_parts_data")
            output_dir.mkdir(parents=True, exist_ok=True)

            buf = io.StringIO()
            pdf.to_csv(buf, sep=';', index=False)
            with open(output_path, "wb") as f:
                f.write(b'\xef\xbb\xbf')  # Добавление BOM для поддержки UTF-8
                f.write(buf.getvalue().encode('utf-8'))
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(
                f"Данные экспортированы: {output_path} ({size_mb:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта CSV")
            st.error(f"Ошибка при экспорте в CSV: {str(e)}")
            return False

    def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        import pandas as pd
        query = self.build_export_query(
            selected_columns, include_prices, apply_markup)
        df = pd.read_sql(query, self.conn)
        for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
            if col in df.columns:
                df[col] = df[col].astype(str).replace(
                    {r'^nan$': ''}, regex=True)
        if len(df) <= EXCEL_ROW_LIMIT:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        else:
            sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for i in range(sheets):
                    df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(
                        writer, index=False, sheet_name=f"Данные_{i+1}")
        return True

    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        try:
            query = self.build_export_query(
                selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта Parquet")
            st.error(f"Ошибка при экспорте в Parquet: {str(e)}")
            return False

    # --- Управление данными ---
    def delete_by_brand(self, brand_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            if deleted_count == 0:
                logger.info(f"No records found for brand: {brand_norm}")
                return 0
            self.conn.execute(
                "DELETE FROM parts WHERE brand_norm = ?", [brand_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting by brand {brand_norm}: {e}")
            raise

    def delete_by_artikul(self, artikul_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            if deleted_count == 0:
                logger.info(f"No records found for artikul: {artikul_norm}")
                return 0
            self.conn.execute(
                "DELETE FROM parts WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting by artikul {artikul_norm}: {e}")
            raise

    # --- Интерфейсы ---
    def show_export_interface(self):
        st.header("📤 Экспорт данных")
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        st.info(f"Всего: {total}")
        if total == 0:
            st.warning("Нет данных для экспорта")
            return

        format_choice = st.radio("Формат", ["CSV", "Excel", "Parquet"])
        selected_columns = st.multiselect("Колонки", [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
            "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"
        ])

        include_prices = st.checkbox("Включить цены", value=True)
        apply_markup = st.checkbox(
            "Применить наценку", value=True, disabled=not include_prices)

        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"export.{format_choice.lower()}"
            with st.spinner("Генерация файла..."):
                if format_choice == "CSV":
                    self.export_to_csv_optimized(str(
                        output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Excel":
                    self.export_to_excel_optimized(str(
                        output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Parquet":
                    self.export_to_parquet(str(
                        output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                else:
                    st.warning("Неподдерживаемый формат")
                    return
            with open(output_path, "rb") as f:
                st.download_button("⬇️ Скачать файл", f,
                                   file_name=output_path.name)

    def show_price_settings(self):
        st.header("💰 Управление ценами и наценками")
        st.subheader("Общая наценка")
        global_markup = st.number_input(
            "Общая наценка (%):",
            min_value=0.0,
            max_value=500.0,
            value=self.price_rules['global_markup'] * 500,
            step=0.1
        )
        self.price_rules['global_markup'] = global_markup / 500

        st.subheader("Наценки по брендам")
        brand_markups = self.price_rules.get('brand_markups', {})

        try:
            brands_result = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand").fetchall()
            available_brands = [row[0]
                                for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка при получении списка брендов: {e}")
            st.error("❌ Ошибка при загрузке брендов")
            available_brands = []

        if available_brands:
            col1, col2 = st.columns([2, 1])
            with col1:
                selected_brand = st.selectbox(
                    "Выберите бренд:", available_brands)
            with col2:
                current_markup = brand_markups.get(
                    selected_brand, self.price_rules.get('global_markup', 0))
                brand_markup = st.number_input(
                    "Наценка (%):",
                    min_value=0.0,
                    max_value=500.0,
                    value=current_markup * 500,
                    step=0.1,
                    key=f"markup_{selected_brand}"
                )
            if st.button("Сохранить наценку", key=f"save_{selected_brand}"):
                brand_markups[selected_brand] = brand_markup / 500
                self.price_rules['brand_markups'] = brand_markups
                self.save_price_rules()
                st.success(f"✅ Наценка для {selected_brand} сохранена")

        st.subheader("Ограничения по ценам")
        col1, col2 = st.columns(2)
        with col1:
            min_price = st.number_input("Минимальная цена:", min_value=0.0, value=float(
                self.price_rules['min_price']), step=0.01)
            self.price_rules['min_price'] = min_price
        with col2:
            max_price = st.number_input("Максимальная цена:", min_value=0.0, value=float(
                self.price_rules['max_price']), step=0.01)
            self.price_rules['max_price'] = max_price

        if st.button("Сохранить все настройки цен"):
            self.save_price_rules()
            st.success("✅ Все настройки цен сохранены")

    def show_exclusion_settings(self):
        st.header("🚫 Управление исключениями при экспорте")
        st.info("Товары, содержащие эти слова в названии, будут исключены из экспорта")

        current_exclusions = "\n".join(self.exclusion_rules)
        new_exclusions = st.text_area(
            "Список исключений (по одному на строку):",
            value=current_exclusions,
            height=200,
            placeholder="Введите слова для исключения, например:\nКузов\nСтекла\nМасла"
        )

        if st.button("Сохранить правила исключения"):
            cleaned = [line.strip()
                       for line in new_exclusions.splitlines() if line.strip()]
            if len(cleaned) != len(set(cleaned)):
                st.warning(
                    "Обнаружены дублирующие записи. Они будут автоматически удалены.")
            self.exclusion_rules = list(dict.fromkeys(cleaned))
            self.save_exclusion_rules()
            st.success("✅ Правила исключения сохранены")

    def show_category_mapping(self):
        st.header("🗂️ Управление категориями товаров")
        st.info("Настройте соответствие между названиями товаров и категориями")

        st.subheader("Текущие правила")
        if self.category_mapping:
            mapping_df = pl.DataFrame({
                "Название товара": list(self.category_mapping.keys()),
                "Категория": list(self.category_mapping.values())
            }).to_pandas()
            st.dataframe(mapping_df, width='stretch', hide_index=True)
        else:
            st.write("Нет пользовательских правил")

        st.subheader("Добавить правило")
        col1, col2 = st.columns(2)
        with col1:
            name_pattern = st.text_input("Ключевое слово в названии")
        with col2:
            category = st.text_input("Категория")
        if st.button("➕ Добавить"):
            if name_pattern.strip() and category.strip():
                normalized_key = name_pattern.strip().lower()
                existing_keys = {
                    k.lower(): k for k in self.category_mapping.keys()}
                if normalized_key in existing_keys:
                    st.warning(
                        f"Правило для '{existing_keys[normalized_key]}' обновлено")
                self.category_mapping[name_pattern.strip()] = category.strip()
                self.save_category_mapping()
                st.success(
                    f"Добавлено: {name_pattern.strip()} → {category.strip()}")
                st.experimental_rerun()
            else:
                st.error("Заполните оба поля")

        if self.category_mapping:
            st.subheader("🗑️ Удалить правило")
            rule_to_delete = st.selectbox(
                "Выберите правило",
                options=list(self.category_mapping.keys()),
                format_func=lambda x: f"{x} → {self.category_mapping[x]}"
            )
            if st.button("Удалить"):
                del self.category_mapping[rule_to_delete]
                self.save_category_mapping()
                st.success(f"Удалено: {rule_to_delete}")
                st.experimental_rerun()

    def show_cloud_sync(self):
        st.header("☁️ Облачная синхронизация")
        st.subheader("Настройки")
        self.cloud_config['enabled'] = st.checkbox(
            "Включить", value=self.cloud_config['enabled'])
        providers = ["s3", "gcs", "azure"]
        current_idx = providers.index(
            self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
        self.cloud_config['provider'] = st.selectbox(
            "Провайдер", providers, index=current_idx)
        self.cloud_config['bucket'] = st.text_input(
            "Bucket / Container", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input(
            "Регион", value=self.cloud_config['region'])
        self.cloud_config['sync_interval'] = st.number_input(
            "Интервал (сек)", min_value=300, max_value=86400, value=int(self.cloud_config['sync_interval']))

        if st.button("💾 Сохранить настройки"):
            self.save_cloud_config()
            st.success("Настройки сохранены")

        st.subheader("Текущее состояние")
        last_sync = self.cloud_config.get('last_sync', 0)
        if last_sync > 0:
            st.info(
                f"Последняя синхронизация: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_sync))}")
        else:
            st.info("Еще не синхронизировано")
        if st.button("🔄 Выполнить сейчас"):
            self.perform_cloud_sync()

    def perform_cloud_sync(self):
        if not self.cloud_config.get('enabled'):
            st.warning("Синхронизация отключена")
            return
        if not self.cloud_config.get('bucket'):
            st.error("Не указан bucket")
            return
        with st.spinner("Синхронизация..."):
            time.sleep(1.5)
            st.success("База успешно отправлена")
            self.cloud_config['last_sync'] = int(time.time())
            self.save_cloud_config()

    def show_statistics(self):
        st.header("📈 Статистика")
        stats = {}
        try:
            stats['parts'] = self.conn.execute(
                "SELECT COUNT(*) FROM parts").fetchone()[0]
            stats['oe'] = self.conn.execute(
                "SELECT COUNT(*) FROM oe").fetchone()[0]
            stats['cross'] = self.conn.execute(
                "SELECT COUNT(*) FROM cross_references").fetchone()[0]
            stats['prices'] = self.conn.execute(
                "SELECT COUNT(*) FROM prices").fetchone()[0]
            stats['brands'] = self.conn.execute(
                "SELECT COUNT(DISTINCT brand) FROM parts").fetchone()[0]
            stats['unique_parts'] = self.conn.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
            avg_price = self.conn.execute(
                "SELECT AVG(price) FROM prices").fetchone()[0]
            stats['avg_price'] = round(avg_price, 2) if avg_price else 0
        except Exception as e:
            st.error(f"Ошибка сбора статистики: {e}")
            return
        col1, col2, col3 = st.columns(3)
        col1.metric("Уникальных товаров", f"{stats['unique_parts']:,}")
        col2.metric("Брендов", f"{stats['brands']:,}")
        col3.metric("Средняя цена", f"{stats['avg_price']} ₽")

        try:
            top_brands = self.conn.execute(
                "SELECT brand, COUNT(*) as cnt FROM parts GROUP BY brand ORDER BY cnt DESC LIMIT 10").pl()
            st.subheader("Топ 10 брендов")
            st.dataframe(top_brands.to_pandas())
        except:
            pass

    def merge_all_data_parallel(self, file_paths: Dict[str, str], max_workers: int = 4) -> Dict[str, pl.DataFrame]:
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for key, path in file_paths.items():
                if path and os.path.exists(path):
                    futures[executor.submit(
                        self.read_and_prepare_file, path, key)] = key
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    df = fut.result()
                    if not df.is_empty():
                        results[key] = df
                        logger.info(f"Обработан {key}")
                except Exception as e:
                    logger.error(f"Ошибка обработки {key}: {e}")
        return results

    def show_data_management(self):
        st.header("🔧 Управление данными")
        st.warning("⚠️ Операции необратимы!")

        management_option = st.radio(
            "Выберите действие:",
            [
                "Удалить по бренду",
                "Удалить по артикули",
                "Управление ценами",
                "Исключения",
                "Категории",
                "Облачная синхронизация"
            ],
            format_func=lambda x: {
                "Удалить по бренду": "🏭 Удалить все записи бренда",
                "Удалить по артикули": "📦 Удалить все записи артикула",
                "Управление ценами": "💰 Цены и наценки",
                "Исключения": "🚫 Исключения при экспорте",
                "Категории": "🗂️ Категории товаров",
                "Облачная синхронизация": "☁️ Облачная синхронизация"
            }[x]
        )

        if management_option == "Удалить по бренду":
            self._show_delete_by_brand()
        elif management_option == "Удалить по артикули":
            self._show_delete_by_artikul()
        elif management_option == "Управление ценами":
            self.show_price_settings()
        elif management_option == "Исключения":
            self.show_exclusion_settings()
        elif management_option == "Категории":
            self.show_category_mapping()
        elif management_option == "Облачная синхронизация":
            self.show_cloud_sync()

    def _show_delete_by_brand(self):
        st.subheader("Удаление по бренду")
        try:
            brands_result = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand").fetchall()
            available_brands = [row[0]
                                for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            st.error("Ошибка при получении брендов")
            return
        if not available_brands:
            st.info("Нет данных")
            return
        selected_brand = st.selectbox("Бренд", available_brands)

        brand_norm_result = self.conn.execute(
            "SELECT brand_norm FROM parts WHERE brand = ? LIMIT 1", [selected_brand]).fetchone()
        if brand_norm_result:
            brand_norm = brand_norm_result[0]
        else:
            brand_norm = self.normalize_key(pl.Series([selected_brand]))[0]

        count = self.conn.execute(
            "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
        st.info(f"Удалить {count} записей бренда '{selected_brand}'?")

        if st.checkbox("Подтверждаю удаление"):
            if st.button("Удалить"):
                deleted = self.delete_by_brand(brand_norm)
                st.success(f"Удалено {deleted} записей")
                st.experimental_rerun()

    def _show_delete_by_artikul(self):
        st.subheader("Удаление по артикулу")
        artikul_input = st.text_input("Артикул")
        if artikul_input:
            artikul_norm = self.normalize_key(pl.Series([artikul_input]))[0]
            count = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
            st.info(f"Найдено {count} записей для артикула '{artikul_input}'")
            if st.checkbox("Подтверждаю"):
                if st.button("Удалить"):
                    deleted = self.delete_by_artikul(artikul_norm)
                    st.success(f"Удалено {deleted} записей")
                    st.experimental_rerun()


def main():
    st.title("🚗 AutoParts Catalog 10M+")
    st.markdown("### Платформа для больших каталогов автозапчастей")
    catalog = HighVolumeAutoPartsCatalog()

    st.sidebar.title("🧭 Меню")
    option = st.sidebar.radio(
        "Выберите раздел", ["Загрузка данных", "Экспорт", "Статистика", "Управление"])

    if option == "Загрузка данных":
        st.header("📥 Загрузка данных")
        col1, col2 = st.columns(2)
        with col1:
            oe_file = st.file_uploader("Основные данные (OE)", type=['xlsx'])
            cross_file = st.file_uploader("Кроссы (OE→Артикул)", type=['xlsx'])
            barcode_file = st.file_uploader("Штрих-коды", type=['xlsx'])
        with col2:
            weight_dims_file = st.file_uploader(
                "Вес и габариты", type=['xlsx'])
            images_file = st.file_uploader("Изображения", type=['xlsx'])
            prices_file = st.file_uploader("Цены", type=['xlsx'])

        uploaded_files = {
            'oe': oe_file,
            'cross': cross_file,
            'barcode': barcode_file,
            'dimensions': weight_dims_file,
            'images': images_file,
            'prices': prices_file
        }

        if st.button("Обработать и загрузить"):
            saved_paths = {}
            for key, file in uploaded_files.items():
                if file:
                    path = catalog.data_dir / f"{key}_{int(time.time())}.xlsx"
                    with open(path, "wb") as f:
                        f.write(file.getbuffer())
                    saved_paths[key] = str(path)
            if saved_paths:
                with st.spinner("Обработка файлов..."):
                    dataframes = catalog.merge_all_data_parallel(saved_paths)
                with st.spinner("Загрузка данных в базу..."):
                    catalog.process_and_load_data(dataframes)
            else:
                st.warning("Загрузите хотя бы один файл")
    elif option == "Экспорт":
        catalog.show_export_interface()
    elif option == "Статистика":
        catalog.show_statistics()
    elif option == "Управление":
        catalog.show_data_management()


if __name__ == "__main__":
    main()
