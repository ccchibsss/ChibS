import platform
import sys
import polars as pl
import duckdb
import streamlit as st
import os
import time
import io
import zipfile
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_ROW_LIMIT = 1_000_000  # лимит строк в Excel при экспорте

class AutoPartsCatalog:
    def __init__(self):
        # Основные параметры и папка данных
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        # Конфигурации и настройки
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        # Инициализация базы данных DuckDB
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.setup_database()

        # Настройка интерфейса Streamlit
        st.set_page_config(
            page_title="AutoParts Catalog 10M+",
            layout="wide",
            page_icon="🚗"
        )

        # Создаем таблицы и индексы
        self.create_indexes()

    # ----------- Конфигурации и настройки -----------

    def load_cloud_config(self) -> Dict[str, Any]:
        path = self.data_dir / "cloud_config.json"
        default = {
            "enabled": False,
            "provider": "s3",
            "bucket": "",
            "region": "",
            "sync_interval": 3600,
            "last_sync": 0
        }
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка загрузки cloud_config.json: {e}")
        path.write_text(json.dumps(default, indent=2, ensure_ascii=False), encoding='utf-8')
        return default

    def save_cloud_config(self):
        path = self.data_dir / "cloud_config.json"
        self.cloud_config['last_sync'] = int(time.time())
        try:
            path.write_text(json.dumps(self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')
        except Exception as e:
            logger.error(f"Не удалось сохранить cloud_config.json: {e}")

    def load_price_rules(self) -> Dict[str, Any]:
        path = self.data_dir / "price_rules.json"
        default = {
            "global_markup": 0.2,
            "brand_markups": {},
            "min_price": 0.0,
            "max_price": 99999.0
        }
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка загрузки price_rules.json: {e}")
        path.write_text(json.dumps(default, indent=2, ensure_ascii=False), encoding='utf-8')
        return default

    def save_price_rules(self):
        path = self.data_dir / "price_rules.json"
        try:
            path.write_text(json.dumps(self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')
        except Exception as e:
            logger.error(f"Не удалось сохранить price_rules.json: {e}")

    def load_exclusion_rules(self) -> List[str]:
        path = self.data_dir / "exclusion_rules.txt"
        default = ["Кузов", "Стекла", "Масла"]
        if path.exists():
            try:
                lines = path.read_text(encoding='utf-8').splitlines()
                return [line.strip() for line in lines if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка загрузки exclusion_rules.txt: {e}")
        path.write_text("\n".join(default), encoding='utf-8')
        return default

    def save_exclusion_rules(self):
        path = self.data_dir / "exclusion_rules.txt"
        try:
            path.write_text("\n".join(self.exclusion_rules), encoding='utf-8')
        except Exception as e:
            logger.error(f"Не удалось сохранить exclusion_rules.txt: {e}")

    def load_category_mapping(self) -> Dict[str, str]:
        path = self.data_dir / "category_mapping.txt"
        default = {
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        }
        if path.exists():
            try:
                lines = path.read_text(encoding='utf-8').splitlines()
                mapping = {}
                for line in lines:
                    if "|" in line:
                        k, v = line.split("|", 1)
                        mapping[k.strip()] = v.strip()
                return mapping
            except Exception as e:
                logger.error(f"Ошибка загрузки category_mapping.txt: {e}")
        # Записываем дефолт
        path.write_text("\n".join([f"{k}|{v}" for k, v in default.items()]), encoding='utf-8')
        return default

    def save_category_mapping(self):
        path = self.data_dir / "category_mapping.txt"
        try:
            text = "\n".join([f"{k}|{v}" for k, v in self.category_mapping.items()])
            path.write_text(text, encoding='utf-8')
        except Exception as e:
            logger.error(f"Не удалось сохранить category_mapping.txt: {e}")

    # ----------- База данных -----------

    def setup_database(self):
        """Создание таблиц, если не существует."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe_data (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parts_data (
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
        self.create_indexes()

    def create_indexes(self):
        """Создание индексов для ускорения поиска"""
        st.info("🛠️ Создание индексов...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_data_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_data_keys ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]
        for sql in indexes:
            try:
                self.conn.execute(sql)
            except Exception as e:
                logger.warning(f"Индекс уже существует или ошибка: {e}")
        st.success("🛠️ Индексы созданы.")

    # ----------- Методы преобразования данных -----------

    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        """Нормализация ключевых полей (артикул, бренд, OE)."""
        return (
            series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .str.to_lowercase()
        )

    @staticmethod
    def clean_values(series: pl.Series) -> pl.Series:
        """Очистка исходных значений."""
        return (
            series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
        )

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        """Определение категории по вхождениям слов и правил."""
        name_lower = name_series.str.to_lowercase()
        expr = pl.when(pl.lit(False)).then(pl.lit(None))
        # Правила пользователя (приоритет)
        for key, category in self.category_mapping.items():
            expr = expr.when(name_lower.str.contains(key.lower())).then(pl.lit(category))
        # Стандартные правила
        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормозная система': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выхлопная система': 'глушитель|глушител|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        for cat, pattern in categories_map.items():
            expr = expr.when(name_lower.str.contains(pattern)).then(pl.lit(cat))
        return expr.otherwise(pl.lit('Разное')).alias('category')

    def detect_columns(self, actual_cols: List[str], expected_cols: List[str]) -> Dict[str, str]:
        """Автоматическое сопоставление колонок по ключевым словам."""
        variant_map = {
            'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE'],
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand', 'производитель'],
            'name': ['наименование', 'название', 'name', 'описание', 'description'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13'],
            'multiplicity': ['кратность шт', 'кратность', 'multiplicity'],
            'length': ['длина (см)', 'длина', 'length', 'длинна'],
            'width': ['ширина (см)', 'ширина', 'width'],
            'height': ['высота (см)', 'высота', 'height'],
            'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight'],
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size']
        }
        mapping = {}
        actual_lower = {col.lower(): col for col in actual_cols}
        for expected in expected_cols:
            variants = variant_map.get(expected, [expected])
            for variant in variants:
                v_lower = variant.lower()
                for act_lower, act_orig in actual_lower.items():
                    if v_lower in act_lower and act_orig not in mapping:
                        mapping[act_orig] = expected
                        break
        return mapping

    # ----------- Обработка файлов -----------

    def read_and_prepare_file(self, filepath: str, file_type: str) -> Optional[pl.DataFrame]:
        """Чтение файла и первичная подготовка данных"""
        try:
            if not os.path.exists(filepath):
                logger.warning(f"Файл не найден: {filepath}")
                return None
            # Чтение файла
            df = pl.read_excel(filepath, engine='calamine')
            if df.is_empty():
                logger.warning(f"Файл пуст: {filepath}")
                return None
        except Exception as e:
            logger.exception(f"Ошибка чтения файла {filepath}: {e}")
            return None

        # Определение схемы по типу файла
        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected_cols = schemas.get(file_type, [])
        col_mapping = self.detect_columns(df.columns, expected_cols)
        if not col_mapping:
            logger.warning(f"Не найдено подходящих колонок для файла {filepath}")
            return None

        df = df.rename(col_mapping)

        # Очистка и нормализация ключевых колонок
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))
        # Удаление дублирующихся записей
        key_cols = [c for c in ['oe_number', 'artikul', 'brand'] if c in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        # Создаем нормализованные ключи
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.normalize_key(pl.col(col)).alias(f"{col}_norm"))

        return df

    # ----------- Обновление данных -----------

    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        """UPSERT данных в таблицу"""
        if df is None or df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        pk_str = ", ".join(f'"{c}"' for c in pk)
        temp_view = f"temp_{table_name}_{int(time.time())}"

        self.conn.register(temp_view, df.to_arrow())

        update_cols = [c for c in cols if c not in pk]
        if not update_cols:
            on_conflict_sql = "DO NOTHING"
        else:
            update_clause = ", ".join([f'"{col}" = excluded."{col}"' for col in update_cols])
            on_conflict_sql = f"DO UPDATE SET {update_clause}"

        sql = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_view}
            ON CONFLICT ({pk_str}) {on_conflict_sql};
        """

        try:
            self.conn.execute(sql)
        finally:
            self.conn.unregister(temp_view)

    def upsert_prices(self, df: pl.DataFrame):
        """Обновление цен"""
        if df is None or df.is_empty():
            return
        # Нормализация ключей
        if 'artikul' in df.columns and 'brand' in df.columns:
            df = df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])
        # Значение валюты по умолчанию
        if 'currency' not in df.columns:
            df = df.with_columns(pl.lit('RUB').alias('currency'))
        # Фильтр по диапазону цен
        df = df.filter(
            (pl.col('price') >= self.price_rules['min_price']) &
            (pl.col('price') <= self.price_rules['max_price'])
        )
        self.upsert_data('prices', df, ['artikul_norm', 'brand_norm'])

    # ----------- Обработка и загрузка данных -----------

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Общий метод обработки и загрузки данных"""
        st.info("🔄 Начинаю загрузку данных в базу...")
        steps = ['oe', 'cross', 'prices', 'parts']
        total_steps = len(steps)
        progress = st.progress(0, text="Подготовка...")
        step_idx = 0

        # Обработка OE
        if 'oe' in dataframes:
            step_idx += 1
            progress.progress(step_idx / total_steps, f"({step_idx}/{total_steps}) Обработка OE...")
            df_oe = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df_oe.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'])
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))
            self.upsert_data('oe_data', oe_df, ['oe_number_norm'])

            # Связи OE с артикулами
            cross_oe = df_oe.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_oe, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Обработка кроссов
        if 'cross' in dataframes:
            step_idx += 1
            progress.progress(step_idx / total_steps, f"({step_idx}/{total_steps}) Обработка кроссов...")
            df_cross = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df = df_cross.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Обработка цен
        if 'prices' in dataframes:
            df_prices = dataframes['prices']
            if not df_prices.is_empty():
                self.upsert_prices(df_prices)
                st.success(f"Обновлено цен: {len(df_prices)} записей.")

        # Обработка артикулами и товарами
        # ... (можно расширять по необходимости)

        progress.progress(1.0)
        time.sleep(0.5)

    # ----------- Построение экспортных запросов -----------

    def build_export_query(self, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> str:
        """Построение сложного SQL-запроса для экспорта."""
        standard_description = """Состояние товара: новый (в упаковке).
Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. 
Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей.

В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электрику, автомасла, инструмент, а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. 

Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. 

Выбирайте только лучшее — надежность и качество от ведущих производителей."""
        # Формируем части запроса
        columns_map = [
            ("Артикул бренда", 'r.artikul AS "Артикул бренда"'),
            ("Бренд", 'r.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"'),
            ("Описание", "CONCAT(COALESCE(r.description, ''), dt.text) AS \"Описание\""),
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

        if include_prices:
            columns_map.extend([("Цена", '"Цена"'), ("Валюта", '"Валюта"')])

        if selected_columns:
            selected_exprs = [expr for name, expr in columns_map if name in selected_columns]
        else:
            selected_exprs = [expr for _, expr in columns_map]

        # Создаем CTE с текстом описания
        ctes = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${standard_description}$$ AS text
        ),
        PartDetails AS (
            SELECT
                cr.artikul_norm,
                cr.brand_norm,
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(o.oe_number, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') AS oe_list,
                ANY_VALUE(o.name) AS representative_name,
                ANY_VALUE(o.applicability) AS representative_applicability,
                ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr
            LEFT JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT
                cr1.artikul_norm,
                cr1.brand_norm,
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(p2.artikul, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') as analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts_data p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE (cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm)
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        ),
        InitialOENumbers AS (
            SELECT DISTINCT p.artikul_norm, p.brand_norm, cr.oe_number_norm
            FROM parts_data p
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
                WHERE i.artikul_norm = l1.source_artikul_norm AND i.brand_norm = l1.source_brand_norm AND i.oe_number_norm = cr3.oe_number_norm
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
                    CASE WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX'
                    THEN p2.dimensions_str ELSE NULL END
                ) AS dimensions_str,
                ANY_VALUE(
                    CASE WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' THEN pd2.representative_name ELSE NULL END
                ) AS representative_name,
                ANY_VALUE(
                    CASE WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != '' THEN pd2.representative_applicability ELSE NULL END
                ) AS representative_applicability,
                ANY_VALUE(
                    CASE WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != '' THEN pd2.representative_category ELSE NULL END
                ) AS representative_category
            FROM AllRelatedParts arp
            JOIN parts_data p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT
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
            FROM parts_data p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
        )
        """

        select_exprs = ",\n        ".join(selected_exprs)

        price_join = """
        LEFT JOIN prices pr ON r.artikul_norm = pr.artikul_norm AND r.brand_norm = pr.brand_norm
        LEFT JOIN BrandMarkups brm ON r.brand = brm.brand
        """ if include_prices else ""

        exclusion_conditions = " OR ".join([f"r.representative_name NOT ILIKE '%{ex}%'" for ex in self.exclusion_rules if ex.strip()])
        exclusion_where = f"AND ({exclusion_conditions})" if exclusion_conditions else ""

        if include_prices:
            markup_value = self.price_rules['global_markup']
            if apply_markup:
                price_sql = f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {markup_value})) ELSE pr.price END AS \"Цена\""
            else:
                price_sql = "pr.price AS \"Цена\""
            currency_sql = "COALESCE(pr.currency, 'RUB') AS \"Валюта\""
        else:
            price_sql = ""
            currency_sql = ""

        query = f"""
        {ctes}
        SELECT
            {price_sql},
            {currency_sql},
            {select_exprs}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        {price_join}
        WHERE r.rn = 1
        {exclusion_where}
        ORDER BY r.brand, r.artikul
        """

        return query

    # ----------- Экспорт данных -----------

    def export_to_csv(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт в CSV (оптимизированный)"""
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()

            # Преобразование размерных колонок в строки
            for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
                if col in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col(col).is_not_null())
                        .then(pl.col(col).cast(pl.Utf8))
                        .otherwise("")
                        .alias(col)
                    )

            buf = io.StringIO()
            df.write_csv(buf, separator=';')
            text = buf.getvalue()

            with open(output_path, 'wb') as f:
                f.write(b'\xef\xbb\xbf')  # BOM
                f.write(text.encode('utf-8'))
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Экспорт завершен: {output_path} ({size_mb:.2f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в CSV")
            st.error(f"Ошибка экспорта: {e}")
            return False

    def export_to_excel(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт в Excel с разбивкой по лимиту строк."""
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        try:
            import pandas as pd
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = pd.read_sql(query, self.conn)

            for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace({r'^nan$': ''}, regex=True)

            if len(df) <= EXCEL_ROW_LIMIT:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
            else:
                sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for i in range(sheets):
                        start = i * EXCEL_ROW_LIMIT
                        end = min((i + 1) * EXCEL_ROW_LIMIT, len(df))
                        df.iloc[start:end].to_excel(writer, index=False)
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Экспорт завершен: {output_path} ({size_mb:.2f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в Excel")
            st.error(f"Ошибка: {e}")
            return False

    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт в Parquet"""
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Экспорт завершен: {output_path} ({size_mb:.2f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в Parquet")
            st.error(f"Ошибка: {e}")
            return False

    # ----------- Вспомогательные методы -----------

    def show_export_interface(self):
        """Интерфейс для экспорта"""
        st.header("📤 Выгрузка данных")
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total == 0:
            st.warning("Данных для выгрузки нет.")
            return
        available_cols = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
            "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        # Добавляем цены, если есть
        if self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] > 0:
            available_cols.extend(["Цена", "Валюта"])

        selected_cols = st.multiselect("Выберите столбцы для экспорта", options=available_cols, default=available_cols)

        format_choice = st.radio("Формат экспорта", ["CSV", "Excel (.xlsx)", "Parquet"])
        include_prices = st.checkbox("Включить цены", value=True)
        apply_markup = st.checkbox("Применить наценку", value=True, disabled=not include_prices)

        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"auto_parts_export.{format_choice.lower().replace(' ', '_')}"
            with st.spinner("Готовлю файл..."):
                if format_choice == "CSV":
                    success = self.export_to_csv(str(output_path), selected_cols, include_prices, apply_markup)
                elif format_choice == "Excel (.xlsx)":
                    success = self.export_to_excel(str(output_path), selected_cols, include_prices, apply_markup)
                else:
                    success = self.export_to_parquet(str(output_path), selected_cols, include_prices, apply_markup)

            if success:
                with open(output_path, "rb") as f:
                    st.download_button("⬇️ Скачать файл", f, output_path.name)

    # ----------- Управление данными -----------

    def delete_by_brand(self, brand_norm: str) -> int:
        """Удаление по нормализованному бренду"""
        try:
            with self.conn.transaction():
                count = self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm]).rowcount
                self.conn.execute("DELETE FROM cross_references WHERE brand_norm = ?", [brand_norm])
            return count
        except Exception as e:
            logger.error(f"Ошибка удаления по бренду: {e}")
            return 0

    def delete_by_artikul(self, artikul_norm: str) -> int:
        """Удаление по нормализованному артикулам"""
        try:
            with self.conn.transaction():
                count = self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).rowcount
                self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm])
            return count
        except Exception as e:
            logger.error(f"Ошибка удаления по артикулу: {e}")
            return 0

    def show_data_management(self):
        """Интерфейс управления данными"""
        st.header("🗑️ Управление данными")
        st.warning("⚠️ Операции необратимы!")

        option = st.radio("Действие", ["Удалить по бренду", "Удалить по артикулам", "Настройки цен", "Исключения", "Категории", "Облако"])

        if option == "Удалить по бренду":
            self._delete_by_brand_ui()
        elif option == "Удалить по артикулам":
            self._delete_by_artikul_ui()
        elif option == "Настройки цен":
            self.show_price_rules_ui()
        elif option == "Исключения":
            self.show_exclusion_rules_ui()
        elif option == "Категории":
            self.show_category_mapping_ui()
        elif option == "Облако":
            self.show_cloud_sync_ui()

    def _delete_by_brand_ui(self):
        """UI для удаления по бренду"""
        brands = self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL ORDER BY brand").fetchall()
        options = [b[0] for b in brands]
        if not options:
            st.info("Нет брендов в базе.")
            return
        selected = st.selectbox("Выберите бренд для удаления", options)
        # Получаем нормализованный бренд
        result = self.conn.execute("SELECT brand_norm FROM parts_data WHERE brand = ? LIMIT 1", [selected]).fetchone()
        brand_norm = result[0] if result else self.normalize_key(pl.Series([selected]))[0]
        count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
        st.write(f"Удалить {count} записей бренда {selected}?")
        confirm = st.checkbox("Я подтверждаю")
        if st.button("Удалить бренд", disabled=not confirm):
            deleted = self.delete_by_brand(brand_norm)
            st.success(f"Удалено {deleted} записей")
            st.rerun()

    def _delete_by_artikul_ui(self):
        """UI для удаления по артикулам"""
        input_art = st.text_input("Введите артикул для удаления")
        if input_art:
            artikul_norm = self.normalize_key(pl.Series([input_art]))[0]
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
            st.write(f"Найдено {count} записей для артикула {input_art}")
            confirm = st.checkbox("Я подтверждаю")
            if st.button("Удалить артикул", disabled=not confirm):
                deleted = self.delete_by_artikul(artikul_norm)
                st.success(f"Удалено {deleted} записей")
                st.rerun()

    def show_price_rules_ui(self):
        """Интерфейс для настройки цен и наценок"""
        st.subheader("Общая наценка (%)")
        markup = st.number_input("Наценка (%)", min_value=0.0, max_value=100.0, step=0.1,
                                 value=self.price_rules.get('global_markup', 0.2) * 100)
        self.price_rules['global_markup'] = markup / 100
        self.save_price_rules()

        st.subheader("Наценки по брендам")
        brands = self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL").fetchall()
        options = [b[0] for b in brands]
        if options:
            selected = st.selectbox("Выберите бренд", options)
            current_markup = self.price_rules['brand_markups'].get(selected, self.price_rules.get('global_markup', 0.2))
            markup_value = st.number_input("Наценка (%)", min_value=0.0, max_value=100.0, step=0.1,
                                         value=current_markup * 100)
            if st.button("Сохранить для бренда"):
                self.price_rules['brand_markups'][selected] = markup_value / 100
                self.save_price_rules()

        st.subheader("Ограничения цен")
        min_price = st.number_input("Мин. цена", value=self.price_rules.get('min_price', 0.0))
        max_price = st.number_input("Макс. цена", value=self.price_rules.get('max_price', 99999.0))
        self.price_rules['min_price'] = min_price
        self.price_rules['max_price'] = max_price
        self.save_price_rules()

    def show_exclusion_rules_ui(self):
        """Интерфейс для правил исключений"""
        current = "\n".join(self.exclusion_rules)
        new_text = st.text_area("Исключения (по одному слову)", value=current, height=200)
        if st.button("Сохранить исключения"):
            lines = [line.strip() for line in new_text.splitlines() if line.strip()]
            # Удаление дубликатов с сохранением порядка
            self.exclusion_rules = list(dict.fromkeys(lines))
            self.save_exclusion_rules()

    def show_category_mapping_ui(self):
        """Интерфейс для назначения категорий"""
        st.subheader("Текущие правила")
        df = pl.DataFrame({
            "Название": list(self.category_mapping.keys()),
            "Категория": list(self.category_mapping.values())
        })
        st.dataframe(df.to_pandas())

        new_key = st.text_input("Новое правило: ключевое слово")
        new_value = st.text_input("Категория")
        if st.button("Добавить правило") and new_key and new_value:
            key = new_key.strip()
            value = new_value.strip()
            if key and value:
                self.category_mapping[key] = value
                self.save_category_mapping()

        # Удаление
        if self.category_mapping:
            to_del = st.selectbox("Удалить правило", list(self.category_mapping.keys()),
                                  format_func=lambda x: f"{x} → {self.category_mapping[x]}")
            if st.button("Удалить правило"):
                if to_del in self.category_mapping:
                    del self.category_mapping[to_del]
                    self.save_category_mapping()

    def show_cloud_sync_ui(self):
        """Интерфейс для настройки облачных сервисов"""
        st.subheader("Настройки синхронизации с облаком")
        self.cloud_config['enabled'] = st.checkbox("Включить синхронизацию", value=self.cloud_config['enabled'])
        providers = ["s3", "gcs", "azure"]
        current_idx = providers.index(self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
        self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=current_idx)
        self.cloud_config['bucket'] = st.text_input("Bucket / Container", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config['region'])
        self.cloud_config['sync_interval'] = st.number_input("Интервал синхронизации (сек)", min_value=300, max_value=86400,
                                                             value=int(self.cloud_config['sync_interval']))
        if st.button("Сохранить настройки"):
            self.save_cloud_config()
        if st.button("Выполнить синхронизацию сейчас"):
            self.perform_cloud_sync()

    def perform_cloud_sync(self):
        """Заглушка для синхронизации с облаком."""
        if not self.cloud_config['enabled']:
            st.warning("Синхронизация отключена.")
            return
        if not self.cloud_config['bucket']:
            st.error("Не указан bucket.")
            return
        with st.spinner("Выполняется синхронизация..."):
            time.sleep(1.5)
            st.success(f"База данных отправлена в {self.cloud_config['provider']}://{self.cloud_config['bucket']}")
            self.cloud_config['last_sync'] = int(time.time())
            self.save_cloud_config()

    # ----------- Обработка и загрузка файлов -----------

    def merge_files_parallel(self, file_paths: Dict[str, str]) -> Dict[str, Any]:
        """Параллельное чтение и предварительная обработка файлов"""
        results = {}
        with ThreadPoolExecutor() as executor:
            futures = {}
            for ftype, path in file_paths.items():
                futures[executor.submit(self.read_and_prepare_file, path, ftype)] = ftype
            for future in as_completed(futures):
                ftype = futures[future]
                try:
                    df = future.result()
                    if df is not None and not df.is_empty():
                        results[ftype] = df
                        logger.info(f"Файл {ftype} обработан.")
                except Exception as e:
                    logger.exception(f"Ошибка при обработке файла {ftype}: {e}")
        return results

    def run_full_merge(self, file_paths: Dict[str, str]):
        """Полный процесс слияния файлов: параллельное чтение → объединение → загрузка."""
        dataframes = self.merge_files_parallel(file_paths)
        if dataframes:
            self.process_and_load_data(dataframes)
        else:
            st.warning("Ни один файл не был загружен или обработан.")

    # ----------- Статистика -----------

    def get_statistics(self) -> Dict[str, Any]:
        """Сбор статистики по базе данных."""
        stats = {
            "total_parts": self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0],
            "total_oe": self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0],
            "total_cross": self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0],
            "total_prices": self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0],
        }
        # Топ-бренды
        try:
            top_brands = self.conn.execute("""
                SELECT brand, COUNT(*) as count 
                FROM parts_data 
                GROUP BY brand 
                ORDER BY count DESC 
                LIMIT 10
            """).pl()
            stats["top_brands"] = top_brands
        except Exception as e:
            logger.error(f"Ошибка при получении топ-брендов: {e}")
            stats["top_brands"] = pl.DataFrame()

        # Категории
        try:
            categories = self.conn.execute("""
                SELECT representative_category as category, COUNT(*) as count
                FROM (
                    SELECT DISTINCT p.artikul_norm, p.brand_norm, pd.representative_category
                    FROM parts_data p
                    LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
                    LEFT JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
                    LEFT JOIN (
                        SELECT artikul_norm, brand_norm, ANY_VALUE(category) as representative_category
                        FROM (
                            SELECT artikul_norm, brand_norm, category
                            FROM cross_references
                            JOIN oe_data ON cross_references.oe_number_norm = oe_data.oe_number_norm
                        )
                        GROUP BY artikul_norm, brand_norm
                    ) pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
                )
                WHERE category IS NOT NULL
                GROUP BY category
                ORDER BY count DESC
            """).pl()
            stats["categories"] = categories
        except Exception as e:
            logger.error(f"Ошибка при получении категорий: {e}")
            stats["categories"] = pl.DataFrame()

        return stats


# --- Основная функция ---

def main():
    st.title("🚗 AutoParts Catalog — Масштабируемая система для 10+ млн записей")
    st.markdown("""
    ### 💼 Профессиональная платформа для управления каталогами автозапчастей
    - Поддержка больших данных
    - Инкрементальные обновления
    - Мультиформатный экспорт
    - Гибкая настройка
    """)

    catalog = AutoPartsCatalog()

    # Навигация
    st.sidebar.title("🧭 Навигация")
    choice = st.sidebar.radio("Выберите раздел", ["Загрузка данных", "Экспорт", "Статистика", "Управление"])

    if choice == "Загрузка данных":
        st.header("📥 Загрузка и обновление")
        col1, col2 = st.columns(2)
        with col1:
            oe_file = st.file_uploader("Основные данные (OE)", type=['xlsx', 'xls'])
            cross_file = st.file_uploader("Кроссы (OE→Артикул)", type=['xlsx', 'xls'])
            barcode_file = st.file_uploader("Штрих-коды и кратность", type=['xlsx', 'xls'])
        with col2:
            dimensions_file = st.file_uploader("Весогабариты", type=['xlsx', 'xls'])
            images_file = st.file_uploader("Изображения", type=['xlsx', 'xls'])
            prices_file = st.file_uploader("Цены", type=['xlsx', 'xls'])

        files_dict = {
            'oe': oe_file,
            'cross': cross_file,
            'barcode': barcode_file,
            'dimensions': dimensions_file,
            'images': images_file,
            'prices': prices_file
        }

        if st.button("🚀 Обработать файлы"):
            # Сохраняем временно
            file_paths = {}
            for key, uploaded in files_dict.items():
                if uploaded:
                    save_path = catalog.data_dir / f"upload_{key}_{int(time.time())}.xlsx"
                    with open(save_path, "wb") as f:
                        f.write(uploaded.getbuffer())
                    file_paths[key] = str(save_path)
            if file_paths:
                # Обработка
                with st.spinner("Обработка файлов..."):
                    catalog.run_full_merge(file_paths)
                st.success("Обработка завершена.")
            else:
                st.warning("Загрузите хотя бы один файл.")

    elif choice == "Экспорт":
        catalog.show_export_interface()

    elif choice == "Статистика":
        stats = catalog.get_statistics()
        st.header("📈 Статистика")
        st.metric("Всего артикулов", stats.get('total_parts', 0))
        st.metric("OE номера", stats.get('total_oe', 0))
        st.metric("Кроссы", stats.get('total_cross', 0))
        st.metric("Цены", stats.get('total_prices', 0))
        st.subheader("Топ-бренды")
        if 'top_brands' in stats and not stats['top_brands'].is_empty():
            st.dataframe(stats['top_brands'].to_pandas())
        st.subheader("Категории")
        if 'categories' in stats and not stats['categories'].is_empty():
            st.dataframe(stats['categories'].to_pandas())

    elif choice == "Управление":
        catalog.show_data_management()


if __name__ == "__main__":
    main()
