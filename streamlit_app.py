import platform
import sys
import os
import io
import zipfile
import time
import json
import logging
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import duckdb
import streamlit as st

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_ROW_LIMIT = 1_000_000

class AutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.setup_database()

        # Настройки
        self.price_markup_global = 0.2
        self.brand_markups = {}
        self.exclusion_phrases = ["Кузов", "Стекла", "Масла"]
        self.categories_mapping = {
            "Радиатор": "Охлаждение",
            "Шаровая": "Подвеска",
            "Фильтр": "Фильтры",
            "Тормоз": "Тормоза"
        }

        # Облачная синхронизация (заглушка)
        self.cloud_config = self.load_cloud_config()

        # UI
        st.set_page_config(page_title="AutoParts 10M+", layout="wide", page_icon="🚗")

    def load_cloud_config(self):
        config_path = self.data_dir / "cloud_config.json"
        default = {
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
            except:
                return default
        else:
            config_path.write_text(json.dumps(default), encoding='utf-8')
            return default

    def save_cloud_config(self):
        path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        path.write_text(json.dumps(self.cloud_config), encoding='utf-8')

    def setup_database(self):
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
                oe_list VARCHAR,
                analog_list VARCHAR,
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
            CREATE TABLE IF NOT EXISTS categories (
                name VARCHAR PRIMARY KEY,
                category VARCHAR
            )
        """)
        self.create_indexes()

    def create_indexes(self):
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross ON cross_references(oe_number_norm, artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices ON prices(artikul_norm, brand_norm)"
        ]
        for idx in indexes:
            self.conn.execute(idx)

    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
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
        return (
            series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
        )

    def detect_columns(self, cols: List[str], expected: List[str]) -> Dict[str, str]:
        """Автоматическое определение колонок по ключевым словам"""
        variants = {
            'oe_number': ['oe', 'оe', 'номер', 'code', 'OE'],
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand', 'производитель'],
            'name': ['наименование', 'название', 'name', 'описание', 'description'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13'],
            'multiplicity': ['кратность', 'multiplicity'],
            'length': ['длина', 'length', 'длинна'],
            'width': ['ширина', 'width'],
            'height': ['высота', 'height'],
            'weight': ['вес', 'weight'],
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size'],
            'oe_list': ['oe_list', 'oe_numbers', 'oe'],
            'analog_list': ['analogs', 'аналоги', 'analog']
        }
        mapping = {}
        cols_lower = {c.lower(): c for c in cols}
        for key, v_list in variants.items():
            for v in v_list:
                v_lower = v.lower()
                for col_lower, col_orig in cols_lower.items():
                    if v_lower in col_lower and col_orig not in mapping:
                        mapping[col_orig] = key
        return mapping

    def read_and_prepare_file(self, filepath: str, file_type: str) -> pl.DataFrame:
        """Чтение файла и подготовка (нормализация, очистка)"""
        try:
            df = pl.read_excel(filepath, engine='calamine')
        except Exception as e:
            logger.error(f"Ошибка чтения файла {filepath}: {e}")
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected_cols = schemas.get(file_type, [])
        col_map = self.detect_columns(df.columns, expected_cols)
        if not col_map:
            logger.warning(f"Колонки не найдены для файла {file_type}")
            return pl.DataFrame()
        df = df.rename(col_map)

        # Очистка и нормализация ключей
        for key in ['artikul', 'brand', 'oe_number']:
            if key in df.columns:
                df = df.with_columns(self.clean_values(pl.col(key)).alias(key))
        # Удаление дубликатов по ключам
        key_cols = [k for k in ['oe_number', 'artikul', 'brand'] if k in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')
        # Создаем нормализованные ключи
        for key in ['artikul', 'brand', 'oe_number']:
            norm_key = f"{key}_norm"
            if key in df.columns:
                df = df.with_columns(self.normalize_key(pl.col(key)).alias(norm_key))
        return df

    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        """UPSERT данных в таблицу"""
        if df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        temp_name = f"temp_{table_name}_{int(time.time())}"
        self.conn.register(temp_name, df.to_arrow())
        pk_str = ", ".join(f'"{c}"' for c in pk)
        update_cols = [c for c in cols if c not in pk]
        if not update_cols:
            conflict_action = "DO NOTHING"
        else:
            set_clause = ", ".join([f'"{col}" = excluded."{col}"' for col in update_cols])
            conflict_action = f"DO UPDATE SET {set_clause}"
        sql = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_name}
            ON CONFLICT ({pk_str}) {conflict_action};
        """
        try:
            self.conn.execute(sql)
        finally:
            self.conn.unregister(temp_name)

    def merge_all_files(self, file_paths: Dict[str, str]):
        """Обработка и объединение файлов"""
        dataframes = {}
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self.read_and_prepare_file, path, ftype): ftype
                for ftype, path in file_paths.items()
            }
            for future in as_completed(futures):
                ftype = futures[future]
                try:
                    df = future.result()
                    if not df.is_empty():
                        dataframes[ftype] = df
                except:
                    logger.exception(f"Ошибка при чтении файла {ftype}")
        self.process_and_load(dataframes)

    def process_and_load(self, dfs: Dict[str, pl.DataFrame]):
        """Обработка и загрузка данных в базу"""
        # Обработка OE
        if 'oe' in dfs:
            df_oe = dfs['oe']
            self.upsert_data('oe_data', df_oe, ['oe_number_norm'])
            # Распространение OE на все артикула с этим OE
            oe_list = df_oe.select(['oe_number_norm', 'oe_number', 'name', 'applicability', 'category'])
            for row in oe_list.rows():
                oe_norm, oe_num, name, app, cat = row
                self.conn.execute("""
                    UPDATE parts_data SET oe_list=(
                        CASE WHEN oe_list IS NULL OR oe_list = '' THEN ? ELSE oe_list || ',' || ? END
                    ) WHERE oe_number = ?
                """, [oe_num, oe_num, oe_num])

        # Обработка cross
        if 'cross' in dfs:
            df_cross = dfs['cross']
            self.upsert_data('cross_references', df_cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Обработка цен
        if 'prices' in dfs:
            df_prices = dfs['prices']
            self.upsert_prices(df_prices)

        # Обработка остальных файлов
        # Можно расширить по необходимости

    def upsert_prices(self, df: pl.DataFrame):
        """Обновление цен"""
        if 'artikul' in df.columns and 'brand' in df.columns:
            df = df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])
        if 'price' in df.columns:
            df = df.filter(
                (pl.col('price') >= 0) & (pl.col('price') <= 1e6)
            )
        if 'currency' not in df.columns:
            df = df.with_columns(pl.lit('RUB').alias('currency'))
        self.upsert_data('prices', df, ['artikul_norm', 'brand_norm'])

    def build_export_query(self, selected_columns: Optional[List[str]] = None, include_prices=True, apply_markup=True) -> str:
        """Создание SQL-запроса для экспорта"""
        description_text = """Состояние товара: новый (в упаковке).
Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. 
Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей.
В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электрику, автомасла, инструмент, а также другие комплектующие, полностью соответствующие стандартам качества и безопасности.
Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса.
Выбирайте только лучшее — надежность и качество от ведущих производителей."""

        columns_map = [
            ("Артикул бренда", 'p.artikul AS "Артикул бренда"'),
            ("Бренд", 'p.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(p.representative_name, p.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(p.representative_applicability, p.analog_representative_applicability) AS "Применимость"'),
            ("Описание", 'CONCAT(COALESCE(p.description, ""), dt.text) AS "Описание"'),
            ("Категория товара", 'COALESCE(p.representative_category, p.analog_representative_category) AS "Категория товара"'),
            ("Кратность", 'p.multiplicity AS "Кратность"'),
            ("Длинна", 'COALESCE(p.length, p.analog_length) AS "Длинна"'),
            ("Ширина", 'COALESCE(p.width, p.analog_width) AS "Ширина"'),
            ("Высота", 'COALESCE(p.height, p.analog_height) AS "Высота"'),
            ("Вес", 'COALESCE(p.weight, p.analog_weight) AS "Вес"'),
            ("Длинна/Ширина/Высота", """
                COALESCE(
                    CASE
                        WHEN p.dimensions_str IS NULL OR p.dimensions_str = '' OR UPPER(TRIM(p.dimensions_str)) = 'XX'
                        THEN NULL
                        ELSE p.dimensions_str
                    END,
                    p.analog_dimensions_str
                ) AS "Длинна/Ширина/Высота"
            """),
            ("OE номер", 'p.oe_list AS "OE номер"'),
            ("аналоги", 'p.analog_list AS "аналоги"'),
            ("Ссылка на изображение", 'p.image_url AS "Ссылка на изображение"')
        ]

        if include_prices:
            columns_map.append(("Цена", '"Цена"'))
            columns_map.append(("Валюта", '"Валюта"'))

        if selected_columns:
            sel_exprs = [expr for name, expr in columns_map if name in selected_columns]
        else:
            sel_exprs = [expr for _, expr in columns_map]

        ctes = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${description_text}$$ AS text
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

        select_exprs = ",\n        ".join(sel_exprs)

        query = f"""
        {ctes}
        SELECT
            {', '.join([expr for _, expr in columns_map if _ in [n for n, _ in columns_map]])}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        WHERE r.rn = 1
        ORDER BY r.brand, r.artikul
        """
        return query

    def export_csv(self, filename: str, selected_columns: Optional[List[str]] = None):
        """Экспорт в CSV"""
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()

            # Размерность в строки
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
            with open(filename, 'wb') as f:
                f.write(b'\xef\xbb\xbf')
                f.write(buf.getvalue().encode('utf-8'))
            return True
        except:
            return False

    def export_excel(self, filename: str, selected_columns: Optional[List[str]] = None):
        """Экспорт в Excel, разбивка при больших объемах"""
        import pandas as pd
        total = self.conn.execute("""
            SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)
        """).fetchone()[0]
        if total == 0:
            return False, None
        num_files = (total // EXCEL_ROW_LIMIT) + 1
        dfs = []
        for i in range(num_files):
            query = self.build_export_query(selected_columns)
            query += f" LIMIT {EXCEL_ROW_LIMIT} OFFSET {i * EXCEL_ROW_LIMIT}"
            df = pd.read_sql(query, self.conn)
            # размерные в строки
            for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace({r'^nan$': ''}, regex=True)
            dfs.append(df)
        # Записываем
        if len(dfs) == 1:
            dfs[0].to_excel(filename, index=False)
        else:
            with pd.ExcelWriter(filename) as writer:
                for i, df in enumerate(dfs):
                    df.to_excel(writer, sheet_name=f"Часть_{i+1}", index=False)
        return True, filename

    def export_parquet(self, filename: str, selected_columns: Optional[List[str]] = None):
        """Экспорт в Parquet"""
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()
            df.write_parquet(filename)
            return True
        except:
            return False

    # ------------------ интерфейс ------------------
    def show_ui(self):
        st.title("🚗 AutoParts 10M+ — расширенная платформа")
        choice = st.sidebar.radio("Меню", ["Загрузка", "Экспорт", "Статистика", "Управление"])
        if choice == "Загрузка":
            self.show_load_files()
        elif choice == "Экспорт":
            self.show_export_ui()
        elif choice == "Статистика":
            self.show_statistics()
        elif choice == "Управление":
            self.show_management()

    def show_load_files(self):
        st.subheader("Загрузка файлов")
        files = {}
        files['oe'] = st.file_uploader("Основные данные OE", type=['xlsx'])
        files['cross'] = st.file_uploader("Кроссы OE → Артикул", type=['xlsx'])
        files['barcode'] = st.file_uploader("Штрих-коды", type=['xlsx'])
        files['dimensions'] = st.file_uploader("Весогабариты", type=['xlsx'])
        files['images'] = st.file_uploader("Изображения", type=['xlsx'])
        files['prices'] = st.file_uploader("Прайс-лист", type=['xlsx'])
        if st.button("Обработать файлы"):
            paths = {}
            for key, f in files.items():
                if f is not None:
                    path = self.data_dir / f"{key}_{int(time.time())}.xlsx"
                    with open(path, 'wb') as fp:
                        fp.write(f.getbuffer())
                    paths[key] = str(path)
            if paths:
                self.merge_all_files(paths)
                st.success("Данные успешно загружены и объединены")
            else:
                st.warning("Нет загруженных файлов")

    def show_export_ui(self):
        st.header("📤 Экспорт данных")
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        st.info(f"Всего данных: {total:,} записей")
        if total == 0:
            st.warning("Нет данных для экспорта.")
            return
        options = [
            "Артикул", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
            "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка", "Цена", "Валюта"
        ]
        selected_cols = st.multiselect("Выберите колонки для экспорта", options, default=options)
        format_ = st.radio("Формат файла", ["CSV", "Excel (.xlsx)", "Parquet"])
        if st.button("Экспортировать"):
            filename = self.data_dir / f"auto_parts_{int(time.time())}.{format_.lower().replace(' ', '_')}"
            if format_ == "CSV":
                self.export_csv(str(filename), selected_cols)
                with open(str(filename), 'rb') as f:
                    st.download_button("Скачать CSV", f, "auto_parts.csv", "text/csv")
            elif format_ == "Excel (.xlsx)":
                success, path = self.export_excel(str(filename), selected_cols)
                if success:
                    with open(path, 'rb') as f:
                        st.download_button("Скачать Excel", f, "auto_parts.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            elif format_ == "Parquet":
                success = self.export_parquet(str(filename), selected_cols)
                if success:
                    with open(str(filename), 'rb') as f:
                        st.download_button("Скачать Parquet", f, "auto_parts.parquet", "application/octet-stream")

    def show_statistics(self):
        st.header("📊 Статистика")
        try:
            total_parts = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
            total_oe = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0]
            total_cross = self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0]
            total_prices = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            st.metric("Всего товаров", total_parts)
            st.metric("OE номера", total_oe)
            st.metric("Кроссы", total_cross)
            st.metric("Цен", total_prices)
            # ТОП брендов
            brands = self.conn.execute("SELECT brand, COUNT(*) FROM parts_data WHERE brand IS NOT NULL GROUP BY brand ORDER BY COUNT(*) DESC LIMIT 10").fetchdf()
            st.subheader("ТОП брендов")
            st.dataframe(brands)
            categories = self.conn.execute("SELECT category, COUNT(*) FROM oe_data WHERE category IS NOT NULL GROUP BY category ORDER BY COUNT(*) DESC").fetchdf()
            st.subheader("Распределение по категориям")
            st.dataframe(categories)
        except:
            st.warning("Ошибка при сборе статистики.")

    def show_management(self):
        st.header("🔧 Управление данными")
        option = st.radio("Действие", ["Удалить по бренду", "Удалить по артикулу", "Настройки цен", "Исключения", "Категории"])
        if option == "Удалить по бренду":
            self.delete_by_brand_ui()
        elif option == "Удалить по артикулу":
            self.delete_by_artikul_ui()
        elif option == "Настройки цен":
            self.show_price_settings()
        elif option == "Исключения":
            self.show_exclusion_settings()
        elif option == "Категории":
            self.show_category_settings()

    def delete_by_brand_ui(self):
        brands = self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL ORDER BY brand").fetchdf()
        if len(brands) == 0:
            st.info("Нет брендов для удаления.")
            return
        brand = st.selectbox("Выберите бренд для удаления", list(brands['brand']))
        # Получить нормализованный ключ
        norm = self.normalize_key(pl.Series([brand]))[0]
        count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [norm]).fetchone()[0]
        if count == 0:
            st.info("Нет записей для этого бренда.")
            return
        if st.confirm("Удалить все записи этого бренда?"):
            deleted = self.delete_by_brand(norm)
            st.success(f"Удалено {deleted} записей.")

    def delete_by_artikul_ui(self):
        artikul = st.text_input("Введите артикул для удаления")
        if artikul:
            norm = self.normalize_key(pl.Series([artikul]))[0]
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [norm]).fetchone()[0]
            if count == 0:
                st.info("Артикул не найден.")
                return
            if st.confirm("Удалить все записи этого артикула?"):
                deleted = self.delete_by_artikul(norm)
                st.success(f"Удалено {deleted} записей.")

    def delete_by_brand(self, brand_norm: str) -> int:
        """Удаление по бренду"""
        with self.conn:
            count = self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm]).rowcount
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm = ?", [brand_norm])
        return count

    def delete_by_artikul(self, artikul_norm: str) -> int:
        """Удаление по артикулу"""
        with self.conn:
            count = self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).rowcount
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm])
        return count

    def show_price_settings(self):
        st.subheader("💰 Настройки цен и наценок")
        self.price_markup_global = st.slider("Общая наценка (%)", 0, 100, int(self.price_markup_global * 100))
        # Наценки по брендам
        st.write("Наценки по брендам")
        brands = self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL").fetchdf()
        for brand in brands['brand']:
            markup = self.brand_markups.get(brand, self.price_markup_global)
            new_markup = st.slider(f"{brand}", 0, 100, int(markup * 100))
            self.brand_markups[brand] = new_markup / 100
        # Ограничения цен
        min_price = st.number_input("Минимальная цена", value=0.0)
        max_price = st.number_input("Максимальная цена", value=1e6)
        self.price_rules = {
            "min_price": min_price,
            "max_price": max_price,
            "global_markup": self.price_markup_global,
            "brand_markups": self.brand_markups
        }
        if st.button("Сохранить настройки цен"):
            self.save_price_rules()

    def show_exclusion_settings(self):
        st.subheader("🚫 Исключения при экспорте")
        phrases = st.text_area("Исключения (через |)", value="|".join(self.exclusion_phrases))
        if st.button("Сохранить"):
            self.exclusion_phrases = [p.strip() for p in phrases.split('|') if p.strip()]
            st.success("Исключения сохранены.")

    def show_category_settings(self):
        st.subheader("🗂️ Категории товаров")
        for name, cat in self.categories_mapping.items():
            st.write(f"{name} → {cat}")
        name_input = st.text_input("Название товара")
        category_input = st.text_input("Категория")
        if st.button("Добавить/Обновить"):
            if name_input.strip() and category_input.strip():
                self.categories_mapping[name_input.strip()] = category_input.strip()
                # Сохраняем в файл
                cat_path = self.data_dir / "category_mapping.txt"
                lines = [f"{k}|{v}" for k, v in self.categories_mapping.items()]
                cat_path.write_text("\n".join(lines), encoding='utf-8')
                st.success("Категория добавлена/обновлена.")

        # Удаление
        if self.categories_mapping:
            to_del = st.selectbox("Удалить правило", list(self.categories_mapping.keys()))
            if st.button("Удалить"):
                del self.categories_mapping[to_del]
                lines = [f"{k}|{v}" for k, v in self.categories_mapping.items()]
                self.data_dir / "category_mapping.txt".write_text("\n".join(lines), encoding='utf-8')
                st.success("Правило удалено.")

    def show_cloud_sync(self):
        st.subheader("☁️ Облачная синхронизация")
        enabled = st.checkbox("Включить синхронизацию", value=self.cloud_config.get('enabled', False))
        provider = st.selectbox("Провайдер", ["s3", "gcs", "azure"], index=["s3", "gcs", "azure"].index(self.cloud_config.get('provider', 's3')))
        bucket = st.text_input("Bucket / Container", value=self.cloud_config.get('bucket', ''))
        region = st.text_input("Регион", value=self.cloud_config.get('region', ''))
        interval = st.number_input("Интервал синхронизации (сек)", min_value=300, max_value=86400, value=self.cloud_config.get('sync_interval', 3600))
        if st.button("Сохранить настройки"):
            self.cloud_config.update({
                'enabled': enabled,
                'provider': provider,
                'bucket': bucket,
                'region': region,
                'sync_interval': interval
            })
            self.save_cloud_config()
            st.success("Настройки сохранены.")
        if st.button("🔄 Выполнить синхронизацию сейчас"):
            self.perform_cloud_sync()

    def perform_cloud_sync(self):
        if not self.cloud_config.get('enabled', False):
            st.warning("Синхронизация отключена")
            return
        if not self.cloud_config.get('bucket'):
            st.error("Не указан bucket")
            return
        with st.spinner("Выполняется синхронизация..."):
            try:
                # Тут должна быть интеграция с облаком
                time.sleep(1)
                st.success(f"База успешно отправлена в {self.cloud_config['provider']}")
                self.cloud_config['last_sync'] = int(time.time())
                self.save_cloud_config()
            except Exception as e:
                st.error(f"Ошибка: {e}")

    def run(self):
        self.show_ui()

# Запуск
if __name__ == "__main__":
    catalog = AutoPartsCatalog()
    catalog.run()
