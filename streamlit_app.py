import platform
import sys
import polars as pl
import duckdb
import streamlit as st
import os
import time
import io
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import json
import logging

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

EXCEL_ROW_LIMIT = 1_000_000

class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.setup_database()

        st.set_page_config(page_title="AutoParts 10M+", layout="wide", page_icon="🚗")
        self.load_settings()

    def load_settings(self):
        self.cloud_config = self.load_json("cloud_config.json", default={
            "enabled": False, "provider": "s3", "bucket": "", "region": "", "sync_interval": 3600, "last_sync": 0
        })
        self.price_rules = self.load_json("price_rules.json", default={
            "global_markup": 0.2, "brand_markups": {}, "min_price": 0.0, "max_price": 99999.0
        })
        self.exclusion_rules = self.load_text("exclusion_rules.txt", default=["Кузов", "Стекла", "Масла"])
        self.category_mapping = self.load_text_mapping("category_mapping.txt", default={
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        })

    def load_json(self, filename, default):
        path = self.data_dir / filename
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except:
                return default
        else:
            path.write_text(json.dumps(default, indent=2, ensure_ascii=False), encoding='utf-8')
            return default

    def save_json(self, filename, data):
        path = self.data_dir / filename
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_text(self, filename, default):
        path = self.data_dir / filename
        if path.exists():
            try:
                return [line.strip() for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except:
                return default
        else:
            path.write_text("\n".join(default), encoding='utf-8')
            return default

    def load_text_mapping(self, filename, default):
        path = self.data_dir / filename
        mapping = default.copy()
        if path.exists():
            try:
                for line in path.read_text(encoding='utf-8').splitlines():
                    if '|' in line:
                        k, v = line.split("|", 1)
                        mapping[k.strip()] = v.strip()
            except:
                pass
        return mapping

    def save_text(self, filename, data):
        path = self.data_dir / filename
        path.write_text("\n".join(data), encoding='utf-8')

    def save_text_mapping(self, filename, mapping):
        path = self.data_dir / filename
        with open(path, 'w', encoding='utf-8') as f:
            for k, v in mapping.items():
                f.write(f"{k}|{v}\n")

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
        st.info("Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_data_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_data_keys ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
        ]
        for index_sql in indexes:
            self.conn.execute(index_sql)
        st.success("Индексы созданы.")

    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        return (
            series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zA-za-яЁё`\-\s]", "")
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
            .str.replace_all(r"[^0-9A-Za-zA-za-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
        )

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        categories_map = {
            'Фильтр': 'фильтр|filter', 
            'Тормозная система': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|Рычаг|Рычаги|Шаровая опора|Опора шаровая|Сайлентблок|Ступиц|подшипник ступицы|подшипники ступицы', 
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission', 
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering', 
            'Выхлопная система': 'глушитель|глушител|катализатор|выхлоп|exhaust|',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling', 
            'Топливо': 'топливный|бензонасос|форсунк|fuel',
        }
        name_lower = name_series.str.to_lowercase()
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(name_lower.str.contains(pattern)).then(pl.lit(category))
        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')

    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        mapping = {}
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
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size']
        }
        actual_lower = {col.lower(): col for col in actual_columns}
        for expected in expected_columns:
            variants = [v.lower() for v in column_variants.get(expected, [expected])]
            for variant in variants:
                for actual_l, actual_orig in actual_lower.items():
                    if variant in actual_l:
                        mapping[actual_orig] = expected
                        break
                if expected in mapping.values():
                    break
        return mapping

    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        try:
            df = pl.read_excel(file_path, engine='calamine')
        except:
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'cross': ['oe_number', 'artikul', 'brand']
        }
        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)
        df = df.rename(column_mapping)
        # Очистка оригинальных значений
        if 'artikul' in df.columns:
            df = df.with_columns(artikul=self.clean_values(pl.col('artikul')))
        if 'brand' in df.columns:
            df = df.with_columns(brand=self.clean_values(pl.col('brand')))
        if 'oe_number' in df.columns:
            df = df.with_columns(oe_number=self.clean_values(pl.col('oe_number')))
        # Удаление дубликатов
        key_cols = [c for c in ['oe_number', 'artikul', 'brand'] if c in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')
        # Создаем нормализованные ключи
        if 'artikul' in df.columns:
            df = df.with_columns(artikul_norm=self.normalize_key(pl.col('artikul')))
        if 'brand' in df.columns:
            df = df.with_columns(brand_norm=self.normalize_key(pl.col('brand')))
        if 'oe_number' in df.columns:
            df = df.with_columns(oe_number_norm=self.normalize_key(pl.col('oe_number')))
        return df

    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        if df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        pk_str = ", ".join(f'"{c}"' for c in pk)
        temp_name = f"temp_{table_name}_{int(time.time())}"
        self.conn.register(temp_name, df.to_arrow())

        update_cols = [col for col in cols if col not in pk]
        if not update_cols:
            on_conflict_action = "DO NOTHING"
        else:
            update_clause = ", ".join([f'"{col}" = excluded."{col}"' for col in update_cols])
            on_conflict_action = f"DO UPDATE SET {update_clause}"

        sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM {temp_name}
        ON CONFLICT ({pk_str}) {on_conflict_action};
        """
        try:
            self.conn.execute(sql)
        except:
            pass
        finally:
            self.conn.unregister(temp_name)

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        st.info("🔄 Начинаю обработку и загрузку данных...")
        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        num_steps = len(steps)
        progress_bar = st.progress(0, text="Подготовка к обновлению базы...")
        step_counter = 0

        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка OE")
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'], keep='first')
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))
            self.upsert_data('oe_data', oe_df, ['oe_number_norm'])
            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка кроссов")
            df = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка артикулами")
        # Обработка артикулами и объединение данных
        file_priority = ['oe', 'barcode', 'images', 'dimensions']
        key_files = {ftype: df for ftype, df in dataframes.items() if ftype in file_priority}
        if key_files:
            all_parts = pl.concat([
                df.select(['artikul', 'artikul_norm', 'brand', 'brand_norm']) 
                for df in key_files.values()
                if 'artikul_norm' in df.columns and 'brand_norm' in df.columns
            ]).filter(pl.col('artikul_norm') != "").unique(subset=['artikul_norm', 'brand_norm'], keep='first')
            parts_df = all_parts
            for ftype in file_priority:
                if ftype not in key_files:
                    continue
                df = key_files[ftype]
                if df.is_empty() or 'artikul_norm' not in df.columns:
                    continue
                join_cols = [c for c in df.columns if c not in ['artikul', 'artikul_norm', 'brand', 'brand_norm']]
                if not join_cols:
                    continue
                existing_cols = set(parts_df.columns)
                join_cols = [c for c in join_cols if c not in existing_cols]
                if not join_cols:
                    continue
                df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique(subset=['artikul_norm', 'brand_norm'])
                parts_df = parts_df.join(df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)
        if parts_df is not None and not parts_df.is_empty():
            if 'multiplicity' not in parts_df.columns:
                parts_df = parts_df.with_columns(multiplicity=pl.lit(1).cast(pl.Int32))
            else:
                parts_df = parts_df.with_columns(pl.col('multiplicity').fill_null(1).cast(pl.Int32))
            for col in ['length', 'width', 'height']:
                if col not in parts_df.columns:
                    parts_df = parts_df.with_columns(pl.lit(None).cast(pl.Float64))
            if 'dimensions_str' not in parts_df.columns:
                parts_df = parts_df.with_columns(dimensions_str=pl.lit(None).cast(pl.Utf8))
            parts_df = parts_df.with_columns([
                pl.col('length').cast(pl.Utf8).fill_null('').alias('_length_str'),
                pl.col('width').cast(pl.Utf8).fill_null('').alias('_width_str'),
                pl.col('height').cast(pl.Utf8).fill_null('').alias('_height_str'),
            ])
            parts_df = parts_df.with_columns(
                dimensions_str=pl.when(
                    (pl.col('dimensions_str').is_not_null()) & (pl.col('dimensions_str').cast(pl.Utf8) != '')
                ).then(
                    pl.concat_str([pl.col('_length_str'), pl.lit('x'), pl.col('_width_str'), pl.lit('x'), pl.col('_height_str')], separator='')
                ).otherwise(
                    pl.concat_str([pl.col('_length_str'), pl.lit('x'), pl.col('_width_str'), pl.lit('x'), pl.col('_height_str')], separator='')
                )
            )
            parts_df = parts_df.drop(['_length_str', '_width_str', '_height_str'])
            if 'artikul' not in parts_df.columns:
                parts_df = parts_df.with_columns(artikul=pl.lit(''))
            if 'brand' not in parts_df.columns:
                parts_df = parts_df.with_columns(brand=pl.lit(''))
            parts_df = parts_df.with_columns([
                pl.col('artikul').cast(pl.Utf8).fill_null('').alias('_artikul_str'),
                pl.col('brand').cast(pl.Utf8).fill_null('').alias('_brand_str'),
                pl.col('multiplicity').cast(pl.Utf8).alias('_multiplicity_str'),
            ])
            parts_df = parts_df.with_columns(
                description=pl.concat_str([
                    pl.lit('Артикул: '), pl.col('_artikul_str'),
                    pl.lit(', Бренд: '), pl.col('_brand_str'),
                    pl.lit(', Кратность: '), pl.col('_multiplicity_str'), pl.lit(' шт.')
                ], separator='')
            )
            parts_df = parts_df.drop(['_artikul_str', '_brand_str', '_multiplicity_str'])
            final_columns = [
                'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode', 
                'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
            ]
            select_exprs = [pl.col(c) if c in parts_df.columns else pl.lit(None).alias(c) for c in final_columns]
            parts_df = parts_df.select(select_exprs)
            self.upsert_data('parts_data', parts_df, ['artikul_norm', 'brand_norm'])
        progress_bar.progress(1.0)
        time.sleep(1)
        progress_bar.empty()
        st.success("💾 Загрузка завершена.")

    def merge_all_data_parallel(self, file_paths: Dict[str, str]) -> Dict[str, Any]:
        start_time = time.time()
        stats = {}
        st.info("🚀 Начинаю параллельное чтение файлов...")
        n_files = len(file_paths)
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.read_and_prepare_file, path, ftype): ftype for ftype, path in file_paths.items()}
            dataframes = {}
            for future in as_completed(futures):
                ftype = futures[future]
                try:
                    df = future.result()
                    if not df.is_empty():
                        dataframes[ftype] = df
                        st.success(f"✅ {ftype} прочитан: {len(df):,} строк")
                except Exception as e:
                    st.error(f"Ошибка при чтении {ftype}: {e}")
            self.process_and_load_data(dataframes)
        total_records = self.get_total_records()
        stats['processing_time'] = time.time() - start_time
        stats['total_records'] = total_records
        st.success(f"Обработка завершена за {stats['processing_time']:.2f} сек, всего {total_records:,} артикулов.")
        self.create_indexes()
        return stats

    def get_total_records(self):
        try:
            return self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
        except:
            return 0

    def build_export_query(self, selected_columns: Optional[List[str]]=None, include_prices=True, apply_markup=True):
        standard_description = """Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей.\n\nВ нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электрику, автомасла, инструмент, а также другие комплектующие, полностью соответствующие стандартам качества и безопасности.\n\nМы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса.\n\nВыбирайте только лучшее — надежность и качество от ведущих производителей."""
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
            ("Длинна/Ширина/Высота", "COALESCE(CASE WHEN r.dimensions_str IS NULL OR r.dimensions_str = '' OR UPPER(TRIM(r.dimensions_str)) = 'XX' THEN NULL ELSE r.dimensions_str END, r.analog_dimensions_str) AS \"Длинна/Ширина/Высота\""),
            ("OE номер", 'r.oe_list AS "OE номер"'),
            ("аналоги", 'r.analog_list AS "аналоги"'),
            ("Ссылка на изображение", 'r.image_url AS "Ссылка на изображение"')
        ]
        if include_prices:
            columns_map.extend([("Цена", '"Цена"'), ("Валюта", '"Валюта"')])
        if not selected_columns:
            selected_exprs = [expr for _, expr in columns_map]
        else:
            selected_exprs = [expr for name, expr in columns_map if name in selected_columns]
            if not selected_exprs:
                selected_exprs = [expr for _, expr in columns_map]
        ctes = """
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${}$$ AS text
        ),
        PartDetails AS (
            SELECT cr.artikul_norm, cr.brand_norm,
            STRING_AGG(DISTINCT regexp_replace(regexp_replace(o.oe_number, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') AS oe_list,
            ANY_VALUE(o.name) AS representative_name,
            ANY_VALUE(o.applicability) AS representative_applicability,
            ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr
            LEFT JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT cr1.artikul_norm, cr1.brand_norm,
            STRING_AGG(DISTINCT regexp_replace(regexp_replace(p2.artikul, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') as analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts_data p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        )
        SELECT
            p.artikul AS "Артикул бренда",
            p.brand AS "Бренд",
            pd.representative_name AS "Наименование",
            pd.representative_applicability AS "Применимость",
            p.description AS "Описание",
            pd.representative_category AS "Категория товара",
            p.multiplicity AS "Кратность",
            p.length AS "Длинна",
            p.width AS "Ширина",
            p.height AS "Высота",
            p.weight AS "Вес",
            p.dimensions_str AS "Длинна/Ширина/Высота",
            pd.oe_list AS "OE номер",
            aa.analog_list AS "аналоги",
            p.image_url AS "Ссылка на изображение"
        FROM parts_data p
        LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
        LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
        WHERE pd.oe_list IS NOT NULL
        ORDER BY p.brand, p.artikul
        """.format("".join(selected_exprs))
        return ctes
    
    def export_to_csv(self, output_path, selected_columns=None):
        total = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных")
            return False
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()
            # преобразование числовых колонок
            for col in ["Длинна","Ширина","Высота","Вес","Длинна/Ширина/Высота","Кратность"]:
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
                f.write(b'\xef\xbb\xbf')
                f.write(text.encode('utf-8'))
            return True
        except:
            return False
    
    def export_to_excel(self, output_path, selected_columns=None):
        total = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных")
            return False
        try:
            import pandas as pd
            num_files = (total + EXCEL_ROW_LIMIT -1) // EXCEL_ROW_LIMIT
            query = self.build_export_query(selected_columns)
            for i in range(num_files):
                q = f"{query} LIMIT {EXCEL_ROW_LIMIT} OFFSET {i * EXCEL_ROW_LIMIT}"
                df = self.conn.execute(q).pl()
                df = df.to_pandas()
                filename = output_path.with_name(f"{output_path.stem}_part_{i+1}.xlsx")
                df.to_excel(str(filename), index=False)
            # при множественных файлах собираем их в zip
            if num_files >1:
                from zipfile import ZipFile
                zipf = ZipFile(output_path.with_suffix('.zip'), 'w', zipfile.ZIP_DEFLATED)
                for i in range(num_files):
                    filename = output_path.with_name(f"{output_path.stem}_part_{i+1}.xlsx")
                    zipf.write(str(filename), arcname=filename.name)
                    os.remove(str(filename))
                zipf.close()
            return True
        except:
            return False
    
    def export_to_parquet(self, output_path, selected_columns=None):
        total = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных")
            return False
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            return True
        except:
            return False

    def show_export_interface(self):
        st.header("📤 Умный экспорт данных")
        total = self.conn.execute("SELECT count(DISTINCT artikul_norm, brand_norm) FROM parts_data").fetchone()[0]
        st.info(f"Всего {total:,} строк")
        if total == 0:
            st.warning("Нет данных")
            return
        options = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
            "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        selected_columns = st.multiselect("Выберите колонки", options=options)
        format_ = st.radio("Формат", ["CSV", "Excel (.xlsx)", "Parquet"])
        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"auto_parts_export.{format_.lower().replace(' ', '_')}"
            if format_ == "CSV":
                self.export_to_csv(str(output_path), selected_columns if selected_columns else None)
                with open(output_path, 'rb') as f:
                    st.download_button("📥 Скачать CSV", f, "auto_parts_report.csv", "text/csv")
            elif format_ == "Excel (.xlsx)":
                self.export_to_excel(str(output_path), selected_columns if selected_columns else None)
                with open(output_path.with_suffix('.zip'), 'rb') as f:
                    st.download_button("📥 Скачать ZIP", f, "auto_parts_report.zip", "application/zip")
            elif format_ == "Parquet":
                self.export_to_parquet(str(output_path), selected_columns if selected_columns else None)
                with open(output_path, 'rb') as f:
                    st.download_button("📥 Скачать Parquet", f, "auto_parts_report.parquet", "application/octet-stream")
    
    def delete_by_brand(self, brand_norm):
        try:
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm])
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm = ?", [brand_norm])
            return True
        except:
            return False

    def delete_by_artikul(self, artikul_norm):
        try:
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm])
            return True
        except:
            return False

    def get_statistics(self):
        stats = {}
        try:
            stats['total_parts'] = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
            stats['total_oe'] = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0]
            stats['total_cross'] = self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0]
            stats['total_prices'] = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            stats['top_brands'] = self.conn.execute("SELECT brand, COUNT(*) cnt FROM parts_data GROUP BY brand ORDER BY cnt DESC LIMIT 10").pl()
            stats['categories'] = self.conn.execute("SELECT COALESCE(category, 'Разное') AS category, COUNT(*) FROM oe_data GROUP BY category").pl()
        except:
            pass
        return stats

def main():
    st.title("🚗 AutoParts 10M+ — Мощная платформа")
    st.markdown("**Инструкции:**\nЗагрузите файлы, нажмите 'Обработать', затем — экспорт или управление.\n\nПрограмма способна работать с миллионами записей, использует DuckDB и Polars для скорости.**")
    catalog = HighVolumeAutoPartsCatalog()

    menu = st.sidebar.radio("Меню", ["Загрузка данных", "Экспорт", "Статистика", "Управление"])

    if menu == "Загрузка данных":
        st.header("📥 Загрузка файлов")
        col1, col2 = st.columns(2)
        with col1:
            f1 = st.file_uploader("Основные данные (OE)", ['xlsx', 'xls'])
            f2 = st.file_uploader("Кроссы OE→Артикул", ['xlsx', 'xls'])
            f3 = st.file_uploader("Штрих-коды", ['xlsx', 'xls'])
        with col2:
            f4 = st.file_uploader("Весогабариты", ['xlsx', 'xls'])
            f5 = st.file_uploader("Изображения", ['xlsx', 'xls'])
        files = {'oe': f1, 'cross': f2, 'barcode': f3, 'dimensions': f4, 'images': f5}
        if st.button("🚀 Обработать"):
            paths = {}
            for k, f in files.items():
                if f:
                    path = catalog.data_dir / f"{k}_{int(time.time())}.xlsx"
                    with open(path, 'wb') as ff:
                        ff.write(f.getbuffer())
                    paths[k] = str(path)
            if paths:
                catalog.merge_all_data_parallel(paths)
                st.success("Обработка завершена.")
            else:
                st.warning("Загрузите хотя бы один файл.")
    elif menu == "Экспорт":
        catalog.show_export_interface()
    elif menu == "Статистика":
        stats = catalog.get_statistics()
        st.subheader("Статистика")
        st.metric("Всего артикулов", stats.get('total_parts', 0))
        st.metric("OE", stats.get('total_oe', 0))
        st.metric("Кроссов", stats.get('total_cross', 0))
        st.metric("Цен", stats.get('total_prices', 0))
        if 'top_brands' in stats:
            st.subheader("ТОП-10 брендов")
            st.dataframe(stats['top_brands'].to_pandas())
        if 'categories' in stats:
            st.subheader("Распределение по категориям")
            st.dataframe(stats['categories'].to_pandas())
    elif menu == "Управление":
        st.header("🔧 Управление")
        opt = st.radio("Действие", ["Удалить по бренду", "Удалить по артикулу"])
        if opt == "Удалить по бренду":
            brands = catalog.conn.execute("SELECT DISTINCT brand FROM parts_data").pl()
            brands_list = brands['brand'].to_list() if not brands.is_empty() else []
            brand = st.selectbox("Выберите бренд", brands_list)
            if brand:
                # Получить нормализованный ключ
                nr = catalog.conn.execute("SELECT brand_norm FROM parts_data WHERE brand = ? LIMIT 1", [brand]).fetchone()
                bn = nr[0] if nr else catalog.normalize_key(pl.Series([brand]))[0]
                count = catalog.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [bn]).fetchone()[0]
                st.info(f"Удалить {count} записей бренда '{brand}'")
                if st.button("Удалить"):
                    catalog.delete_by_brand(bn)
                    st.success("Удалено.")
        else:
            arti = st.text_input("Введите артикул")
            if arti:
                nr = catalog.conn.execute("SELECT artikul_norm FROM parts_data WHERE artikul = ? LIMIT 1", [arti]).fetchone()
                an = nr[0] if nr else catalog.normalize_key(pl.Series([arti]))[0]
                count = catalog.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [an]).fetchone()[0]
                st.info(f"Удалить {count} записей артикула '{arti}'")
                if st.button("Удалить"):
                    catalog.delete_by_artikul(an)
                    st.success("Удалено.")

if __name__ == "__main__":
    main()
