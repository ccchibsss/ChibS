import polars as pl
import duckdb
import streamlit as st
import os
import time
import logging
import io
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
EXCEL_ROW_LIMIT = 1_000_000

class HighVolumeAutoPartsCatalog:
    
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path), read_only=False)
        self.setup_database()
        
        st.set_page_config(
            page_title="AutoParts Catalog 10M+", 
            layout="wide",
            page_icon="🚗"
        )
    
    def setup_database(self):
        """Инициализация структуры базы данных с возможностью повторного вызова."""
        try:
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
            logger.info("Структура базы данных инициализирована.")
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {e}")
            raise

    def create_indexes(self):
        """Создание индексов для оптимизации производительности запросов."""
        st.info("🔧 Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_data_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_data_keys ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
        ]
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except duckdb.CatalogException:
                pass  # индекс уже существует
        st.success("✅ Индексы успешно созданы.")

    @staticmethod
    def normalize_key(key_series: pl.Series) -> pl.Series:
        """Нормализация ключевых полей: очистка, приведение к нижнему регистру, удаление мусорных символов."""
        return (
            key_series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")  # удаление апострофов
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "", literal=False)  # оставить только разрешённые символы
            .str.replace_all(r"\s+", " ", literal=False)  # нормализовать пробелы
            .str.strip_chars()
            .str.to_lowercase()
        )

    @staticmethod
    def clean_values(value_series: pl.Series) -> pl.Series:
        """Очистка значений от ненужных символов, аналогично normalize_key, но без to_lowercase."""
        return (
            value_series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "", literal=False)
            .str.replace_all(r"\s+", " ", literal=False)
            .str.strip_chars()
        )

    @staticmethod
    def determine_category_vectorized(name_series: pl.Series) -> pl.Series:
        """Векторизованное определение категории по ключевым словам в названии."""
        categories_map = {
            'Фильтр': 'фильтр|filter', 
            'Тормозная система': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг|шаровая опора|сайлентблок|ступиц|подшипник ступицы',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission', 
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering', 
            'Выхлопная система': 'глушитель|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling', 
            'Топливо': 'топливный|бензонасос|форсунк|fuel',
        }
        name_lower = name_series.str.to_lowercase()
        categorization_expr = pl.lit(None)
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(name_lower.str.contains(pattern)).then(pl.lit(category))
        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')

    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        """Автоматическое сопоставление колонок по ключевым словам."""
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
        """Чтение Excel-файла и подготовка данных: нормализация, удаление дублей."""
        logger.info(f"Чтение файла: {file_type} ({file_path})")
        try:
            df = pl.read_excel(file_path, engine='calamine')
            if df.is_empty():
                logger.warning(f"Файл пуст: {file_path}")
                return df
        except Exception as e:
            logger.error(f"Не удалось прочитать файл {file_path}: {e}")
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

        # Очистка входных данных
        clean_fields = ['artikul', 'brand', 'oe_number']
        for field in clean_fields:
            if field in df.columns:
                df = df.with_columns(**{field: self.clean_values(pl.col(field))})

        # Удаление дубликатов по ключевым полям
        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        # Нормализация ключевых полей
        norm_fields = ['artikul', 'brand', 'oe_number']
        for field in norm_fields:
            if field in df.columns:
                df = df.with_columns(**{f"{field}_norm": self.normalize_key(pl.col(field))})

        logger.info(f"Файл {file_type} обработан: {len(df)} строк")
        return df

    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        """Настройка UPSERT с использованием временного представления и ON CONFLICT."""
        if df.is_empty():
            return

        df = df.unique(keep='first')
        cols = df.columns
        pk_str = ", ".join(f'"{c}"' for c in pk)
        temp_view_name = f"temp_{table_name}_{int(time.time())}"

        try:
            self.conn.register(temp_view_name, df.to_arrow())
            update_cols = [col for col in cols if col not in pk]
            update_clause = ", ".join([f'"{col}" = excluded."{col}"' for col in update_cols])
            on_conflict_action = f"DO UPDATE SET {update_clause}" if update_cols else "DO NOTHING"

            sql = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_view_name}
            ON CONFLICT ({pk_str}) {on_conflict_action};
            """
            self.conn.execute(sql)
            logger.info(f"UPSERT в таблицу {table_name}: {len(df)} записей")
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(f"Ошибка при записи в {table_name}. Подробности в логах.")
        finally:
            self.conn.unregister(temp_view_name)

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Основная логика обработки и слияния данных из нескольких источников."""
        st.info("🔄 Начало загрузки данных в базу...")
        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        total_steps = len(steps) + 1  # +1 за объединение parts
        progress = st.progress(0)
        step = 0

        # OE данные
        if 'oe' in dataframes:
            step += 1
            progress.progress(step / total_steps, text="Обработка OE данных...")
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'])
            oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            self.upsert_data('oe_data', oe_df, ['oe_number_norm'])

            cross_from_oe = df.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_from_oe, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Кроссы
        if 'cross' in dataframes:
            step += 1
            progress.progress(step / total_steps, text="Обработка кросс-ссылок...")
            df = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_data = df.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_data, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Сборка данных по артикулам
        step += 1
        progress.progress(step / total_steps, text="Сборка данных по артикулам...")
        file_priority = ['oe', 'barcode', 'images', 'dimensions']
        parts_sources = {ftype: dataframes[ftype] for ftype in file_priority if ftype in dataframes}

        if not parts_sources:
            progress.empty()
            st.warning("Нет данных для сборки parts_data.")
            return

        # Инициализация parts_df с артикулами и брендами
        all_parts = pl.concat([
            df.select(['artikul_norm', 'brand_norm', 'artikul', 'brand'])
            for df in parts_sources.values()
            if 'artikul_norm' in df.columns and 'brand_norm' in df.columns
        ]).unique(subset=['artikul_norm', 'brand_norm'])

        parts_df = all_parts

        # Последовательное присоединение по приоритету
        for ftype in file_priority:
            if ftype not in parts_sources:
                continue
            df = parts_sources[ftype]
            join_cols = [c for c in df.columns if c not in ['artikul', 'artikul_norm', 'brand', 'brand_norm']]
            if not join_cols:
                continue
            missing_cols = [c for c in join_cols if c not in parts_df.columns]
            if not missing_cols:
                continue
            df_subset = df.select(['artikul_norm', 'brand_norm'] + missing_cols).unique()
            parts_df = parts_df.join(df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)

        # Обеспечение наличия полей
        if 'multiplicity' not in parts_df.columns:
            parts_df = parts_df.with_columns(multiplicity=pl.lit(1).cast(pl.Int32))
        else:
            parts_df = parts_df.with_columns(pl.col('multiplicity').fill_null(1).cast(pl.Int32))

        # Габариты и dimensions_str
        for dim in ['length', 'width', 'height']:
            if dim not in parts_df.columns:
                parts_df = parts_df.with_columns(pl.lit(None).cast(pl.Float64).alias(dim))

        if 'dimensions_str' not in parts_df.columns:
            parts_df = parts_df.with_columns(dimensions_str=pl.lit(None).cast(pl.Utf8))

        # Генерация dimensions_str
        parts_df = parts_df.with_columns([
            pl.col('length').cast(pl.Utf8).fill_null('').alias('_l'),
            pl.col('width').cast(pl.Utf8).fill_null('').alias('_w'),
            pl.col('height').cast(pl.Utf8).fill_null('').alias('_h'),
        ]).with_columns(
            dimensions_str=pl.when(pl.col('dimensions_str').str.len_chars() > 0)
                            .then(pl.col('dimensions_str'))
                            .otherwise(pl.concat_str([pl.col('_l'), pl.lit('x'), pl.col('_w'), pl.lit('x'), pl.col('_h')], separator=''))
        ).drop(['_l', '_w', '_h'])

        # Генерация описания
        for col in ['artikul', 'brand', 'multiplicity']:
            if col not in parts_df.columns:
                parts_df = parts_df.with_columns(pl.lit('').alias(col))

        parts_df = parts_df.with_columns([
            pl.col('artikul').cast(pl.Utf8).fill_null('').alias('_a'),
            pl.col('brand').cast(pl.Utf8).fill_null('').alias('_b'),
            pl.col('multiplicity').cast(pl.Utf8).alias('_m'),
        ]).with_columns(
            description=pl.concat_str([
                pl.lit('Артикул: '), pl.col('_a'),
                pl.lit(', Бренд: '), pl.col('_b'),
                pl.lit(', Кратность: '), pl.col('_m'), pl.lit(' шт.')
            ], separator='')
        ).drop(['_a', '_b', '_m'])

        # Финальная селекция
        final_cols = [
            'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode',
            'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
        ]
        parts_df = parts_df.select([
            pl.col(c) if c in parts_df.columns else pl.lit(None).cast(pl.Utf8).alias(c)
            for c in final_cols
        ])

        self.upsert_data('parts_data', parts_df, ['artikul_norm', 'brand_norm'])
        progress.progress(1.0, text="Загрузка завершена!")
        time.sleep(0.5)
        progress.empty()
        st.success("💾 Данные успешно загружены.")

    def merge_all_data_parallel(self, file_paths: Dict[str, str]) -> Dict[str, any]:
        """Параллельное чтение файлов и обработка."""
        start = time.time()
        st.info("🚀 Параллельное чтение файлов...")
        dataframes = {}
        n_files = len(file_paths)
        progress = st.progress(0)

        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.read_and_prepare_file, path, ftype): ftype for ftype, path in file_paths.items()}
            for i, future in enumerate(as_completed(futures), 1):
                ftype = futures[future]
                try:
                    df = future.result()
                    if not df.is_empty():
                        dataframes[ftype] = df
                        st.success(f"✅ {ftype}: {len(df):,} строк")
                    else:
                        st.warning(f"⚠️ {ftype}: пустой или ошибка")
                except Exception as e:
                    logger.error(f"Ошибка в {ftype}: {e}")
                    st.error(f"❌ {ftype}: {e}")
                finally:
                    progress.progress(i / n_files)

        progress.empty()
        if not dataframes:
            st.error("❌ Нет данных для обработки.")
            return {}

        self.process_and_load_data(dataframes)
        self.create_indexes()

        total = self.get_total_records()
        elapsed = time.time() - start
        st.success(f"✅ Обработка завершена за {elapsed:.2f} с. Артикулов: {total:,}")
        return {'processing_time': elapsed, 'total_records': total}

    def get_total_records(self) -> int:
        """Получение количества уникальных артикулов."""
        try:
            res = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()
            return res[0] if res else 0
        except:
            return 0

    def build_export_query(self, selected_columns: Optional[List[str]] = None) -> str:
        """Построение сложного SQL-запроса для экспорта с поддержкой кастомных колонок и описаний."""
        standard_description = """\n\nСостояние товара: новый (в упаковке).
Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. 
Обеспечьте безопасность, долговечность и высокую производительность с нашим ассортиментом.

В каталоге: тормозные системы, фильтры, свечи, расходники, автохимия, электрика, масла, инструмент и др.

Гарантируем: быстрая доставка, выгодные цены, профессиональная консультация.

Выбирайте лучшее — надежность и качество от ведущих производителей."""

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
            ("Длинна/Ширина/Высота", "COALESCE(NULLIF(CASE WHEN UPPER(TRIM(r.dimensions_str)) IN ('', 'XX') THEN NULL ELSE r.dimensions_str END, ''), r.analog_dimensions_str) AS \"Длинна/Ширина/Высота\""),
            ("OE номер", 'r.oe_list AS "OE номер"'),
            ("аналоги", 'r.analog_list AS "аналоги"'),
            ("Ссылка на изображение", 'r.image_url AS "Ссылка на изображение"')
        ]

        selected_exprs = [expr for name, expr in columns_map if selected_columns is None or name in selected_columns]
        if not selected_exprs:
            selected_exprs = [expr for _, expr in columns_map]

        ctes = f"""
        WITH DescriptionTemplate AS (SELECT CHR(10) || CHR(10) || $${standard_description}$$ AS text),
        PartDetails AS (
            SELECT cr.artikul_norm, cr.brand_norm,
                   STRING_AGG(DISTINCT regexp_replace(o.oe_number, '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') AS oe_list,
                   ANY_VALUE(o.name) AS representative_name,
                   ANY_VALUE(o.applicability) AS representative_applicability,
                   ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT cr1.artikul_norm, cr1.brand_norm,
                   STRING_AGG(DISTINCT regexp_replace(p2.artikul, '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') AS analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts_data p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE NOT (cr1.artikul_norm = p2.artikul_norm AND cr1.brand_norm = p2.brand_norm)
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        ),
        AggregatedAnalogData AS (
            SELECT arp.source_artikul_norm AS artikul_norm, arp.source_brand_norm AS brand_norm,
                   MAX(p2.length) AS length, MAX(p2.width) AS width, MAX(p2.height) AS height, MAX(p2.weight) AS weight,
                   ANY_VALUE(NULLIF(NULLIF(p2.dimensions_str, ''), 'XX')) AS dimensions_str,
                   ANY_VALUE(NULLIF(pd2.representative_name, '')) AS representative_name,
                   ANY_VALUE(NULLIF(pd2.representative_applicability, '')) AS representative_applicability,
                   ANY_VALUE(NULLIF(pd2.representative_category, '')) AS representative_category
            FROM (SELECT DISTINCT i.artikul_norm, i.brand_norm, cr2.artikul_norm AS related_artikul_norm, cr2.brand_norm AS related_brand_norm
                  FROM (SELECT artikul_norm, brand_norm FROM parts_data WHERE artikul_norm IS NOT NULL AND brand_norm IS NOT NULL) i
                  JOIN cross_references cr2 ON i.oe_number_norm = cr2.oe_number_norm
                  WHERE NOT (i.artikul_norm = cr2.artikul_norm AND i.brand_norm = cr2.brand_norm)
                 ) arp
            JOIN parts_data p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT p.artikul, p.brand, p.description, p.multiplicity, p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url,
                   pd.representative_name, pd.representative_applicability, pd.representative_category, pd.oe_list, aa.analog_list,
                   pa.length AS analog_length, pa.width AS analog_width, pa.height AS analog_height, pa.weight AS analog_weight,
                   pa.dimensions_str AS analog_dimensions_str, pa.representative_name AS analog_representative_name,
                   pa.representative_applicability AS analog_representative_applicability, pa.representative_category AS analog_representative_category,
                   ROW_NUMBER() OVER(PARTITION BY p.artikul_norm, p.brand_norm ORDER BY pd.representative_name DESC NULLS LAST) AS rn
            FROM parts_data p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData pa ON p.artikul_norm = pa.artikul_norm AND p.brand_norm = pa.brand_norm
        )
        """

        select_clause = ",\n            ".join(selected_exprs)
        query = ctes + f"""
        SELECT
            {select_clause}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        WHERE r.rn = 1
        ORDER BY r.brand, r.artikul
        """
        return query

    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None) -> bool:
        """Экспорт в CSV с кодировкой UTF-8 с BOM."""
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data) AS t").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта.")
            return False

        st.info(f"📤 Экспорт в CSV: {total:,} строк...")
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]
            for col in dimension_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).cast(pl.Utf8).fill_null("").alias(col)
                    )
            csv_str = df.write_csv(separator=";", include_header=True)
            with open(output_path, 'wb') as f:
                f.write(b'\xef\xbb\xbf')  # BOM для Excel
                f.write(csv_str.encode('utf-8'))
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ CSV сохранён: {output_path} ({size_mb:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в CSV")
            st.error(f"❌ Ошибка: {e}")
            return False

    def export_to_excel(self, output_path: Path, selected_columns: Optional[List[str]] = None) -> tuple[bool, Optional[Path]]:
        """Экспорт в Excel с разбиением на части при необходимости."""
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data) AS t").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта.")
            return False, None

        st.info(f"📤 Экспорт в Excel: {total:,} строк...")
        try:
            n_parts = (total + EXCEL_ROW_LIMIT - 1) // EXCEL_ROW_LIMIT
            base_query = self.build_export_query(selected_columns)
            exported = []
            progress = st.progress(0)

            for i in range(n_parts):
                progress.progress((i + 1) / n_parts, text=f"Часть {i+1}/{n_parts}...")
                df = self.conn.execute(f"{base_query} LIMIT {EXCEL_ROW_LIMIT} OFFSET {i * EXCEL_ROW_LIMIT}").pl()
                for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]:
                    if col in df.columns:
                        df = df.with_columns(pl.col(col).cast(pl.Utf8).fill_null("").alias(col))
                part_path = output_path.with_name(f"{output_path.stem}_part_{i+1}.xlsx")
                df.write_excel(part_path)
                exported.append(part_path)

            progress.empty()
            if n_parts > 1:
                zip_path = output_path.with_suffix(".zip")
                with zipfile.ZipFile(zip_path, 'w') as zf:
                    for f in exported:
                        zf.write(f, f.name)
                        f.unlink()
                st.success(f"✅ Части упакованы в ZIP: {zip_path.name}")
                return True, zip_path
            else:
                part_path = exported[0]
                part_path.rename(output_path)
                st.success(f"✅ Excel сохранён: {output_path.name}")
                return True, output_path
        except Exception as e:
            logger.exception("Ошибка экспорта в Excel")
            st.error(f"❌ Ошибка: {e}")
            return False, None

    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None) -> bool:
        """Экспорт в Parquet — оптимальный формат для хранения и анализа."""
        total = self.get_total_records()
        if total == 0:
            st.warning("Нет данных для экспорта.")
            return False
        st.info(f"📤 Экспорт в Parquet: {total:,} строк...")
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Parquet сохранён: {output_path} ({size_mb:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в Parquet")
            st.error(f"❌ Ошибка: {e}")
            return False

    def show_export_interface(self):
        """Интерфейс экспорта с выбором формата и колонок."""
        st.header("📤 Экспорт данных")
        total = self.get_total_records()
        st.info(f"Записей для экспорта: {total:,}")
        if total == 0:
            st.warning("Загрузите данные перед экспортом.")
            return

        cols = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание", "Категория товара",
            "Кратность", "Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        selected = st.multiselect("Выберите колонки", options=cols, default=cols)

        fmt = st.radio("Формат", ["CSV", "Excel (.xlsx)", "Parquet"])

        if fmt == "CSV" and st.button("🚀 Экспорт в CSV"):
            path = self.data_dir / "report.csv"
            with st.spinner("Экспорт..."):
                ok = self.export_to_csv_optimized(str(path), selected)
            if ok:
                with open(path, "rb") as f:
                    st.download_button("📥 Скачать CSV", f, "report.csv", "text/csv")

        elif fmt == "Excel (.xlsx)" and st.button("📊 Экспорт в Excel"):
            path = self.data_dir / "report.xlsx"
            with st.spinner("Экспорт..."):
                ok, final = self.export_to_excel(path, selected)
            if ok and final:
                with open(final, "rb") as f:
                    mime = "application/zip" if final.suffix == ".zip" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    st.download_button("📥 Скачать Excel", f, final.name, mime)

        elif fmt == "Parquet" and st.button("⚡️ Экспорт в Parquet"):
            path = self.data_dir / "report.parquet"
            with st.spinner("Экспорт..."):
                ok = self.export_to_parquet(str(path), selected)
            if ok:
                with open(path, "rb") as f:
                    st.download_button("📥 Скачать Parquet", f, "report.parquet", "application/octet-stream")

    def delete_by_brand(self, brand_norm: str) -> int:
        """Безопасное удаление по нормализованному бренду."""
        try:
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
            if count == 0:
                return 0
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT artikul_norm, brand_norm FROM parts_data)")
            logger.info(f"Удалено {count} записей для бренда: {brand_norm}")
            return count
        except Exception as e:
            logger.error(f"Ошибка удаления бренда {brand_norm}: {e}")
            raise

    def delete_by_artikul(self, artikul_norm: str) -> int:
        """Безопасное удаление по нормализованному артикулу."""
        try:
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
            if count == 0:
                return 0
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT artikul_norm, brand_norm FROM parts_data)")
            logger.info(f"Удалено {count} записей для артикула: {artikul_norm}")
            return count
        except Exception as e:
            logger.error(f"Ошибка удаления артикула {artikul_norm}: {e}")
            raise

    def get_statistics(self) -> Dict:
        """Сбор статистики по базе данных."""
        stats = {'total_parts': 0, 'total_oe': 0, 'total_brands': 0, 'top_brands': pl.DataFrame(), 'categories': pl.DataFrame()}
        try:
            total_parts = self.get_total_records()
            if total_parts == 0:
                return stats

            total_oe = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0]
            total_brands = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data WHERE brand IS NOT NULL").fetchone()[0]

            top_brands = self.conn.execute("""
                SELECT brand, COUNT(*) as count 
                FROM parts_data 
                WHERE brand IS NOT NULL 
                GROUP BY brand 
                ORDER BY count DESC 
                LIMIT 10
            """).pl()

            categories = self.conn.execute("""
                SELECT category, COUNT(*) as count 
                FROM oe_data 
                WHERE category IS NOT NULL 
                GROUP BY category 
                ORDER BY count DESC
            """).pl()

            stats.update({
                'total_parts': total_parts,
                'total_oe': total_oe,
                'total_brands': total_brands,
                'top_brands': top_brands,
                'categories': categories
            })
        except Exception as e:
            logger.error(f"Ошибка при сборе статистики: {e}")
        return stats


def main():
    """Основная функция запуска Streamlit-приложения."""
    st.title("🚗 AutoParts Catalog — Система управления каталогом")
    st.markdown("""
    ### 💼 Профессиональное решение для 10M+ автозапчастей
    - ✅ **Инкрементальные обновления** — дополняйте базу без потерь
    - 🔀 **Автоматическое слияние** — данные из разных источников объединяются корректно
    - 🚀 **Высокая производительность** — DuckDB + Polars для быстрой обработки
    - 📤 **Гибкий экспорт** — CSV, Excel, Parquet с фильтрацией колонок
    """)

    catalog = HighVolumeAutoPartsCatalog()

    menu = st.sidebar.radio("Меню", ["Загрузка данных", "Экспорт", "Статистика", "Управление данными"])

    if menu == "Загрузка данных":
        st.header("📥 Загрузка данных")
        st.info("""
        **Как использовать:**
        1. Загрузите один или несколько файлов Excel.
        2. Нажмите «Начать обработку».
        3. Данные будут объединены, дубли удалены, база — обновлена.

        💡 Поддерживаются: OE, кроссы, штрих-коды, весогабариты, изображения.
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            oe_file = st.file_uploader("1. Основные данные (OE)", type=['xlsx', 'xls'])
            cross_file = st.file_uploader("2. Кроссы", type=['xlsx', 'xls'])
            barcode_file = st.file_uploader("3. Штрих-коды", type=['xlsx', 'xls'])
        with col2:
            dimensions_file = st.file_uploader("4. Весогабариты", type=['xlsx', 'xls'])
            images_file = st.file_uploader("5. Изображения", type=['xlsx', 'xls'])

        files = {
            'oe': oe_file, 'cross': cross_file, 'barcode': barcode_file,
            'dimensions': dimensions_file, 'images': images_file
        }

        if st.button("🚀 Начать обработку", type="primary"):
            paths = {}
            for ftype, file in files.items():
                if file:
                    path = catalog.data_dir / f"{ftype}_{int(time.time())}_{file.name}"
                    with open(path, "wb") as f:
                        f.write(file.getvalue())
                    paths[ftype] = str(path)

            if paths:
                stats = catalog.merge_all_data_parallel(paths)
                if stats:
                    st.subheader("📊 Результат")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Время", f"{stats['processing_time']:.2f} с")
                    c2.metric("Артикулов", f"{stats['total_records']:,}")
                    c3.metric("Файлов", len(paths))
            else:
                st.warning("Загрузите хотя бы один файл.")

    elif menu == "Экспорт":
        catalog.show_export_interface()

    elif menu == "Статистика":
        st.header("📈 Статистика")
        with st.spinner("Загрузка..."):
            stats = catalog.get_statistics()
        if stats['total_parts'] > 0:
            c1, c2, c3 = st.columns(3)
            c1.metric("Артикулов", f"{stats['total_parts']:,}")
            c2.metric("OE номеров", f"{stats['total_oe']:,}")
            c3.metric("Брендов", f"{stats['total_brands']:,}")

            st.subheader("🏆 Топ-10 брендов")
            if not stats['top_brands'].is_empty():
                st.dataframe(stats['top_brands'].to_pandas())
            else:
                st.write("Нет данных.")

            st.subheader("📊 Категории")
            if not stats['categories'].is_empty():
                st.bar_chart(stats['categories'].to_pandas().set_index('category'))
            else:
                st.write("Нет данных.")
        else:
            st.info("Загрузите данные для просмотра статистики.")

    elif menu == "Управление данными":
        st.header("🗑️ Удаление данных")
        st.warning("⚠️ Операции необратимы!")
        action = st.radio("Действие", ["Удалить по бренду", "Удалить по артикулу"])

        if action == "Удалить по бренду":
            brands = catalog.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL ORDER BY brand").pl()
            brand_list = brands['brand'].to_list() if not brands.is_empty() else []
            if not brand_list:
                st.warning("Нет брендов.")
                return
            selected = st.selectbox("Выберите бренд", brand_list)
            if selected:
                norm = catalog.conn.execute("SELECT brand_norm FROM parts_data WHERE brand = ? LIMIT 1", [selected]).fetchone()
                if not norm:
                    # fallback
                    norm_val = catalog.normalize_key(pl.Series([selected]))[0]
                else:
                    norm_val = norm[0]
                count = catalog.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [norm_val]).fetchone()[0]
                st.info(f"Будет удалено: {count} записей")
                if count > 0 and st.checkbox("Подтвердить удаление", key="del_brand"):
                    if st.button("❌ Удалить бренд"):
                        deleted = catalog.delete_by_brand(norm_val)
                        st.success(f"Удалено: {deleted}")
                        st.rerun()

        elif action == "Удалить по артикулу":
            art = st.text_input("Введите артикул")
            if art:
                norm_series = catalog.normalize_key(pl.Series([art]))
                norm_val = norm_series[0] if len(norm_series) > 0 else ""
                count = catalog.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [norm_val]).fetchone()
                count = count[0] if count else 0
                if count == 0:
                    st.warning("Артикул не найден.")
                else:
                    st.info(f"Найдено: {count} записей")
                    if st.checkbox("Подтвердить удаление", key="del_art"):
                        if st.button("❌ Удалить артикул"):
                            deleted = catalog.delete_by_artikul(norm_val)
                            st.success(f"Удалено: {deleted}")
                            st.rerun()


if __name__ == "__main__":
    main()
