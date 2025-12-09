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
import warnings

warnings.filterwarnings('ignore')

EXCEL_ROW_LIMIT = 1_000_000

class AutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.setup_database()

        # Загрузка настроек
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

        # Настройки Streamlit
        st.set_page_config(page_title="AutoParts 10M+", layout="wide", page_icon="🚗")
        # Можно добавить ещё настройки здесь

    def load_json(self, filename, default):
        path = self.data_dir / filename
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except:
                return default
        else:
            path.write_text(json.dumps(default), encoding='utf-8')
            return default

    def save_json(self, filename, data):
        path = self.data_dir / filename
        path.write_text(json.dumps(data), encoding='utf-8')

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
                        k, v = line.split('|', 1)
                        mapping[k.strip()] = v.strip()
            except:
                pass
        return mapping

    def save_text(self, filename, data):
        path = self.data_dir / filename
        path.write_text("\n".join(data), encoding='utf-8')

    def save_text_mapping(self, filename, mapping):
        path = self.data_dir / filename
        txt = "\n".join([f"{k}|{v}" for k, v in mapping.items()])
        path.write_text(txt, encoding='utf-8')

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
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS category_mapping (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                category VARCHAR,
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.create_indexes()

    def create_indexes(self):
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_oe ON oe_data(oe_number_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_parts ON parts_data(artikul_norm, brand_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cross ON cross_references(oe_number_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cross_art ON cross_references(artikul_norm, brand_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_prices ON prices(artikul_norm, brand_norm)")

    @staticmethod
    def normalize_key(series):
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

    def read_and_prepare_file(self, filepath, file_type):
        try:
            df = pl.read_excel(filepath, engine='calamine')
        except:
            return pl.DataFrame()
        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected_cols = schemas.get(file_type, [])
        col_map = self.detect_columns(df.columns, expected_cols)
        df = df.rename(col_map)

        # очистка
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))
        # дубли
        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        # создание нормализованных
        if 'artikul' in df.columns:
            df = df.with_columns(self.normalize_key(pl.col('artikul')).alias('artikul_norm'))
        if 'brand' in df.columns:
            df = df.with_columns(self.normalize_key(pl.col('brand')).alias('brand_norm'))
        if 'oe_number' in df.columns:
            df = df.with_columns(self.normalize_key(pl.col('oe_number')).alias('oe_number_norm'))
        return df

    def detect_columns(self, actual_cols, expected_cols):
        variants = {
            'oe': ['oe', 'оe', 'oe номер'],
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand'],
            'name': ['наименование', 'название', 'name'],
            'applicability': ['применимость', 'vehicle'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13'],
            'multiplicity': ['кратность', 'multiplicity'],
            'length': ['длина', 'length'],
            'width': ['ширина', 'width'],
            'height': ['высота', 'height'],
            'weight': ['вес', 'weight'],
            'image_url': ['ссылка', 'url', 'image', 'картинка'],
            'dimensions_str': ['размеры', 'dimensions', 'size']
        }
        mapping = {}
        actual_lower = {c.lower(): c for c in actual_cols}
        for expected in expected_cols:
            for v in variants.get(expected, [expected]):
                v_l = v.lower()
                for ac_l, ac_o in actual_lower.items():
                    if v_l in ac_l:
                        mapping[ac_o] = expected
        return mapping

    def clean_values(self, series):
        return (
            series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
        )

    def upsert_data(self, table_name, df, pk):
        if df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        pk_str = ", ".join(f'"{col}"' for col in pk)
        temp_name = f"temp_{table_name}_{int(time.time())}"
        self.conn.register(temp_name, df.to_arrow())

        update_cols = [c for c in cols if c not in pk]
        if not update_cols:
            conflict_action = "DO NOTHING"
        else:
            set_clause = ", ".join([f'"{col}"=excluded."{col}"' for col in update_cols])
            conflict_action = f"DO UPDATE SET {set_clause}"

        sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM {temp_name}
        ON CONFLICT ({pk_str}) {conflict_action};
        """
        try:
            self.conn.execute(sql)
        except:
            pass
        finally:
            self.conn.unregister(temp_name)

    def upsert_prices(self, df):
        if df.is_empty():
            return
        if 'artikul' in df.columns:
            df = df.with_columns(self.normalize_key(pl.col('artikul')).alias('artikul_norm'))
        if 'brand' in df.columns:
            df = df.with_columns(self.normalize_key(pl.col('brand')).alias('brand_norm'))
        if 'currency' not in df.columns:
            df = df.with_columns(pl.lit('RUB').alias('currency'))
        df = df.filter(
            (pl.col('price') >= self.price_rules['min_price']) & (pl.col('price') <= self.price_rules['max_price'])
        )
        self.upsert_data('prices', df, ['artikul_norm', 'brand_norm'])

    def process_and_load_data(self, dataframes):
        # Весь процесс обработки из предыдущих кодов
        st.info("🔄 Начинаю обработку и загрузку данных...")

        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes or s == 'parts']
        num_steps = len(steps)
        progress_bar = st.progress(0, text="Подготовка к обновлению базы...")
        step_counter = 0

        # Обработка OE-данных
        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка OE данных...")
            df_oe = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df_oe.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'], keep='first')
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))
            self.upsert_data('oe_data', oe_df, ['oe_number_norm'])
            cross_df_from_oe = df_oe.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Обработка кроссов
        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка кроссов...")
            df_cross = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df_cross.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Обработка parts
        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка артикулами...")
        # Комплексная обработка артикулами
        # Объединение данных из разных файлов
        parts_df = None
        # Собираем все артикули и бренды
        all_parts_list = []
        for key in ['oe', 'barcode', 'dimensions', 'images']:
            if key in dataframes:
                df_tmp = dataframes[key]
                if 'artikul' in df_tmp.columns and 'brand' in df_tmp.columns:
                    all_parts_list.append(df_tmp.select(['artikul', 'artikul', 'brand', 'brand']))
        if all_parts_list:
            parts_combined = pl.concat(all_parts_list).unique(subset=['artikul', 'brand'])
            # Создаем нормальные ключи
            parts_combined = parts_combined.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])
            # Обработка некоторых полей
            # Обеспечить заполнение
            parts_df = parts_combined

        # Обновляем/вставляем в parts_data
        if parts_df is not None and not parts_df.is_empty():
            # Обработка полей: описание, размеры, кратность, веса
            # Пример: заполнение полей по умолчанию
            for col in ['length', 'width', 'height', 'weight']:
                if col not in parts_df.columns:
                    parts_df = parts_df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
            if 'multiplicity' not in parts_df.columns:
                parts_df = parts_df.with_columns(pl.lit(1).cast(pl.Int32).alias('multiplicity'))

            # Создать описание
            parts_df = parts_df.with_columns([
                self.clean_values(pl.col('artikul')).alias('artikul'),
                self.clean_values(pl.col('brand')).alias('brand')
            ])

            # Создавать описание из артикула и бренда
            parts_df = parts_df.with_columns([
                pl.lit('Артикул: ').cast(pl.Utf8).concat(pl.col('artikul')).alias('artikul_str'),
                pl.lit('Бренд: ').cast(pl.Utf8).concat(pl.col('brand')).alias('brand_str')
            ])

            # Создавать описание
            parts_df = parts_df.with_columns([
                pl.lit('Кратность: ').cast(pl.Utf8).concat(pl.col('multiplicity').cast(pl.Utf8)).alias('description')
            ])

            self.upsert_data('parts_data', parts_df, ['artikul_norm', 'brand_norm'])

        progress_bar.progress(1.0, text="Обновление базы данных завершено!")
        time.sleep(1)
        progress_bar.empty()
        st.success("💾 Загрузка данных в базу завершена.")

    def build_export_query(self, selected_columns=None):
        # Стандартный текст описания
        standard_description = """Состояние товара: новый (в упаковке).
Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей.

В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электрику, автомасла, инструмент, а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. 

Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. 

Выбирайте только лучшее — надежность и качество от ведущих производителей."""
        # Колонки
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

        # Если есть цены, добавляем
        if self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] > 0:
            columns_map.extend([("Цена", '"Цена"'), ("Валюта", '"Валюта"')])

        if not selected_columns:
            selected_exprs = [expr for _, expr in columns_map]
        else:
            selected_exprs = [expr for name, expr in columns_map if name in selected_columns]
            if not selected_exprs:
                selected_exprs = [expr for _, expr in columns_map]

        # Создаем CTE для текста
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
                ANY_VALUE(CASE WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX' THEN p2.dimensions_str ELSE NULL END) AS dimensions_str,
                ANY_VALUE(CASE WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' THEN pd2.representative_name ELSE NULL END) AS representative_name,
                ANY_VALUE(CASE WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != '' THEN pd2.representative_applicability ELSE NULL END) AS representative_applicability,
                ANY_VALUE(CASE WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != '' THEN pd2.representative_category ELSE NULL END) AS representative_category
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
                ROW_NUMBER() OVER(PARTITION BY p.artikul_norm, p.brand_norm ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST) as rn
            FROM parts_data p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
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

    def export_to_csv(self, output_path, selected_columns=None):
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total_records:,} записей в CSV...")
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()

            # Преобразуем числовые столбцы в строки
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]
            for col in dimension_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col(col).is_not_null())
                        .then(pl.col(col).cast(pl.Utf8))
                        .otherwise(pl.lit(""))
                        .alias(col)
                    )

            buf = io.StringIO()
            df.write_csv(buf, separator=';')
            csv_text = buf.getvalue()

            with open(output_path, 'wb') as f:
                f.write(b'\xef\xbb\xbf')
                f.write(csv_text.encode('utf-8'))

            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в CSV: {output_path} ({file_size:.1f} МБ)")
            return True
        except:
            import traceback
            traceback.print_exc()
            st.error("❌ Ошибка при экспорте в CSV")
            return False

    def export_to_excel(self, output_path, selected_columns=None):
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total_records:,} записей в Excel...")
        try:
            import pandas as pd
            num_files = (total_records + EXCEL_ROW_LIMIT - 1) // EXCEL_ROW_LIMIT
            base_query = self.build_export_query(selected_columns)
            exported_files = []

            progress_bar = st.progress(0, text=f"Подготовка к экспорту {num_files} файла(ов)...")
            for i in range(num_files):
                progress_bar.progress((i + 1) / num_files, text=f"Экспорт части {i+1} из {num_files}...")
                offset = i * EXCEL_ROW_LIMIT
                query = f"{base_query} LIMIT {EXCEL_ROW_LIMIT} OFFSET {offset}"
                df = self.conn.execute(query).pl()

                # преобразуем числовые колонки в строки
                dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]
                for col in dimension_cols:
                    if col in df.columns:
                        df = df.with_columns(
                            pl.when(pl.col(col).is_not_null())
                            .then(pl.col(col).cast(pl.Utf8))
                            .otherwise(pl.lit(""))
                            .alias(col)
                        )

                file_part_path = self.data_dir / f"{output_path.stem}_part_{i+1}.xlsx"
                df.to_pandas().to_excel(str(file_part_path), index=False)
                exported_files.append(file_part_path)

            progress_bar.empty()

            if num_files > 1:
                st.info("Архивация файлов в ZIP...")
                zip_path = self.data_dir / f"{output_path.stem}.zip"
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in exported_files:
                        zipf.write(file, arcname=file.name)
                        os.remove(file)
                final_path = zip_path
            else:
                final_path = exported_files[0]
                if final_path.name != output_path.name:
                    os.rename(final_path, output_path)
                    final_path = output_path

            file_size = os.path.getsize(final_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы: {final_path.name} ({file_size:.1f} МБ)")
            return True
        except:
            import traceback
            traceback.print_exc()
            st.error("❌ Ошибка при экспорте в Excel")
            return False

    def export_to_parquet(self, output_path, selected_columns=None):
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total_records:,} записей в Parquet...")
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()
            df.write_parquet(str(output_path))
            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в Parquet: {output_path} ({file_size:.1f} МБ)")
            return True
        except:
            import traceback
            traceback.print_exc()
            st.error("❌ Ошибка при экспорте в Parquet")
            return False

    def get_statistics(self):
        stats = {}
        try:
            stats['total_parts'] = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
            stats['total_oe'] = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0]
            stats['total_cross'] = self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0]
            stats['total_prices'] = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            stats['total_brands'] = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data").fetchone()[0]
            stats['total_unique'] = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
            avg_price_res = self.conn.execute("SELECT AVG(price) FROM prices").fetchone()
            stats['avg_price'] = round(avg_price_res[0], 2) if avg_price_res and avg_price_res[0] else 0.0
        except:
            import traceback
            traceback.print_exc()
            return {
                'total_parts':0,'total_oe':0,'total_cross':0,'total_prices':0,
                'total_brands':0,'total_unique':0,'avg_price':0
            }
        return stats

    def merge_all_data_parallel(self, file_paths):
        start_time = time.time()
        # Чтение файлов параллельно
        dataframes = {}
        with st.spinner("Чтение файлов..."):
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.read_and_prepare_file, path, ftype): ftype for ftype, path in file_paths.items()}
                for future in futures:
                    ftype = futures[future]
                    try:
                        df = future.result()
                        if not df.is_empty():
                            dataframes[ftype] = df
                            st.success(f"Обработан файл: {ftype}")
                        else:
                            st.warning(f"Файл {ftype} пуст или не прочитан")
                    except:
                        import traceback
                        traceback.print_exc()
                        st.error(f"Ошибка при чтении файла: {ftype}")
        if not dataframes:
            st.warning("Нет данных для обработки")
            return {}
        self.process_and_load_data(dataframes)
        duration = time.time() - start_time
        total_records = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
        st.success(f"Обработка завершена за {duration:.2f} секунд. Всего артикулов: {total_records}")
        return {'processing_time': duration, 'total_records': total_records}

    def build_export_query(self, selected_columns=None):
        # Стандартный текст описания
        standard_description = """Состояние товара: новый (в упаковке).
Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей.

В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электрику, автомасла, инструмент, а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. 

Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. 

Выбирайте только лучшее — надежность и качество от ведущих производителей."""
        # Колонки
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

        if self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] > 0:
            columns_map.extend([("Цена", '"Цена"'), ("Валюта", '"Валюта"')])

        if not selected_columns:
            selected_exprs = [expr for _, expr in columns_map]
        else:
            selected_exprs = [expr for name, expr in columns_map if name in selected_columns]
            if not selected_exprs:
                selected_exprs = [expr for _, expr in columns_map]

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
                ANY_VALUE(CASE WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX' THEN p2.dimensions_str ELSE NULL END) AS dimensions_str,
                ANY_VALUE(CASE WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' THEN pd2.representative_name ELSE NULL END) AS representative_name,
                ANY_VALUE(CASE WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != '' THEN pd2.representative_applicability ELSE NULL END) AS representative_applicability,
                ANY_VALUE(CASE WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != '' THEN pd2.representative_category ELSE NULL END) AS representative_category
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
                ROW_NUMBER() OVER(PARTITION BY p.artikul_norm, p.brand_norm ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST) as rn
            FROM parts_data p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
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

    def show_export_interface(self):
        st.header("📤 Умный экспорт данных")
        total_records = self.conn.execute("SELECT COUNT(DISTINCT artikul_norm, brand_norm) FROM parts_data").fetchone()[0]
        st.info(f"Всего записей для экспорта (строк): {total_records:,}")
        if total_records == 0:
            st.warning("База данных пуста или нет связей для экспорта. Сначала загрузите данные.")
            return
        # Выбор колонок
        available_columns = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота",
            "Вес", "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        selected_columns = st.multiselect("Выберите колонки для экспорта", options=available_columns, default=available_columns)

        export_format = st.radio("Выберите формат", ["CSV", "Excel (.xlsx)", "Parquet (для разработчиков)"])

        if export_format == "CSV":
            if st.button("🚀 Экспорт в CSV"):
                output_path = self.data_dir / "auto_parts_report.csv"
                with st.spinner("Идет экспорт..."):
                    self.export_to_csv(output_path, selected_columns)
                with open(output_path, "rb") as f:
                    st.download_button("📥 Скачать CSV", f, "auto_parts_report.csv", "text/csv")

        elif export_format == "Excel (.xlsx)":
            if st.button("📊 Экспорт в Excel"):
                output_path = self.data_dir / "auto_parts_report.xlsx"
                with st.spinner("Идет экспорт..."):
                    self.export_to_excel(output_path, selected_columns)
                with open(output_path, "rb") as f:
                    st.download_button("📥 Скачать Excel", f, "auto_parts_report.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        elif export_format == "Parquet (для разработчиков)":
            if st.button("⚡️ Экспорт в Parquet"):
                output_path = self.data_dir / "auto_parts_report.parquet"
                with st.spinner("Идет экспорт..."):
                    self.export_to_parquet(output_path, selected_columns)
                with open(output_path, "rb") as f:
                    st.download_button("📥 Скачать Parquet", f, "auto_parts_report.parquet", "application/octet-stream")

    def export_to_csv(self, output_path, selected_columns=None):
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()

            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]
            for col in dimension_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col(col).is_not_null())
                        .then(pl.col(col).cast(pl.Utf8))
                        .otherwise(pl.lit(""))
                        .alias(col)
                    )

            buf = io.StringIO()
            df.write_csv(buf, separator=';')
            csv_text = buf.getvalue()

            with open(output_path, 'wb') as f:
                f.write(b'\xef\xbb\xbf')
                f.write(csv_text.encode('utf-8'))

            return True
        except:
            import traceback
            traceback.print_exc()
            st.error("❌ Ошибка при экспорте CSV")
            return False

    def export_to_excel(self, output_path, selected_columns=None):
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        try:
            import pandas as pd
            num_files = (total_records + EXCEL_ROW_LIMIT - 1) // EXCEL_ROW_LIMIT
            base_query = self.build_export_query(selected_columns)
            exported_files = []

            progress_bar = st.progress(0, text=f"Подготовка к экспорту {num_files} файла(ов)...")
            for i in range(num_files):
                progress_bar.progress((i + 1) / num_files, text=f"Экспорт части {i+1} из {num_files}...")
                offset = i * EXCEL_ROW_LIMIT
                query = f"{base_query} LIMIT {EXCEL_ROW_LIMIT} OFFSET {offset}"
                df = self.conn.execute(query).pl()

                # преобразуем числовые колонки в строки
                dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]
                for col in dimension_cols:
                    if col in df.columns:
                        df = df.with_columns(
                            pl.when(pl.col(col).is_not_null())
                            .then(pl.col(col).cast(pl.Utf8))
                            .otherwise(pl.lit(""))
                            .alias(col)
                        )

                file_part_path = self.data_dir / f"{output_path.stem}_part_{i+1}.xlsx"
                df.to_pandas().to_excel(str(file_part_path), index=False)
                exported_files.append(file_part_path)

            progress_bar.empty()

            if num_files > 1:
                st.info("Архивация файлов в ZIP...")
                zip_path = self.data_dir / f"{output_path.stem}.zip"
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in exported_files:
                        zipf.write(file, arcname=file.name)
                        os.remove(file)
                final_path = zip_path
            else:
                final_path = exported_files[0]
                if final_path.name != output_path.name:
                    os.rename(final_path, output_path)
                    final_path = output_path

            return True
        except:
            import traceback
            traceback.print_exc()
            st.error("❌ Ошибка при экспорте Excel")
            return False

    def export_to_parquet(self, output_path, selected_columns=None):
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        try:
            query = self.build_export_query(selected_columns)
            df = self.conn.execute(query).pl()
            df.write_parquet(str(output_path))
            return True
        except:
            import traceback
            traceback.print_exc()
            st.error("❌ Ошибка при экспорте Parquet")
            return False

    def build_export_query(self, selected_columns=None):
        # Стандартный текст описания
        standard_description = """Состояние товара: новый (в упаковке).
Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей.

В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электрику, автомасла, инструмент, а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. 

Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. 

Выбирайте только лучшее — надежность и качество от ведущих производителей."""
        # Колонки
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

        if self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] > 0:
            columns_map.extend([("Цена", '"Цена"'), ("Валюта", '"Валюта"')])

        if selected_columns:
            selected_exprs = [expr for name, expr in columns_map if name in selected_columns]
        else:
            selected_exprs = [expr for _, expr in columns_map]

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
        """

        return query

    def show_data_management(self):
        st.header("🔧 Управление данными")
        st.warning("⚠️ Операции необратимы!")

        operation = st.radio("Выберите операцию", ["Удалить по бренду", "Удалить по артикулу"])

        if operation == "Удалить по бренду":
            st.subheader("Удаление по бренду")
            try:
                brands = self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL ORDER BY brand").pl()
                available_brands = brands['brand'].to_list() if not brands.is_empty() else []
            except:
                available_brands = []

            if available_brands:
                selected_brand = st.selectbox("Выберите бренд", available_brands)
                # Получение нормализованного
                res = self.conn.execute("SELECT brand_norm FROM parts_data WHERE brand = ? LIMIT 1", [selected_brand]).fetchone()
                brand_norm = res[0] if res else self.normalize_key(pl.Series([selected_brand]))[0]
                count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
                st.info(f"Удалится {count} записей для бренда {selected_brand}")
                confirm = st.checkbox("Я подтверждаю удаление", key=f"del_brand_{selected_brand}")
                if st.button("❌ Удалить бренд") and confirm:
                    deleted = self.delete_by_brand(brand_norm)
                    st.success(f"Удалено {deleted} записей")
            else:
                st.warning("Нет доступных брендов.")

        elif operation == "Удалить по артикулу":
            st.subheader("Удаление по артикулу")
            artic_input = st.text_input("Введите артикул")
            if artic_input:
                artic_norm_series = self.normalize_key(pl.Series([artic_input]))
                artic_norm = artic_norm_series[0]
                count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [artic_norm]).fetchone()[0]
                st.info(f"Найдено {count} записей для артикула {artic_input}")
                confirm = st.checkbox("Я подтверждаю удаление", key=f"del_art_{artic_input}")
                if st.button("❌ Удалить артикул") and confirm:
                    deleted = self.delete_by_artikul(artic_norm)
                    st.success(f"Удалено {deleted} записей для артикула {artic_input}")

    def delete_by_brand(self, brand_norm):
        try:
            count1 = self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm]).rowcount
            count2 = self.conn.execute("DELETE FROM cross_references WHERE brand_norm = ?", [brand_norm]).rowcount
            return count1 + count2
        except:
            return 0

    def delete_by_artikul(self, artikul_norm):
        try:
            count1 = self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).rowcount
            count2 = self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm]).rowcount
            return count1 + count2
        except:
            return 0

    def show_statistics(self):
        st.header("📊 Статистика")
        stats = self.get_statistics()
        st.metric("Общее количество артикула", value=f"{stats.get('total_parts', 0):,}")
        st.metric("Количество OE", value=f"{stats.get('total_oe', 0):,}")
        st.metric("Количество брендов", value=f"{stats.get('total_brands', 0):,}")
        st.subheader("ТОП-10 брендов")
        if 'top_brands' in stats and not stats['top_brands'].is_empty():
            st.dataframe(stats['top_brands'].to_pandas())
        st.subheader("Распределение по категориям")
        if 'categories' in stats and not stats['categories'].is_empty():
            st.bar_chart(stats['categories'].to_pandas().set_index('category'))

    def get_statistics(self):
        stats = {}
        try:
            stats['total_parts'] = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
            stats['total_oe'] = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0]
            stats['total_brands'] = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data").fetchone()[0]
            stats['top_brands'] = self.conn.execute("SELECT brand, COUNT(*) as cnt FROM parts_data WHERE brand IS NOT NULL GROUP BY brand ORDER BY cnt DESC LIMIT 10").pl()
            stats['categories'] = self.conn.execute("SELECT COALESCE(representative_category, 'Разное') AS category, COUNT(*) AS cnt FROM parts_data LEFT JOIN oe_data ON parts_data.artikul_norm=oe_data.oe_number_norm GROUP BY category ORDER BY cnt DESC").pl()
        except:
            import traceback
            traceback.print_exc()
            stats = {}
        return stats

    def delete_by_brand(self, brand_norm):
        try:
            count1 = self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm]).rowcount
            count2 = self.conn.execute("DELETE FROM cross_references WHERE brand_norm = ?", [brand_norm]).rowcount
            return count1 + count2
        except:
            return 0

    def delete_by_artikul(self, artikul_norm):
        try:
            count1 = self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).rowcount
            count2 = self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm]).rowcount
            return count1 + count2
        except:
            return 0

def main():
    st.title("🚗 AutoParts Catalog — Масштабируемая система до 10+ млн записей")
    st.markdown("""
    ### 💪 Мощная платформа для работы с большими каталогами автозапчастей
    - Поддержка больших объемов данных
    - Инкрементальные обновления
    - Быстрый экспорт
    - Гибкое управление
    """)

    catalog = AutoPartsCatalog()

    choice = st.sidebar.radio("Навигация", ["Загрузка данных", "Экспорт", "Статистика", "Управление"])

    if choice == "Загрузка данных":
        st.header("📥 Загрузка и обработка файлов")
        col1, col2 = st.columns(2)
        with col1:
            f_oe = st.file_uploader("Основные данные (OE)", type=['xlsx', 'xls'])
            f_cross = st.file_uploader("Кроссы", type=['xlsx', 'xls'])
            f_barcode = st.file_uploader("Штрих-коды", type=['xlsx', 'xls'])
        with col2:
            f_dim = st.file_uploader("Весогабариты", type=['xlsx', 'xls'])
            f_img = st.file_uploader("Изображения", type=['xlsx', 'xls'])
            f_price = st.file_uploader("Прайс-лист", type=['xlsx', 'xls'])

        files = {'oe': f_oe, 'cross': f_cross, 'barcode': f_barcode, 'dimensions': f_dim, 'images': f_img, 'prices': f_price}
        if st.button("🚀 Начать обработку данных"):
            paths = {}
            for k, f in files.items():
                if f:
                    path = catalog.data_dir / f"{k}_{int(time.time())}.xlsx"
                    with open(path, "wb") as ff:
                        ff.write(f.read())
                    paths[k] = str(path)
            if paths:
                catalog.merge_all_data_parallel(paths)
            else:
                st.warning("⚠️ Пожалуйста, загрузите хотя бы один файл для обработки.")

    elif choice == "Экспорт":
        catalog.show_export_interface()

    elif choice == "Статистика":
        catalog.show_statistics()

    elif choice == "Управление":
        catalog.show_data_management()

if __name__ == "__main__":
    main()
