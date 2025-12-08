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
import json

warnings.filterwarnings('ignore')

# Настройка логирования для отладки и информативных сообщений
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ограничение на количество строк в Excel при экспорте
EXCEL_ROW_LIMIT = 1_000_000

class HighVolumeAutoPartsCatalog:
    def __init__(self):
        # Директория для хранения данных
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        # Загрузка конфигурации облачного хранилища, правил ценообразования и категорий
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        # Инициализация базы данных DuckDB
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()

        # Настройка интерфейса Streamlit
        st.set_page_config(
            page_title="AutoParts Catalog 10M+",
            layout="wide",
            page_icon="🚗"
        )

    # =================== Конфигурации облака ===================
    def load_cloud_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации облачного хранилища из файла"""
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
            config_path.write_text(json.dumps(default_config, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_config

    def save_cloud_config(self):
        """Сохранение конфигурации облачного хранилища"""
        config_path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        config_path.write_text(json.dumps(self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

    # =================== Правила ценообразования ===================
    def load_price_rules(self) -> Dict[str, Any]:
        """Загрузка правил ценообразования из файла"""
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
            price_rules_path.write_text(json.dumps(default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_rules

    def save_price_rules(self):
        """Сохранение правил ценообразования"""
        price_rules_path = self.data_dir / "price_rules.json"
        price_rules_path.write_text(json.dumps(self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

    # =================== Правила исключений ===================
    def load_exclusion_rules(self) -> List[str]:
        """Загрузка правил исключения товаров по словам"""
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        if exclusion_path.exists():
            try:
                return [
                    line.strip()
                    for line in exclusion_path.read_text(encoding='utf-8').splitlines()
                    if line.strip()
                ]
            except Exception as e:
                logger.error(f"Ошибка чтения exclusion_rules.txt: {e}")
                return []
        else:
            content = "Кузов\nСтекла\nМасла"
            exclusion_path.write_text(content, encoding='utf-8')
            return ["Кузов", "Стекла", "Масла"]

    def save_exclusion_rules(self):
        """Сохранение правил исключений"""
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        exclusion_path.write_text("\n".join(self.exclusion_rules), encoding='utf-8')

    # =================== Категории товаров ===================
    def load_category_mapping(self) -> Dict[str, str]:
        """Загрузка маппинга категорий"""
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
            content = "\n".join([f"{k}|{v}" for k, v in default_mapping.items()])
            category_path.write_text(content, encoding='utf-8')
            return default_mapping

    def save_category_mapping(self):
        """Сохранение маппинга категорий"""
        category_path = self.data_dir / "category_mapping.txt"
        content = "\n".join([f"{k}|{v}" for k, v in self.category_mapping.items()])
        category_path.write_text(content, encoding='utf-8')

    # =================== Создание базы данных ===================
    def setup_database(self):
        """Создание таблиц базы данных"""
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
        st.info("🛠️ Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_data_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_data_keys ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]
        for index_sql in indexes:
            self.conn.execute(index_sql)
        st.success("🛠️ Индексы созданы.")

    # =================== Вспомогательные функции ===================
    @staticmethod
    def normalize_key(key_series: pl.Series) -> pl.Series:
        """Нормализация ключевых полей (артикул, бренд, OE): удаление мусора, приведение к нижнему регистру"""
        return (key_series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\\-\\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    @staticmethod
    def clean_values(value_series: pl.Series) -> pl.Series:
        """Очистка исходных значений от мусорных символов и апострофов"""
        return (value_series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\\-\\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        """Определение категории на основе названия товара"""
        name_lower = name_series.str.to_lowercase()
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        # Пользовательские правила (приоритет)
        for key, category in self.category_mapping.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower())
            ).then(pl.lit(category))
        # Стандартные правила по ключевым словам
        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выхлопная система': 'глушитель|глушител|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(pattern, literal=False)
            ).then(pl.lit(category))
        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')

    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        """Автоматическое сопоставление колонок по ключевым словам"""
        column_variants = {
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
        """Чтение и предварительная обработка файла"""
        logger.info(f"Начинаю обработку файла: {file_type} ({file_path})")
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return pl.DataFrame()

            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.warning(f"Файл пуст: {file_path}")
                return pl.DataFrame()

            df = pl.read_excel(file_path, engine='calamine')
            if df.is_empty():
                logger.warning(f"Файл прочитан, но не содержит данных: {file_path}")
                return pl.DataFrame()

        except Exception as e:
            logger.exception(f"Не удалось прочитать файл {file_path}: {e}")
            return pl.DataFrame()

        # Определение схемы по типу файла
        schemas = {
            'oe': ['oe_number', 'артикул', 'бренд', 'наименование', 'применимость'],
            'barcode': ['бренд', 'артикул', 'barcode', 'кратность'],
            'dimensions': ['артикул', 'бренд', 'длина', 'ширина', 'высота', 'вес', 'весогабариты'],
            'images': ['артикул', 'бренд', 'ссылка'],
            'cross': ['oe_number', 'артикул', 'бренд'],
            'prices': ['артикул', 'бренд', 'цена', 'валюта']
        }
        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)
        if not column_mapping:
            logger.warning(f"Не удалось определить колонки для файла {file_type}. Доступные: {df.columns}")
            return pl.DataFrame()

        df = df.rename(column_mapping)

        # Очистка и нормализация ключевых полей
        for col in ['артикул', 'бренд', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))

        # Удаление дубликатов по ключам
        key_cols = [col for col in ['oe_number', 'артикул', 'бренд'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        # Нормализация ключевых полей
        for col in ['артикул', 'бренд', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(
                    self.normalize_key(pl.col(col)).alias(f"{col}_норм")
                )

        return df

    # =================== Операции с базой данных ===================
    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        """Обновление или вставка данных (UPSERT)"""
        if df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        pk_str = ", ".join(f'"{c}"' for c in pk)
        temp_view_name = f"temp_{table_name}_{int(time.time())}"

        # Регистрация DataFrame как временной таблицы
        self.conn.register(temp_view_name, df.to_arrow())

        # Колонки для обновления, исключая первичные ключи
        update_cols = [col for col in cols if col not in pk]

        # Формируем SQL-запрос
        if not update_cols:
            on_conflict_action = "DO NOTHING"
        else:
            update_clause = ", ".join([f'"{col}" = excluded."{col}"' for col in update_cols])
            on_conflict_action = f"DO UPDATE SET {update_clause}"

        sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM {temp_view_name}
        ON CONFLICT ({pk_str}) {on_conflict_action};
        """

        try:
            self.conn.execute(sql)
            logger.info(f"Успешно обновлено/вставлено {len(df)} записей в {table_name}")
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(f"Ошибка при записи в таблицу {table_name}. Детали в логе.")
        finally:
            self.conn.unregister(temp_view_name)

    def upsert_prices(self, price_df: pl.DataFrame):
        """Обновление цен с фильтрацией по диапазону"""
        if price_df.is_empty():
            return
        # Нормализация ключей
        if 'артикул' in price_df.columns and 'бренд' in price_df.columns:
            price_df = price_df.with_columns([
                self.normalize_key(pl.col('артикул')).alias('артикул_норм'),
                self.normalize_key(pl.col('бренд')).alias('бренд_норм')
            ])
        # Установка валюты по умолчанию
        if 'валюта' not in price_df.columns:
            price_df = price_df.with_columns(pl.lit('RUB').alias('валюта'))
        # Фильтрация по диапазону цен
        price_df = price_df.filter(
            (pl.col('цена') >= self.price_rules['min_price']) &
            (pl.col('цена') <= self.price_rules['max_price'])
        )
        self.upsert_data('prices', price_df, ['артикул_норм', 'бренд_норм'])

    # =================== Основной процесс загрузки и обработки ===================
    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Загрузка и обновление данных в базе с прогресс-баром"""
        st.info("🔄 Начинаю загрузку и обновление базы данных...")
        steps = [s for s in ['oe', 'cross', 'parts', 'prices'] if s in dataframes]
        num_steps = len(steps)
        progress_bar = st.progress(0, text="Подготовка к обновлению...")
        step_counter = 0

        # Обработка OE-данных
        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка OE...")
            df = dataframes['oe'].filter(pl.col('oe_number_норм') != "")
            oe_df = df.select(['oe_number_норм', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_норм'])
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))
            self.upsert_data('oe_data', oe_df, ['oe_number_норм'])

            # Связи OE -> Артикула
            cross_df_from_oe = df.filter(pl.col('артикул_норм') != "").select(['oe_number_норм', 'артикул_норм', 'бренд_норм']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, ['oe_number_норм', 'артикул_норм', 'бренд_норм'])

        # Обработка кроссов
        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка кроссов...")
            df = dataframes['cross'].filter((pl.col('oe_number_норм') != "") & (pl.col('артикул_норм') != ""))
            cross_df_from_cross = df.select(['oe_number_норм', 'артикул_норм', 'бренд_норм']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, ['oe_number_норм', 'артикул_норм', 'бренд_норм'])

        # Обработка цен
        if 'prices' in dataframes:
            price_df = dataframes['prices']
            if not price_df.is_empty():
                st.info("💰 Обработка цен...")
                self.upsert_prices(price_df)
                st.success(f"✅ Обновлено {len(price_df)} ценовых записей")

        # Обработка артикула и дополнительных данных
        step_counter += 1
        progress_bar.progress(step_counter / (num_steps +1), text=f"({step_counter}/{num_steps}) Обработка артикула и дополнительных данных...")

        progress_bar.progress(1.0, text="Обновление завершено!")
        time.sleep(1)
        progress_bar.empty()

    # =================== Построение сложных SQL-запросов для экспорта ===================
    def build_export_query(self, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> str:
        """Построение SQL-запроса для экспорта данных"""
        # Текст шаблона для описания
        standard_description = """Состояние товара: новый (в упаковке).
Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей.
В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электрику, автомасла, инструмент, а также другие комплектующие, полностью соответствующие стандартам качества и безопасности.
Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса.
Выбирайте только лучшее — надежность и качество от ведущих производителей."""

        # Конструкция для цен
        price_select = ""
        if include_prices:
            if apply_markup:
                global_markup = self.price_rules['global_markup']
                price_select = """
                CASE
                    WHEN p_brand.brand IS NOT NULL AND pr.price IS NOT NULL
                    THEN pr.price * (1 + COALESCE(brm.markup, {global_markup}))
                    ELSE pr.price
                END AS "Цена",
                COALESCE(pr.currency, 'RUB') AS "Валюта",
                """.format(global_markup=global_markup)
            else:
                price_select = """
                pr.price AS "Цена",
                COALESCE(pr.currency, 'RUB') AS "Валюта",
                """

        # Условие исключений по названию
        exclusion_conditions = " OR ".join([f"r.representative_name NOT ILIKE '%{ex}%'" for ex in self.exclusion_rules if ex.strip()])
        exclusion_where = f"AND ({exclusion_conditions})" if exclusion_conditions else ""

        # Колонки для отображения
        columns_map = [
            ("Артикул бренда", 'r.artikul AS "Артикул бренда"'),
            ("Бренд", 'r.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"'),
            ("Описание", 'CONCAT(COALESCE(r.description, ""), dt.text) AS "Описание"'),
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

        # Выбор колонок
        if selected_columns:
            selected_exprs = [expr for name, expr in columns_map if name in selected_columns]
        else:
            selected_exprs = [expr for _, expr in columns_map]

        # Построение CTE-запросов (подготовка текста)
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
            SELECT DISTINCT
                p.artikul_norm,
                p.brand_norm,
                cr.oe_number_norm
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
            SELECT DISTINCT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
            FROM Level1Analogs
            UNION
            SELECT DISTINCT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
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

        select_clause = ",\n        ".join(selected_exprs)

        # Собираем полный запрос
        query = f"""
        {ctes}
        SELECT
            {price_select}
            {select_clause}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        WHERE r.rn = 1
        {exclusion_where}
        ORDER BY r.brand, r.artikul
        """
        return query.strip()

    # =================== Экспорт данных ===================
    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт данных в CSV, оптимизация типов и размера"""
        total_records = self.conn.execute("""
            SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)
        """).fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total_records:,} записей в CSV...")
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()

            # Преобразование размерных колонок в строки
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]
            for col in dimension_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col(col).is_not_null())
                         .then(pl.col(col).cast(pl.Utf8))
                         .otherwise(pl.lit(""))
                         .alias(col)
                    )

            # Запись в CSV с BOM для Excel
            buf = io.StringIO()
            df.write_csv(buf, separator=';')
            csv_text = buf.getvalue()

            with open(output_path, 'wb') as f:
                f.write(b'\xef\xbb\xbf')  # BOM для корректной работы в Excel
                f.write(csv_text.encode('utf-8'))

            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в CSV: {output_path} ({file_size:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в CSV")
            st.error(f"❌ Ошибка экспорта в CSV: {e}")
            return False

    def show_export_interface(self):
        """Интерфейс выбора формата экспорта и запуск"""
        st.header("📤 Умный экспорт данных")
        total_records = self.conn.execute("""
            SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)
        """).fetchone()[0]
        st.info(f"Всего записей для экспорта: {total_records:,}")
        if total_records == 0:
            st.warning("База данных пуста или нет связей для экспорта.")
            return

        available_columns = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота",
            "Вес", "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        # Проверка цен
        if self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] > 0:
            available_columns.extend(["Цена", "Валюта"])

        selected_columns = st.multiselect("Выберите колонки для экспорта", options=available_columns, default=available_columns)

        export_format = st.radio("Выберите формат экспорта:", ["CSV", "Excel (.xlsx)", "Parquet (для разработчиков)"], index=0)

        if st.button("🚀 Выполнить экспорт", type="primary"):
            output_path = self.data_dir / f"auto_parts_report.{export_format.lower().replace(' ', '_')}"
            with st.spinner("Формирование отчета..."):
                if export_format == "CSV":
                    success = self.export_to_csv_optimized(str(output_path), selected_columns if selected_columns else None)
                elif export_format == "Excel (.xlsx)":
                    success = self.export_to_excel_optimized(str(output_path), selected_columns if selected_columns else None)
                elif export_format == "Parquet (для разработчиков)":
                    success = self.export_to_parquet(str(output_path), selected_columns if selected_columns else None)
                else:
                    st.warning("Неподдерживаемый формат")
                    success = False

                if success:
                    with open(output_path, "rb") as f:
                        st.download_button("⬇️ Скачать файл", f, output_path.name, mime="application/octet-stream")

    def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт в Excel с разбивкой по листам при лимите"""
        total_records = self.conn.execute("""
            SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)
        """).fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта в Excel")
            return False
        st.info(f"📊 Подготовка экспорта в Excel: {total_records:,} записей...")

        try:
            import pandas as pd
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = pd.read_sql(query, self.conn)

            # Преобразование размерных колонок
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]
            for col in dimension_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace({r'^nan$': ''}, regex=True)

            # Логика разбивки по листам
            if len(df) <= EXCEL_ROW_LIMIT:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Данные')
            else:
                num_sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for i in range(num_sheets):
                        start_idx = i * EXCEL_ROW_LIMIT
                        end_idx = min((i+1) * EXCEL_ROW_LIMIT, len(df))
                        df.iloc[start_idx:end_idx].to_excel(writer, index=False, sheet_name=f"Данные_{i+1}")

            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в Excel: {output_path} ({file_size:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в Excel")
            st.error(f"❌ Ошибка экспорта в Excel: {e}")
            return False

    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт в Parquet"""
        st.info("📦 Подготовка экспорта в Parquet...")
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в Parquet: {output_path} ({file_size:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в Parquet")
            st.error(f"❌ Ошибка экспорта в Parquet: {e}")
            return False

    # =================== Статистика ===================
    def show_statistics(self):
        """Отображение статистики базы данных"""
        st.header("📈 Статистика по базе данных")
        stats = {}
        try:
            stats['parts'] = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
            stats['oe'] = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0]
            stats['cross'] = self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0]
            stats['prices'] = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            stats['brands'] = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data").fetchone()[0]
            stats['unique_parts'] = self.conn.execute("""
                SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)
            """).fetchone()[0]
            avg_price_result = self.conn.execute("SELECT AVG(price) FROM prices WHERE price IS NOT NULL").fetchone()
            stats['avg_price'] = round(avg_price_result[0], 2) if avg_price_result and avg_price_result[0] else 0.0
        except Exception as e:
            st.error(f"❌ Ошибка при сборе статистики: {e}")
            return

        # Отображение метрик
        col1, col2, col3 = st.columns(3)
        col1.metric("Уникальные товары", f"{stats['unique_parts']:,}")
        col2.metric("Бренды", f"{stats['brands']:,}")
        col3.metric("Средняя цена", f"{stats['avg_price']} ₽")

        col1, col2, col3 = st.columns(3)
        col1.metric("Записи (parts)", f"{stats['parts']:,}")
        col2.metric("OE-номера", f"{stats['oe']:,}")
        col3.metric("Кроссы", f"{stats['cross']:,}")

        col1, col2 = st.columns(2)
        col1.metric("Ценовые записи", f"{stats['prices']:,}")
        col2.metric("Размер файла БД", f"{os.path.getsize(self.db_path) / (1024**2):.1f} МБ")

        # Топ-бренды
        st.subheader("🏆 Топ-10 брендов по количеству артикулов")
        try:
            top_brands = self.conn.execute("""
                SELECT brand, COUNT(*) as cnt
                FROM parts_data
                WHERE brand IS NOT NULL
                GROUP BY brand
                ORDER BY cnt DESC
                LIMIT 10
            """).pl()
            st.dataframe(top_brands.to_pandas(), use_container_width=True)
        except Exception as e:
            st.warning(f"Не удалось загрузить топ брендов: {e}")

        # Распределение по категориям
        st.subheader("🗂️ Распределение по категориям")
        try:
            category_stats = self.conn.execute("""
                SELECT 
                    COALESCE(representative_category, 'Разное') as category,
                    COUNT(*) as cnt
                FROM (
                    SELECT DISTINCT p.artikul_norm, p.brand_norm, pd.representative_category
                    FROM parts_data p
                    LEFT JOIN part_details_view pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
                )
                GROUP BY category
                ORDER BY cnt DESC
                LIMIT 15
            """).pl()
            st.dataframe(category_stats.to_pandas(), use_container_width=True)
        except Exception as e:
            st.warning("Не удалось загрузить статистику по категориям")

    # =================== Параллельная загрузка файлов ===================
    def merge_all_data_parallel(self, file_paths: Dict[str, str], max_workers: int = 4) -> Dict[str, pl.DataFrame]:
        """Параллельное чтение и подготовка файлов для ускорения обработки"""
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for file_type, file_path in file_paths.items():
                if file_path and os.path.exists(file_path):
                    futures[executor.submit(self.read_and_prepare_file, file_path, file_type)] = file_type
            for future in as_completed(futures):
                file_type = futures[future]
                try:
                    df = future.result()
                    if not df.is_empty():
                        results[file_type] = df
                        logger.info(f"✅ Обработан файл: {file_type}")
                    else:
                        logger.warning(f"⚠️ Файл пуст или не обработан: {file_type}")
                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке {file_type}: {e}")
        return results

# =================== Основной интерфейс и логика ===================
def main():
    st.title("🚗 AutoParts Catalog — Масштабируемая система для 10+ млн записей")
    st.markdown("""
    ### 💼 Профессиональная платформа для управления каталогами автозапчастей
    - Поддержка больших данных
    - Инкрементальные обновления
    - Мультиформатный экспорт
    - Гибкая настройка
    """)

    # Создаем экземпляр системы
    catalog = HighVolumeAutoPartsCatalog()

    # Боковая навигация
    st.sidebar.title("🧭 Навигация")
    menu_option = st.sidebar.radio("Выберите раздел:", [
        "Загрузка данных",
        "Экспорт",
        "Статистика",
        "Управление данными"
    ])

    # Обработка выбранного раздела
    if menu_option == "Загрузка данных":
        st.header("📥 Загрузка и обновление данных")
        col1, col2 = st.columns(2)
        with col1:
            oe_file = st.file_uploader("1. Основные данные (OE)", type=['xlsx', 'xls'])
            cross_file = st.file_uploader("2. Кроссы (OE → Артикул)", type=['xlsx', 'xls'])
            barcode_file = st.file_uploader("3. Штрих-коды и кратность", type=['xlsx', 'xls'])
        with col2:
            dimensions_file = st.file_uploader("4. Весогабариты", type=['xlsx', 'xls'])
            images_file = st.file_uploader("5. Ссылки на изображения", type=['xlsx', 'xls'])
            prices_file = st.file_uploader("6. Прайс-лист с ценами", type=['xlsx', 'xls'])

        # Сохраняем файлы на диске
        saved_paths = {}
        for file_type, uploaded_file in {
            'oe': oe_file,
            'cross': cross_file,
            'barcode': barcode_file,
            'dimensions': dimensions_file,
            'images': images_file,
            'prices': prices_file
        }.items():
            if uploaded_file is not None:
                save_path = catalog.data_dir / f"upload_{file_type}_{int(time.time())}.xlsx"
                with open(save_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                saved_paths[file_type] = str(save_path)

        if st.button("🚀 Обработать и загрузить данные"):
            if not saved_paths:
                st.warning("Загрузите хотя бы один файл")
            else:
                with st.spinner("Чтение и обработка файлов..."):
                    dataframes = catalog.merge_all_data_parallel(saved_paths)
                if dataframes:
                    with st.spinner("Загрузка в базу..."):
                        catalog.process_and_load_data(dataframes)
                else:
                    st.error("❌ Не удалось обработать ни один файл")
    elif menu_option == "Экспорт":
        # Экспорт данных
        catalog.show_export_interface()
    elif menu_option == "Статистика":
        # Статистика
        catalog.show_statistics()
    elif menu_option == "Управление данными":
        # Управление удалением и настройками
        catalog.show_data_management()

if __name__ == "__main__":
    main()
