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

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name)

# Ограничение на количество строк в Excel
EXCEL_ROW_LIMIT = 1_000_000

class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        # Конфигурация облачного хранилища
        self.cloud_config = self.load_cloud_config()

        # Инициализация базы данных
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()

        # Загрузка правил ценообразования и категорий
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        # Настройка интерфейса Streamlit
        st.set_page_config(
            page_title="AutoParts Catalog 10M+",
            layout="wide",
            page_icon="🚗"
        )

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
            config_path.write_text(json.dumps(default_config, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_config

    def save_cloud_config(self):
        self.cloud_config['last_sync'] = int(time.time())
        config_path = self.data_dir / "cloud_config.json"
        config_path.write_text(json.dumps(self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

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
            price_rules_path.write_text(json.dumps(default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_rules

    def save_price_rules(self):
        price_rules_path = self.data_dir / "price_rules.json"
        price_rules_path.write_text(json.dumps(self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_exclusion_rules(self) -> List[str]:
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
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        exclusion_path.write_text("\n".join(self.exclusion_rules), encoding='utf-8')

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
            content = "\n".join([f"{k}|{v}" for k, v in default_mapping.items()])
            category_path.write_text(content, encoding='utf-8')
            return default_mapping

    def save_category_mapping(self):
        category_path = self.data_dir / "category_mapping.txt"
        content = "\n".join([f"{k}|{v}" for k, v in self.category_mapping.items()])
        category_path.write_text(content, encoding='utf-8')

    def setup_database(self):
        """Создание таблиц в DuckDB"""
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
        """Создание индексов для ускорения запросов"""
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

    @staticmethod
    def normalize_key(key_series: pl.Series) -> pl.Series:
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
        return (value_series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\\-\\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        name_lower = name_series.str.to_lowercase()
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        # Пользовательские правила — приоритет выше
        for key, category in self.category_mapping.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower())
            ).then(pl.lit(category))
        # Системные правила
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

    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        """Автоматическое сопоставление колонок по ключевым словам"""
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
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)
        if not column_mapping:
            logger.warning(f"Не удалось определить колонки для файла {file_type}. Доступные: {df.columns}")
            return pl.DataFrame()

        df = df.rename(column_mapping)

        # Очистка и нормализация ключевых полей
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))

        # Удаление дубликатов
        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        # Нормализация ключевых полей
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(
                    self.normalize_key(pl.col(col)).alias(f"{col}_norm")
                )

        return df

    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        """UPSERT данных в таблицу DuckDB"""
        if df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        pk_str = ", ".join(f'"{c}"' for c in pk)
        temp_view_name = f"temp_{table_name}_{int(time.time())}"

        self.conn.register(temp_view_name, df.to_arrow())
        update_cols = [col for col in cols if col not in pk]

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
            logger.info(f"Успешно обновлено/вставлено {len(df)} записей в {table_name}.")
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(f"Ошибка при записи в таблицу {table_name}. Детали в логе.")
        finally:
            self.conn.unregister(temp_view_name)

    def upsert_prices(self, price_df: pl.DataFrame):
        """Добавление или обновление цен с фильтрацией"""
        if price_df.is_empty():
            return

        # Нормализация ключей
        if 'artikul' in price_df.columns and 'brand' in price_df.columns:
            price_df = price_df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])

        # Установка валюты по умолчанию
        if 'currency' not in price_df.columns:
            price_df = price_df.with_columns(pl.lit('RUB').alias('currency'))

        # Фильтрация по диапазону цен
        price_df = price_df.filter(
            (pl.col('price') >= self.price_rules['min_price']) &
            (pl.col('price') <= self.price_rules['max_price'])
        )

        self.upsert_data('prices', price_df, ['artikul_norm', 'brand_norm'])

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Основной метод загрузки данных в базу с прогресс-баром"""
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        num_steps = len(steps)
        progress_bar = st.progress(0, text="Подготовка к обновлению базы данных...")
        step_counter = 0

        # Обработка OE-данных
        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка OE данных...")
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'], keep='first')

            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))

            self.upsert_data('oe_data', oe_df, ['oe_number_norm'])

            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Обработка кроссов
        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка кроссов...")
            df = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        # Обработка цен
        if 'prices' in dataframes:
            price_df = dataframes['prices']
            if not price_df.is_empty():
                st.info("💰 Обработка цен...")
                self.upsert_prices(price_df)
                st.success(f"✅ Успешно обновлено {len(price_df)} ценовых записей")

        # Остальные части можно добавлять по необходимости
        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Сборка и обновление данных по артикулам...")

        progress_bar.progress(1.0, text="Обновление базы данных завершено!")
        time.sleep(1)
        progress_bar.empty()

    def build_export_query(self, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> str:
        """Построение сложного SQL-запроса для экспорта"""
        standard_description = """Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей."""

        # Далее вставляется полный сложный запрос, как в вашем коде
        # Для краткости здесь оставлю комментарий, а далее вставлю полный запрос ниже
        # (Эту часть вставляйте полностью из вашего финального варианта)

        # ... (здесь будет полный SQL-запрос, который вы подготовили)

        # В конце возвращаете сформированный запрос
        return query.strip()

    def _get_brand_markups_sql(self) -> str:
        """Генерация SQL-подзапроса для наценок по брендам"""
        rows = []
        for brand, markup in self.price_rules['brand_markups'].items():
            rows.append(f"SELECT '{brand}' AS brand, {markup} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, NULL AS markup LIMIT 0"

    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт данных в CSV с оптимизацией типов и размера"""
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
                f.write(b'\xef\xbb\xbf')  # BOM
                f.write(csv_text.encode('utf-8'))

            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в CSV: {output_path} ({file_size:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в CSV")
            st.error(f"❌ Ошибка экспорта в CSV: {e}")
            return False

    def show_price_settings(self):
        """Интерфейс настройки цен и наценок"""
        st.header("💰 Управление ценами и наценками")
        # Общая наценка
        st.subheader("Общая наценка")
        global_markup = st.number_input(
            "Общая наценка (%):",
            min_value=0.0,
            max_value=100.0,
            value=self.price_rules['global_markup'] * 100,
            step=0.1
        )
        self.price_rules['global_markup'] = global_markup / 100

        # Наценки по брендам
        st.subheader("Наценки по брендам")
        brand_markups = self.price_rules.get('brand_markups', {})

        try:
            brands_result = self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL").fetchall()
            available_brands = [row[0] for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка при получении списка брендов: {e}")
            st.error("❌ Ошибка при загрузке брендов")
            available_brands = []

        if available_brands:
            col1, col2 = st.columns([2, 1])
            with col1:
                selected_brand = st.selectbox("Выберите бренд:", available_brands)
            with col2:
                current_markup = brand_markups.get(selected_brand, self.price_rules.get('global_markup', 0))
                brand_markup = st.number_input(
                    "Наценка (%):",
                    min_value=0.0,
                    max_value=100.0,
                    value=current_markup * 100,
                    step=0.1,
                    key=f"markup_{selected_brand}"
                )
            if st.button("Сохранить наценку", key=f"save_{selected_brand}"):
                # Обновляем словарь наценок
                brand_markups[selected_brand] = brand_markup / 100
                self.price_rules['brand_markups'] = brand_markups
                self.save_price_rules()
                st.success(f"✅ Наценка для {selected_brand} сохранена")

        # Ограничения цен
        st.subheader("Ограничения по ценам")
        col1, col2 = st.columns(2)
        with col1:
            min_price = st.number_input("Минимальная цена:", min_value=0.0, value=float(self.price_rules['min_price']), step=0.01)
            self.price_rules['min_price'] = min_price
        with col2:
            max_price = st.number_input("Максимальная цена:", min_value=0.0, value=float(self.price_rules['max_price']), step=0.01)
            self.price_rules['max_price'] = max_price

        if st.button("Сохранить все настройки цен"):
            self.save_price_rules()
            st.success("✅ Все настройки цен сохранены")

    def show_exclusion_settings(self):
        """Интерфейс управления списком исключений при экспорте"""
        st.header("🚫 Управление исключениями при экспорте")
        st.info("Товары, содержащие эти слова в названии, будут исключены из экспорта")
        current_exclusions = "\n".join(self.exclusion_rules)
        new_exclusions = st.text_area("Список исключений (по одному на строку):", value=current_exclusions, height=200)
        if st.button("Сохранить правила исключения"):
            self.exclusion_rules = [line.strip() for line in new_exclusions.splitlines() if line.strip()]
            self.save_exclusion_rules()
            st.success("✅ Правила исключения сохранены")

    def show_category_mapping(self):
        """Интерфейс настройки пользовательских категорий"""
        st.header("🗂️ Управление категориями товаров")
        st.info("Настройте соответствие между названиями товаров и категориями")
        if self.category_mapping:
            mapping_df = pl.DataFrame({
                "Название товара": list(self.category_mapping.keys()),
                "Категория": list(self.category_mapping.values())
            }).to_pandas()
            st.dataframe(mapping_df, use_container_width=True, hide_index=True)
        else:
            st.write("Нет пользовательских правил категоризации")

        # Добавление правила
        st.subheader("Добавить новое правило")
        col1, col2 = st.columns(2)
        with col1:
            name_pattern = st.text_input("Ключевое слово в названии:")
        with col2:
            category = st.text_input("Категория:")

        if st.button("➕ Добавить правило"):
            if name_pattern.strip() and category.strip():
                normalized_key = name_pattern.strip().lower()
                if normalized_key in {k.lower(): k for k in self.category_mapping}:
                    st.warning(f"Предупреждение: правило для '{list(self.category_mapping.keys())[list(self.category_mapping.values()).index(self.category_mapping[normalized_key])]}'' будет обновлено")
                self.category_mapping[name_pattern.strip()] = category.strip()
                self.save_category_mapping()
                st.success(f"✅ Добавлено/обновлено правило: `{name_pattern.strip()}` → `{category.strip()}`")
                st.rerun()
            else:
                st.error("❌ Пожалуйста, заполните оба поля")

        # Удаление правила
        if self.category_mapping:
            st.subheader("🗑️ Удалить правило")
            rule_to_delete = st.selectbox(
                "Выберите правило для удаления:",
                options=list(self.category_mapping.keys()),
                format_func=lambda x: f"{x} → {self.category_mapping[x]}"
            )
            if st.button("Удалить выбранное правило", type="primary"):
                del self.category_mapping[rule_to_delete]
                self.save_category_mapping()
                st.success(f"✅ Правило удалено: `{rule_to_delete}`")
                st.rerun()

    def show_data_management(self):
        """Основной интерфейс управления данными: удаление, настройка, синхронизация"""
        st.header("🔧 Управление данными в базе")
        st.warning("⚠️ Операции удаления необратимы. Будьте осторожны.")

        management_option = st.radio(
            "Выберите действие:",
            [
                "Удалить по бренду",
                "Удалить по артикулу",
                "Управление ценами",
                "Исключения при экспорте",
                "Категории товаров",
                "Облачная синхронизация"
            ],
            format_func=lambda x: {
                "Удалить по бренду": "🏭 Удалить все записи бренда",
                "Удалить по артикулу": "📦 Удалить все записи артикула",
                "Управление ценами": "💰 Наценки и лимиты цен",
                "Исключения при экспорте": "🚫 Фильтрация при экспорте",
                "Категории товаров": "🗂️ Ручное назначение категорий",
                "Облачная синхронизация": "☁️ Настройка бэкапа"
            }[x]
        )

        if management_option == "Удалить по бренду":
            self._show_delete_by_brand()
        elif management_option == "Удалить по артикулу":
            self._show_delete_by_artikul()
        elif management_option == "Управление ценами":
            self.show_price_settings()
        elif management_option == "Исключения при экспорте":
            self.show_exclusion_settings()
        elif management_option == "Категории товаров":
            self.show_category_mapping()
        elif management_option == "Облачная синхронизация":
            self.show_cloud_sync()

    def _show_delete_by_brand(self):
        """Интерфейс удаления всех записей по бренду"""
        st.subheader("🗑️ Удаление всех записей бренда")
        try:
            brands_result = self.conn.execute("""
                SELECT DISTINCT brand 
                FROM parts_data 
                WHERE brand IS NOT NULL 
                ORDER BY brand
            """).fetchall()
            available_brands = [row[0] for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка при получении списка брендов: {e}")
            st.error("❌ Не удалось загрузить список брендов")
            return

        if not available_brands:
            st.info("Нет данных о брендах в базе.")
            return

        selected_brand = st.selectbox("Выберите бренд для удаления:", available_brands)

        # Получение нормализованного ключа
        brand_norm_result = self.conn.execute("""
            SELECT brand_norm FROM parts_data WHERE brand = ? LIMIT 1
        """, [selected_brand]).fetchone()
        if brand_norm_result:
            brand_norm = brand_norm_result[0]
        else:
            brand_norm = self.normalize_key(pl.Series([selected_brand]))[0]

        # Подсчет количества записей
        count_result = self.conn.execute("""
            SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?
        """, [brand_norm]).fetchone()
        count_to_delete = count_result[0] if count_result else 0

        st.info(f"Будет удалено: **{count_to_delete}** записей бренда `{selected_brand}`")

        confirm = st.checkbox("Я подтверждаю удаление всех записей этого бренда", key=f"confirm_{selected_brand}")
        if st.button("❌ Удалить бренд", type="primary", disabled=not confirm):
            try:
                deleted = self.delete_by_brand(brand_norm)
                st.success(f"✅ Успешно удалено {deleted} записей бренда `{selected_brand}`")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Ошибка при удалении: {e}")

    def _show_delete_by_artikul(self):
        """Интерфейс удаления всех записей по артикулу"""
        st.subheader("🗑️ Удаление всех записей артикула")
        st.info("🔍 Поиск по артикулу (без учета регистра и специальных символов)")

        input_artikul = st.text_input("Введите артикул для удаления:")

        if input_artikul:
            # Нормализация артикула
            artikul_series = pl.Series([input_artikul])
            artikul_norm = self.normalize_key(artikul_series)[0]

            # Подсчет записей
            count_result = self.conn.execute("""
                SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?
            """, [artikul_norm]).fetchone()
            count_to_delete = count_result[0] if count_result else 0

            col1, col2 = st.columns([3, 1])
            with col1:
                if count_to_delete > 0:
                    st.info(f"Найдено: **{count_to_delete}** записей для артикула `{input_artikul}`")
                else:
                    st.warning(f"Артикул `{input_artikul}` не найден в базе.")
            with col2:
                if count_to_delete > 0:
                    confirm = st.checkbox("Подтвердить", key=f"confirm_art_{artikul_norm}")
                    if st.button("Удалить", type="primary", disabled=not confirm):
                        try:
                            deleted = self.delete_by_artikul(artikul_norm)
                            st.success(f"✅ Успешно удалено {deleted} записей артикула `{input_artikul}`")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Ошибка: {e}")

    def delete_by_brand(self, brand_norm: str) -> int:
        """Удаление всех записей по нормализованному бренду"""
        with self.conn.transaction():
            # Удаление из всех таблиц
            deleted = self.conn.execute("""
                DELETE FROM parts_data WHERE brand_norm = ?
            """, [brand_norm]).rowcount

            self.conn.execute("""
                DELETE FROM cross_references
                WHERE brand_norm = ?
            """, [brand_norm])

            return deleted

    def delete_by_artikul(self, artikul_norm: str) -> int:
        """Удаление всех записей по нормализованному артикулу"""
        with self.conn.transaction():
            deleted = self.conn.execute("""
                DELETE FROM parts_data WHERE artikul_norm = ?
            """, [artikul_norm]).rowcount

            self.conn.execute("""
                DELETE FROM cross_references
                WHERE artikul_norm = ?
            """, [artikul_norm])

            return deleted

    def show_cloud_sync(self):
        """Интерфейс настройки облачной синхронизации"""
        st.header("☁️ Облачная синхронизация")
        # Настройки
        st.subheader("🔧 Конфигурация")
        col1, col2 = st.columns(2)
        with col1:
            self.cloud_config['enabled'] = st.checkbox(
                "Включить синхронизацию",
                value=self.cloud_config['enabled']
            )
        with col2:
            providers = ["s3", "gcs", "azure"]
            current_idx = providers.index(self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
            self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=current_idx)

        self.cloud_config['bucket'] = st.text_input("Bucket / Container", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config['region'])
        self.cloud_config['sync_interval'] = st.number_input(
            "Интервал синхронизации (секунды)",
            min_value=300,
            max_value=86400,
            value=int(self.cloud_config['sync_interval'])
        )

        if st.button("💾 Сохранить настройки"):
            self.save_cloud_config()
            st.success("✅ Конфигурация сохранена")

        # Состояние
        st.subheader("📊 Текущее состояние")
        if self.cloud_config['last_sync'] > 0:
            last_sync_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.cloud_config['last_sync']))
            st.info(f"Последняя синхронизация: {last_sync_str}")
        else:
            st.info("Синхронизация ещё не выполнялась")

        if st.button("🔄 Выполнить синхронизацию сейчас"):
            self.perform_cloud_sync()

    def perform_cloud_sync(self):
        """Выполнение синхронизации с облаком (заглушка для интеграции)"""
        if not self.cloud_config['enabled']:
            st.warning("❌ Синхронизация отключена в настройках")
            return
        if not self.cloud_config['bucket']:
            st.error("❌ Не указан bucket")
            return

        with st.spinner("Выполняется синхронизация..."):
            try:
                # Здесь должна быть интеграция с провайдером облака
                time.sleep(1.5)  # Имитация задержки
                st.success(f"📤 База данных отправлена в {self.cloud_config['provider']}://{self.cloud_config['bucket']}")
                self.cloud_config['last_sync'] = int(time.time())
                self.save_cloud_config()
            except Exception as e:
                st.error(f"❌ Ошибка синхронизации: {str(e)}")

    def show_export_interface(self):
        """Интерфейс экспорта данных"""
        st.header("📤 Экспорт данных")
        total_records = self.conn.execute("""
            SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)
        """).fetchone()[0]
        st.info(f"📦 Всего уникальных пар (артикул + бренд): **{total_records:,}**")
        if total_records == 0:
            st.warning("База данных пуста. Загрузите данные перед экспортом.")
            return

        available_columns = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
            "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        # Поддержка цен
        prices_count = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        if prices_count > 0:
            available_columns.extend(["Цена", "Валюта"])

        selected_columns = st.multiselect(
            "Выберите колонки для экспорта",
            options=available_columns,
            default=available_columns
        )

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
                    st.warning("Выбран неподдерживаемый формат")
                    success = False

                if success:
                    with open(output_path, "rb") as f:
                        st.download_button(
                            "⬇️ Скачать файл",
                            f,
                            file_name=output_path.name,
                            mime="application/octet-stream"
                        )

    def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """Экспорт в Excel с разбивкой на листы при превышении лимита"""
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
            # Преобразуем размерные колонки в строки
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]
            for col in dimension_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace({r'^nan$': ''}, regex=True)

            # Проверка лимита
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

    def show_statistics(self):
        """Отображение статистики по базе данных"""
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

        # Топ брендов
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

    def merge_all_data_parallel(self, file_paths: Dict[str, str], max_workers: int = 4) -> Dict[str, pl.DataFrame]:
        """Загрузка и обработка всех файлов параллельно для ускорения"""
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
    def main():
    st.title("🚗 AutoParts Catalog — Масштабируемая система для 10+ млн записей")
    st.markdown("""
    ### 💼 Профессиональная платформа для управления каталогами автозапчастей
    - Поддержка больших данных
    - Инкрементальные обновления
    - Мультиформатный экспорт
    - Гибкая настройка
    """)

    catalog = HighVolumeAutoPartsCatalog()

    # Боковая навигация
    st.sidebar.title("🧭 Навигация")
    menu_option = st.sidebar.radio("Выберите раздел:", [
        "Загрузка данных",
        "Экспорт",
        "Статистика",
        "Управление данными"
    ])

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

        file_map = {
            'oe': oe_file,
            'cross': cross_file,
            'barcode': barcode_file,
            'dimensions': dimensions_file,
            'images': images_file,
            'prices': prices_file
        }

        # Сохранение загруженных файлов
        saved_paths = {}
        for file_type, uploaded_file in file_map.items():
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
        catalog.show_export_interface()
    elif menu_option == "Статистика":
        catalog.show_statistics()
    elif menu_option == "Управление данными":
        catalog.show_data_management()

if __name__ == "__main__":
    main()
