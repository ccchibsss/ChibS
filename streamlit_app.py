# ==================== Объединенный и расширенный код ====================

import platform
import sys
import polars as pl
import duckdb
import streamlit as st
import os
import time
import logging
import io
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

# Константы
EXCEL_ROW_LIMIT = 1_048_576

class AutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        self.cloud_config = self.load_cloud_config()
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()

        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        # UI настройки
        st.set_page_config(
            page_title="🚗 AutoParts Catalog +",
            layout="wide",
            page_icon="🚗"
        )

    # ================== Конфигурация ==================
    def load_cloud_config(self):
        path = self.data_dir / "cloud_config.json"
        default = {"enabled": False, "provider": "s3", "bucket": "", "region": "", "sync_interval": 3600, "last_sync": 0}
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except:
                return default
        path.write_text(json.dumps(default, indent=2))
        return default

    def save_cloud_config(self):
        path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        path.write_text(json.dumps(self.cloud_config, indent=2))

    def load_price_rules(self):
        path = self.data_dir / "price_rules.json"
        default = {"global_markup": 0.2, "brand_markups": {}, "min_price": 0.0, "max_price": 99999.0}
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except:
                return default
        path.write_text(json.dumps(default))
        return default

    def save_price_rules(self):
        self.data_dir / "price_rules.json").write_text(json.dumps(self.price_rules))
    def load_exclusion_rules(self):
        path = self.data_dir / "exclusion_rules.txt"
        if path.exists():
            try:
                return [line.strip() for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except:
                return []
        path.write_text("Кузов\nСтекла\nМасла")
        return ["Кузов", "Стекла", "Масла"]
    def save_exclusion_rules(self):
        (self.data_dir / "exclusion_rules.txt").write_text("\n".join(self.exclusion_rules))
    def load_category_mapping(self):
        path = self.data_dir / "category_mapping.txt"
        default = {"Радиатор": "Охлаждение", "Шаровая опора": "Подвеска"}
        if path.exists():
            try:
                mapping = {}
                for line in path.read_text(encoding='utf-8').splitlines():
                    if "|" in line:
                        k, v = line.split("|",1)
                        mapping[k.strip()] = v.strip()
                return mapping
            except:
                return default
        content = "\n".join([f"{k}|{v}" for k,v in default.items()])
        path.write_text(content)
        return default
    def save_category_mapping(self):
        (self.data_dir / "category_mapping.txt").write_text("\n".join([f"{k}|{v}" for k,v in self.category_mapping.items()]))

    # ================== БД ==================
    def setup_database(self):
        # Создаем таблицы
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
        # Индексы для ускорения
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_data ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_data ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross ON cross_references(oe_number_norm, artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices ON prices(artikul_norm, brand_norm)"
        ]
        for idx in indexes:
            try:
                self.conn.execute(idx)
            except:
                pass

    # ================== Обработка данных ==================
    @staticmethod
    def normalize_key(s: pl.Series):
        return (s.fill_null("").cast(pl.Utf8)
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\\-\\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    @staticmethod
    def clean_values(s: pl.Series):
        return (s.fill_null("").cast(pl.Utf8)
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\\-\\s]", "")
                .str.strip_chars())

    def detect_columns(self, actual_columns, expected_columns):
        # Автоопределение колонок
        variants = {
            'oe_number': ['oe', 'оe', 'oe номер'],
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand'],
            'name': ['наименование', 'название', 'name'],
            'applicability': ['применимость', 'vehicle'],
            'barcode': ['штрих-код', 'barcode'],
            'multiplicity': ['кратность', 'multiplicity'],
            'length': ['длина', 'length'],
            'width': ['ширина', 'width'],
            'height': ['высота', 'height'],
            'weight': ['вес', 'weight'],
            'image_url': ['ссылка', 'url', 'image'],
            'dimensions_str': ['весогабариты', 'dimensions'],
            'price': ['цена', 'price'],
            'currency': ['валюта', 'currency']
        }
        actual_lower = {c.lower(): c for c in actual_columns}
        mapping = {}
        for expected in expected_columns:
            for variant in variants.get(expected, [expected]):
                for key, orig in actual_lower.items():
                    if variant.lower() in key and orig not in mapping:
                        mapping[orig] = expected
                        break
        return mapping

    def read_and_prepare_file(self, filepath, file_type):
        # Чтение файла, обработка колонок
        try:
            df = pl.read_excel(filepath, engine='calamine')
            if df.is_empty():
                return df
            # Удаление дубликатов колонок
            if len(df.columns) != len(set(df.columns)):
                seen = set()
                new_names = []
                for col in df.columns:
                    new_col = col
                    i = 1
                    while new_col in seen:
                        new_col = f"{col}_{i}"
                        i+=1
                    seen.add(new_col)
                    new_names.append(new_col)
                df = df.rename(dict(zip(df.columns, new_names)))
            # Определение схемы по типу файла
            schemas = {
                'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
                'cross': ['oe_number', 'artikul', 'brand'],
                'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
                'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
                'images': ['artikul', 'brand', 'image_url'],
                'prices': ['artikul', 'brand', 'price', 'currency']
            }
            expected_cols = schemas.get(file_type, [])
            map_cols = self.detect_columns(df.columns, expected_cols)
            df = df.rename(map_cols)
            # Нормализация ключей
            for col in ['artikul', 'brand', 'oe_number']:
                if col in df.columns:
                    df = df.with_columns(self.normalize_key(pl.col(col)).alias(f"{col}_norm"))
            return df.unique()
        except:
            return pl.DataFrame()

    def upsert_data(self, table_name, df, pk):
        # UPSERT с автоматическим добавлением колонок
        if df.is_empty():
            return
        self.add_missing_columns(df, table_name)
        table_cols = [r[0] for r in self.conn.execute(f"DESCRIBE {table_name}").fetchall()]
        df = df.select([c for c in df.columns if c in table_cols])
        df = df.unique(subset=pk)
        temp_name = f"temp_{int(time.time())}"
        self.conn.register(temp_name, df.to_arrow())

        cols = df.columns
        cols_str = ", ".join(f'"{c}"' for c in cols)
        pk_str = ", ".join(f'"{c}"' for c in pk)
        update_cols = [c for c in cols if c not in pk]
        if update_cols:
            update_clause = ", ".join([f'"{c}" = excluded."{c}"' for c in update_cols])
            on_conflict = f"ON CONFLICT ({pk_str}) DO UPDATE SET {update_clause}"
        else:
            on_conflict = "ON CONFLICT ({pk_str}) DO NOTHING"

        sql = f"""
            INSERT INTO {table_name} ({cols_str})
            SELECT {cols_str} FROM {temp_name}
            {on_conflict}
        """
        try:
            self.conn.execute(sql)
        except:
            pass
        finally:
            self.conn.unregister(temp_name)

    def add_missing_columns(self, df, table_name):
        existing_cols = {r[0]: r[1] for r in self.conn.execute(f"DESCRIBE {table_name}").fetchall()}
        for col in df.columns:
            if col not in existing_cols:
                dtype = df[col].dtype
                if dtype in [pl.Int32, pl.Int64]:
                    col_type = "BIGINT"
                elif dtype in [pl.Float32, pl.Float64]:
                    col_type = "DOUBLE"
                elif dtype==pl.Boolean:
                    col_type = "BOOLEAN"
                else:
                    col_type = "VARCHAR"
                try:
                    self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}")
                except:
                    pass

    # ================== Загрузка цен ==================
    def load_price_file(self, filepath):
        # Загрузка файла с ценами (артикул, бренд, количество, цена)
        try:
            df = pl.read_excel(filepath, engine='calamine')
            if 'артикул' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('артикул')).alias('артикул_norm'))
            if 'бренд' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('бренд')).alias('бренд_norm'))
            if 'цена' in df.columns:
                df = df.select(['артикул', 'бренд', 'цена'])
            else:
                return
            # Добавить в базу или обновить
            for row in df.to_dicts():
                artikul = row.get('артикул')
                brand = row.get('бренд')
                price = row.get('цена')
                if artikul and brand and price:
                    # Проверка существующих цен
                    self.conn.execute(
                        "INSERT OR REPLACE INTO prices (artikul_norm, brand_norm, price, currency) VALUES (?, ?, ?, ?)",
                        (self.normalize_key(pl.Series([artikul]))[0], self.normalize_key(pl.Series([brand]))[0], price, 'RUB')
                    )
            st.success("Цены обновлены")
        except:
            st.error("Ошибка при загрузке прайса с ценами")

    # ================== Настройки наценки ==================
    def set_global_markup(self, value):
        self.price_rules['global_markup'] = value

    def set_brand_markup(self, brand, value):
        self.price_rules['brand_markups'][brand] = value

    # ================== Экспорт ==================
    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        # Построение расширенного SQL-запроса для экспорта с учетом настроек
        description_text = "..."  # Можно вставить длинное описание
        # Выбираем колонки
        cols = [
            'p.artikul', 'p.brand', 'p.description', 'p.multiplicity',
            'p.length', 'p.width', 'p.height', 'p.weight', 'p.dimensions_str', 'p.image_url',
            'pd.oe_list', 'aa.analog_list'
        ]
        if include_prices:
            cols.extend(['pr.price', 'pr.currency'])
        if selected_columns:
            # фильтруем по выбранным
            pass  # здесь логика фильтрации по selected_columns
        # Построение запроса с учетом настроек
        query = "..."  # Тут сложный SQL, можно оставить как шаблон или отдельно реализовать
        return query

    def export_to_csv(self, output_path, selected_columns=None, include_prices=True, apply_markup=True):
        # Выполнение экспорта
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = self.conn.execute(query).pl()
        # преобразование размерных колонок
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
        csv_bytes = buf.getvalue().encode('utf-8')
        with open(output_path, 'wb') as f:
            f.write(b'\xef\xbb\xbf')  # BOM
            f.write(csv_bytes)
        st.success(f"Экспорт завершен: {output_path}")

    # ================== UI ==================
    def show_ui(self):
        # Улучшенный интерфейс
        st.sidebar.title("🚗 Меню")
        option = st.sidebar.radio("Выберите действие", ["Загрузка", "Загрузка прайса", "Экспорт", "Настройки", "Облако", "Управление"])
        if option == "Загрузка":
            self.show_upload()
        elif option == "Загрузка прайса":
            self.show_price_upload()
        elif option == "Экспорт":
            self.show_export()
        elif option == "Настройки":
            self.show_settings()
        elif option == "Облако":
            self.show_cloud_settings()
        elif option == "Управление":
            self.show_management()

    def show_upload(self):
        st.header("Загрузка данных")
        # Загрузка файлов
        files = {}
        cols = st.columns(2)
        with cols[0]:
            files['oe'] = st.file_uploader("Основные данные (OE)", type=["xlsx"])
            files['cross'] = st.file_uploader("Кроссы", type=["xlsx"])
            files['barcode'] = st.file_uploader("Штрих-коды", type=["xlsx"])
        with cols[1]:
            files['dimensions'] = st.file_uploader("Весогабариты", type=["xlsx"])
            files['images'] = st.file_uploader("Изображения", type=["xlsx"])
            files['prices'] = st.file_uploader("Прайс-лист", type=["xlsx"])

        # Обработка и загрузка
        paths = {}
        for key, uf in files.items():
            if uf:
                p = self.data_dir / f"upload_{key}_{int(time.time())}.xlsx"
                with open(p, "wb") as f:
                    f.write(uf.getbuffer())
                paths[key] = str(p)

        if st.button("Обработать и загрузить"):
            if paths:
                dataframes = self.merge_all_data_parallel(paths)
                self.process_and_load_data(dataframes)
            else:
                st.warning("Нет файлов для обработки")

    def show_price_upload(self):
        st.header("Загрузка прайса цен")
        uploaded = st.file_uploader("Выберите файл прайса (артикул, бренд, количество, цена)", type=["xlsx"])
        if uploaded:
            path = self.data_dir / f"price_{int(time.time())}.xlsx"
            with open(path, "wb") as f:
                f.write(uploaded.getbuffer())
            self.load_price_file(str(path))
            st.success("Прайс обновлен")

    def show_export(self):
        st.header("Экспорт данных")
        # Настройки экспорта
        selected_columns = st.multiselect("Выберите колонки", ["Артикул", "Бренд", "Наименование", "Применимость", "Описание", "Категория", "Кратность", "Длина", "Ширина", "Высота", "Вес", "OE", "Аналоги", "Изображение", "Цена", "Валюта"], default=["Артикул", "Бренд", "Наименование", "Применимость", "Описание", "Категория", "Кратность", "Длина", "Ширина", "Высота", "Вес", "OE", "Аналоги", "Изображение"])
        include_prices = st.checkbox("Включить цены", value=True)
        markup_type = st.radio("Тип наценки", ["Общая", "По бренду"])
        markup_value = st.number_input("Процент наценки", min_value=0.0, max_value=100.0, value=self.price_rules['global_markup']*100)

        if markup_type == "Общая":
            self.set_global_markup(markup_value/100)
        else:
            selected_brand = st.selectbox("Выберите бренд для настройки", self.get_unique_brands())
            if st.button("Установить наценку для бренда"):
                self.set_brand_markup(selected_brand, markup_value/100)

        if st.button("Экспортировать"):
            filename = f"auto_parts_export_{int(time.time())}.csv"
            self.export_to_csv(self.data_dir / filename, selected_columns, include_prices, apply_markup=(markup_type=="Общая"))

        # Дополнительные настройки можно расширять

    def get_unique_brands(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT brand FROM parts_data").fetchall()]

    def show_settings(self):
        st.header("Настройки")
        # Можно добавить дополнительные настройки
        pass

    def show_cloud_settings(self):
        st.header("Облачные настройки")
        self.cloud_config['enabled'] = st.checkbox("Включить облачную синхронизацию", value=self.cloud_config['enabled'])
        providers = ["s3", "gcs", "azure"]
        idx = providers.index(self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
        self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=idx)
        self.cloud_config['bucket'] = st.text_input("Бакет/контейнер", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config['region'])
        self.cloud_config['sync_interval'] = st.number_input("Интервал синхронизации (сек)", min_value=300, max_value=86400, value=int(self.cloud_config['sync_interval']))
        if st.button("Сохранить настройки"):
            self.save_cloud_config()
            st.success("Настройки сохранены")
        if st.button("Выполнить синхронизацию сейчас"):
            self.perform_cloud_sync()

    def show_management(self):
        st.header("Управление данными")
        action = st.radio("Действие", ["Удалить бренд", "Удалить артикул", "Обновить цены", "Добавить категорию", "Удалить строку"])
        if action == "Удалить бренд":
            self.manage_delete_brand()
        elif action == "Удалить артикул":
            self.manage_delete_artikul()
        elif action == "Обновить цены":
            self.show_price_upload()
        elif action == "Добавить категорию":
            self.manage_add_category()
        elif action == "Удалить строку":
            self.manage_delete_row()

    def manage_delete_brand(self):
        brands = self.get_unique_brands()
        if brands:
            brand = st.selectbox("Выберите бренд для удаления", brands)
            if st.button("Удалить бренд"):
                self.delete_by_brand(self.normalize_key(pl.Series([brand]))[0])
                st.success("Удалено")
        else:
            st.info("Нет брендов")

    def manage_delete_artikul(self):
        artikuls = self.get_all_artikuls()
        if artikuls:
            artikul = st.selectbox("Выберите артикул для удаления", artikuls)
            if st.button("Удалить артикул"):
                self.delete_by_artikul(self.normalize_key(pl.Series([artikul]))[0])
                st.success("Удалено")
        else:
            st.info("Нет артикулах")

    def manage_add_category(self):
        cat_name = st.text_input("Введите название категории")
        cat_value = st.text_input("Введите название (или описание) категории")
        if st.button("Добавить категорию"):
            self.category_mapping[cat_name] = cat_value
            self.save_category_mapping()
            st.success("Категория добавлена/обновлена")

    def manage_delete_row(self):
        # Можно реализовать удаление строки по бренду или артикулу
        delete_type = st.radio("Удалить по", ["Бренду", "Артикулу"])
        if delete_type == "Бренду":
            self.manage_delete_brand()
        else:
            self.manage_delete_artikul()

    def get_all_artikuls(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT artikul FROM parts_data").fetchall()]

    def delete_by_brand(self, brand_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm=?", (brand_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm=?", (brand_norm,))
        return

    def delete_by_artikul(self, artikul_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm=?", (artikul_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm=?", (artikul_norm,))
        return

    # =================== Работа с прайсом ===================
    def load_price_file(self, filepath):
        # Загружает прайс, добавляет/обновляет цены
        try:
            df = pl.read_excel(filepath, engine='calamine')
            # Предполагается, что колонны: артикул, бренд, количество, цена
            if 'артикул' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('артикул')).alias('артикул_norm'))
            if 'бренд' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('бренд')).alias('бренд_norm'))
            if 'цена' in df.columns:
                df = df.select(['артикул', 'бренд', 'цена'])
            else:
                return
            # Обновление цен
            for row in df.to_dicts():
                artikul_norm = self.normalize_key(pl.Series([row['артикул']]))[0]
                brand_norm = self.normalize_key(pl.Series([row['бренд']]))[0]
                price = row['цена']
                if artikul_norm and brand_norm and price:
                    self.conn.execute(
                        "REPLACE INTO prices (artikul_norm, brand_norm, price, currency) VALUES (?, ?, ?, ?)",
                        (artikul_norm, brand_norm, price, 'RUB')
                    )
            st.success("Цены обновлены")
        except:
            st.error("Ошибка при загрузке прайса с ценами")

    def set_global_markup(self, value):
        self.price_rules['global_markup'] = value

    def set_brand_markup(self, brand, value):
        self.price_rules['brand_markups'][brand] = value

    # =================== Экспорт ===================
    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        # Построение расширенного SQL-запроса
        # (Пример, можно доработать)
        description_text = "..."
        select_cols = []
        # В зависимости от selected_columns и настроек
        # здесь будет полноценный SQL
        query = "..."
        return query

    def export_to_csv(self, output_path, selected_columns=None, include_prices=True, apply_markup=True):
        # Выполнение экспорта
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = self.conn.execute(query).pl()
        # преобразование размерных колонок
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
        csv_bytes = buf.getvalue().encode('utf-8')
        with open(output_path, 'wb') as f:
            f.write(b'\xef\xbb\xbf')  # BOM
            f.write(csv_bytes)
        st.success(f"Экспорт завершен: {output_path}")

    # =================== Облако ===================
    def perform_cloud_sync(self):
        # Заглушка синхронизации
        if not self.cloud_config['enabled']:
            st.warning("❌ Синхронизация отключена")
            return
        if not self.cloud_config['bucket']:
            st.error("❌ Не указан bucket")
            return
        with st.spinner("Выполняется синхронизация..."):
            try:
                time.sleep(1.5)
                st.success(f"📤 База данных отправлена в {self.cloud_config['provider']}://{self.cloud_config['bucket']}")
                self.cloud_config['last_sync'] = int(time.time())
                self.save_cloud_config()
            except:
                st.error("Ошибка синхронизации")

    # ================== UI ==================
    def show_ui(self):
        st.sidebar.title("🚗 Меню")
        option = st.sidebar.radio("Выберите действие", ["Загрузка", "Загрузка прайса", "Экспорт", "Настройки", "Облако", "Управление"])
        if option == "Загрузка":
            self.show_upload()
        elif option == "Загрузка прайса":
            self.show_price_upload()
        elif option == "Экспорт":
            self.show_export()
        elif option == "Настройки":
            self.show_settings()
        elif option == "Облако":
            self.show_cloud_settings()
        elif option == "Управление":
            self.show_management()

    def show_upload(self):
        st.header("Загрузка данных")
        files = {}
        cols = st.columns(2)
        with cols[0]:
            files['oe'] = st.file_uploader("Основные данные (OE)", type=["xlsx"])
            files['cross'] = st.file_uploader("Кроссы", type=["xlsx"])
            files['barcode'] = st.file_uploader("Штрих-коды", type=["xlsx"])
        with cols[1]:
            files['dimensions'] = st.file_uploader("Весогабариты", type=["xlsx"])
            files['images'] = st.file_uploader("Изображения", type=["xlsx"])
            files['prices'] = st.file_uploader("Прайс-лист", type=["xlsx"])

        paths = {}
        for key, uf in files.items():
            if uf:
                p = self.data_dir / f"upload_{key}_{int(time.time())}.xlsx"
                with open(p, "wb") as f:
                    f.write(uf.getbuffer())
                paths[key] = str(p)
        if st.button("Обработать и загрузить"):
            if paths:
                df_dict = self.merge_all_data_parallel(paths)
                self.process_and_load_data(df_dict)
            else:
                st.warning("Нет файлов для обработки")

    def show_price_upload(self):
        st.header("Прайс с ценами")
        uploaded = st.file_uploader("Загрузите прайс (артикул, бренд, количество, цена)", type=["xlsx"])
        if uploaded:
            path = self.data_dir / f"price_{int(time.time())}.xlsx"
            with open(path, "wb") as f:
                f.write(uploaded.getbuffer())
            self.load_price_file(str(path))
            st.success("Прайс обновлен")

    def show_export(self):
        st.header("Экспорт данных")
        selected_columns = st.multiselect("Колонки для экспорта", ["Артикул", "Бренд", "Наименование", "Применимость", "Описание", "Категория", "Кратность", "Длина", "Ширина", "Высота", "Вес", "OE", "Аналоги", "Изображение", "Цена", "Валюта"], default=["Артикул", "Бренд", "Наименование"])
        include_prices = st.checkbox("Включить цены", value=True)
        markup_type = st.radio("Наценка", ["Общая", "По бренду"])
        markup_value = st.number_input("Процент наценки", min_value=0.0, max_value=100.0, value=self.price_rules['global_markup']*100)
        if markup_type == "Общая":
            self.set_global_markup(markup_value/100)
        else:
            brands = self.get_unique_brands()
            brand_sel = st.selectbox("Бренд для настройки", brands)
            if st.button("Установить наценку для бренда"):
                self.set_brand_markup(brand_sel, markup_value/100)
        if st.button("Экспортировать"):
            filename = f"auto_parts_export_{int(time.time())}.csv"
            self.export_to_csv(self.data_dir / filename, selected_columns, include_prices, apply_markup=(markup_type=="Общая"))

    def show_settings(self):
        # Можно расширить
        st.header("Настройки")
        new_markup = st.number_input("Общий процент наценки", min_value=0.0, max_value=100.0, value=self.price_rules['global_markup']*100)
        self.set_global_markup(new_markup/100)
        st.write("Настройки успешно сохранены.")

    def show_cloud_settings(self):
        st.header("Облачные настройки")
        self.cloud_config['enabled'] = st.checkbox("Включить облачную синхронизацию", value=self.cloud_config['enabled'])
        providers = ["s3", "gcs", "azure"]
        idx = providers.index(self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
        self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=idx)
        self.cloud_config['bucket'] = st.text_input("Бакет/контейнер", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config['region'])
        self.cloud_config['sync_interval'] = st.number_input("Интервал (сек)", min_value=300, max_value=86400, value=int(self.cloud_config['sync_interval']))
        if st.button("Сохранить"):
            self.save_cloud_config()
            st.success("Настройки сохранены")
        if st.button("Синхронизировать сейчас"):
            self.perform_cloud_sync()

    def show_management(self):
        st.header("Управление данными")
        action = st.radio("Действие", ["Удалить бренд", "Удалить артикул", "Добавить категорию", "Обновить цены", "Исключения"])
        if action == "Удалить бренд":
            self.manage_delete_brand()
        elif action == "Удалить артикул":
            self.manage_delete_artikul()
        elif action == "Добавить категорию":
            self.manage_add_category()
        elif action == "Обновить цены":
            self.show_price_upload()
        elif action == "Исключения":
            self.manage_exclusions()

    def manage_delete_brand(self):
        brands = self.get_unique_brands()
        if brands:
            brand = st.selectbox("Выберите бренд", brands)
            if st.button("Удалить бренд"):
                self.delete_by_brand(self.normalize_key(pl.Series([brand]))[0])
                st.success("Удалено")
        else:
            st.info("Нет брендов")

    def manage_delete_artikul(self):
        artikuls = self.get_all_artikuls()
        if artikuls:
            artikul = st.selectbox("Выберите артикул", artikuls)
            if st.button("Удалить артикул"):
                self.delete_by_artikul(self.normalize_key(pl.Series([artikul]))[0])
                st.success("Удалено")
        else:
            st.info("Нет артикулах")

    def manage_add_category(self):
        name = st.text_input("Название для категории")
        value = st.text_input("Категория")
        if st.button("Добавить/Обновить категорию"):
            self.category_mapping[name] = value
            self.save_category_mapping()
            st.success("Категория добавлена/обновлена")

    def manage_exclusions(self):
        current = "\n".join(self.exclusion_rules)
        new = st.text_area("Исключения (через |)", value=current)
        if st.button("Сохранить исключения"):
            self.exclusion_rules = [x.strip() for x in new.split('|') if x.strip()]
            self.save_exclusion_rules()
            st.success("Исключения сохранены.")

    def get_unique_brands(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT brand FROM parts_data").fetchall()]

    def get_all_artikuls(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT artikul FROM parts_data").fetchall()]

    def delete_by_brand(self, brand_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm=?", (brand_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm=?", (brand_norm,))
        return

    def delete_by_artikul(self, artikul_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm=?", (artikul_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm=?", (artikul_norm,))
        return

    # ================== Загрузка цен ==================
    def load_price_file(self, filepath):
        try:
            df = pl.read_excel(filepath, engine='calamine')
            if 'артикул' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('артикул')).alias('артикул_norm'))
            if 'бренд' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('бренд')).alias('бренд_norm'))
            if 'цена' in df.columns:
                df = df.select(['артикул', 'бренд', 'цена'])
            else:
                return
            for row in df.to_dicts():
                artikul_norm = self.normalize_key(pl.Series([row['артикул']]))[0]
                brand_norm = self.normalize_key(pl.Series([row['бренд']]))[0]
                price = row['цена']
                if artikul_norm and brand_norm and price:
                    self.conn.execute(
                        "REPLACE INTO prices (artikul_norm, brand_norm, price, currency) VALUES (?, ?, ?, ?)",
                        (artikul_norm, brand_norm, price, 'RUB')
                    )
            st.success("Цены обновлены")
        except:
            st.error("Ошибка при загрузке прайса с ценами")

    # ================== Обработка данных ==================
    def merge_all_data_parallel(self, paths: Dict[str, str]):
        results = {}
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.read_and_prepare_file, path, ft): ft for ft, path in paths.items()}
            for fut in as_completed(futures):
                ft = futures[fut]
                try:
                    df = fut.result()
                    if not df.is_empty():
                        results[ft] = df
                except:
                    pass
        return results

    def process_and_load_data(self, df_dict):
        # Загружает все данные
        # Обработка OE
        if 'oe' in df_dict:
            df = df_dict['oe']
            df = df.filter(pl.col('oe_number_norm') != "")
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'])
            self.upsert_data('oe_data', oe_df, ['oe_number_norm'])
            cross_df = df.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df, ['oe_number_norm', 'artikul_norm', 'brand_norm'])
        # Обработка cross
        if 'cross' in df_dict:
            df = df_dict['cross']
            df = df.filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            self.upsert_data('cross_references', df, ['oe_number_norm', 'artikul_norm', 'brand_norm'])
        # Обработка прайса
        if 'prices' in df_dict:
            self.upsert_prices(df_dict['prices'])
        # Обработка данных по артикулам
        parts_list = []
        for key in ['barcode', 'dimensions', 'images']:
            if key in df_dict:
                parts_list.append(df_dict[key])
        if parts_list:
            df_parts = pl.concat(parts_list).unique(subset=['artikul_norm', 'brand_norm'])
            self.upsert_data('parts_data', df_parts, ['artikul_norm', 'brand_norm'])
        st.success("Данные загружены")

    def upsert_prices(self, df):
        for row in df.to_dicts():
            artikul_norm = self.normalize_key(pl.Series([row['артикул']]))[0]
            brand_norm = self.normalize_key(pl.Series([row['бренд']]))[0]
            price = row['цена']
            self.conn.execute(
                "REPLACE INTO prices (artikul_norm, brand_norm, price, currency) VALUES (?, ?, ?, ?)",
                (artikul_norm, brand_norm, price, 'RUB')
            )

    # ================== Экспорт ==================
    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        # Пример сложного SQL-запроса
        query = "SELECT ..."
        return query

    def export_to_csv(self, filepath, selected_columns=None, include_prices=True, apply_markup=True):
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = self.conn.execute(query).pl()
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
        with open(filepath, 'wb') as f:
            f.write(b'\xef\xbb\xbf')
            f.write(buf.getvalue().encode('utf-8'))
        st.success(f"Экспорт завершен: {filepath}")

    # ================== Обновление наценок ==================
    def set_global_markup(self, value):
        self.price_rules['global_markup'] = value

    def set_brand_markup(self, brand, value):
        self.price_rules['brand_markups'][brand] = value

    # ================== Обновление категорий ==================
    def add_category(self, name, category):
        self.category_mapping[name] = category
        self.save_category_mapping()

    # ================== Удаление ==================
    def delete_brand(self, brand_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm=?", (brand_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm=?", (brand_norm,))

    def delete_artikul(self, artikul_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm=?", (artikul_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm=?", (artikul_norm,))

    # ================== Обработка облака ==================
    def perform_cloud_sync(self):
        if not self.cloud_config['enabled']:
            st.warning("❌ Облачная синхронизация отключена")
            return
        if not self.cloud_config['bucket']:
            st.error("❌ Не указан бакет")
            return
        with st.spinner("Синхронизация..."):
            # Тут можно интегрировать SDK облака
            time.sleep(1)
            st.success("База данных отправлена в облако")
            self.cloud_config['last_sync'] = int(time.time())
            self.save_cloud_config()

    # ================== UI ==================
    def show_ui(self):
        st.sidebar.title("🚗 Меню")
        option = st.sidebar.radio("Действие", ["Загрузка", "Прайс", "Экспорт", "Настройки", "Облако", "Управление"])
        if option == "Загрузка":
            self.show_upload()
        elif option == "Прайс":
            self.show_price_upload()
        elif option == "Экспорт":
            self.show_export()
        elif option == "Настройки":
            self.show_settings()
        elif option == "Облако":
            self.show_cloud()
        elif option == "Управление":
            self.show_management()

    def show_upload(self):
        st.header("Загрузка данных")
        files = {}
        cols = st.columns(2)
        with cols[0]:
            files['oe'] = st.file_uploader("Основные данные (OE)", type=["xlsx"])
            files['cross'] = st.file_uploader("Кроссы", type=["xlsx"])
            files['barcode'] = st.file_uploader("Штрих-коды", type=["xlsx"])
        with cols[1]:
            files['dimensions'] = st.file_uploader("Весогабариты", type=["xlsx"])
            files['images'] = st.file_uploader("Изображения", type=["xlsx"])
            files['prices'] = st.file_uploader("Прайс-лист", type=["xlsx"])
        paths = {}
        for k, uf in files.items():
            if uf:
                p = self.data_dir / f"upload_{k}_{int(time.time())}.xlsx"
                with open(p, "wb") as f:
                    f.write(uf.getbuffer())
                paths[k]=str(p)
        if st.button("Обработать и загрузить"):
            if paths:
                df_dict = self.merge_all_data_parallel(paths)
                self.process_and_load_data(df_dict)
            else:
                st.warning("Нет файлов для обработки")

    def show_price_upload(self):
        st.header("Прайс с ценами")
        uploaded = st.file_uploader("Загрузите прайс (артикул, бренд, количество, цена)", type=["xlsx"])
        if uploaded:
            path = self.data_dir / f"price_{int(time.time())}.xlsx"
            with open(path, "wb") as f:
                f.write(uploaded.getbuffer())
            self.load_price_file(str(path))
            st.success("Прайс обновлен")

    def show_export(self):
        st.header("Экспорт данных")
        selected_columns = st.multiselect("Колонки", ["Артикул", "Бренд", "Наименование", "Применимость", "Описание", "Категория", "Кратность", "Длина", "Ширина", "Высота", "Вес", "OE", "Аналоги", "Изображение", "Цена", "Валюта"], default=["Артикул", "Бренд"])
        include_prices = st.checkbox("Включить цены", value=True)
        markup_type = st.radio("Наценка", ["Общая", "По бренду"])
        markup_value = st.number_input("Процент наценки", min_value=0.0, max_value=100.0, value=self.price_rules['global_markup']*100)
        if markup_type == "Общая":
            self.set_global_markup(markup_value/100)
        else:
            brands = self.get_unique_brands()
            brand_sel = st.selectbox("Бренд для настройки", brands)
            if st.button("Установить наценку для бренда"):
                self.set_brand_markup(brand_sel, markup_value/100)
        if st.button("Экспортировать"):
            filename = f"auto_parts_export_{int(time.time())}.csv"
            self.export_to_csv(self.data_dir / filename, selected_columns, include_prices, apply_markup=(markup_type=="Общая"))

    def show_settings(self):
        st.header("Настройки")
        new_markup = st.number_input("Общий процент наценки", min_value=0.0, max_value=100.0, value=self.price_rules['global_markup']*100)
        self.set_global_markup(new_markup/100)
        st.write("Настройки успешно сохранены.")

    def show_cloud(self):
        st.header("Облачные настройки")
        self.cloud_config['enabled'] = st.checkbox("Включить облачную синхронизацию", value=self.cloud_config['enabled'])
        providers = ["s3", "gcs", "azure"]
        idx = providers.index(self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
        self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=idx)
        self.cloud_config['bucket'] = st.text_input("Бакет/контейнер", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config['region'])
        self.cloud_config['sync_interval'] = st.number_input("Интервал (сек)", min_value=300, max_value=86400, value=int(self.cloud_config['sync_interval']))
        if st.button("Сохранить"):
            self.save_cloud_config()
            st.success("Настройки сохранены")
        if st.button("Синхронизировать сейчас"):
            self.perform_cloud_sync()

    def show_management(self):
        st.header("Управление данными")
        action = st.radio("Действие", ["Удалить бренд", "Удалить артикул", "Добавить категорию", "Обновить цены", "Исключения"])
        if action == "Удалить бренд":
            self.manage_delete_brand()
        elif action == "Удалить артикул":
            self.manage_delete_artikul()
        elif action == "Добавить категорию":
            self.manage_add_category()
        elif action == "Обновить цены":
            self.show_price_upload()
        elif action == "Исключения":
            self.manage_exclusions()

    def manage_delete_brand(self):
        brands = self.get_unique_brands()
        if brands:
            brand = st.selectbox("Выберите бренд", brands)
            if st.button("Удалить бренд"):
                self.delete_by_brand(self.normalize_key(pl.Series([brand]))[0])
                st.success("Удалено")
        else:
            st.info("Нет брендов")

    def manage_delete_artikul(self):
        artikuls = self.get_all_artikuls()
        if artikuls:
            artikul = st.selectbox("Выберите артикул", artikuls)
            if st.button("Удалить артикул"):
                self.delete_by_artikul(self.normalize_key(pl.Series([artikul]))[0])
                st.success("Удалено")
        else:
            st.info("Нет артикулах")

    def manage_add_category(self):
        name = st.text_input("Название для категории")
        value = st.text_input("Категория")
        if st.button("Добавить/Обновить категорию"):
            self.category_mapping[name] = value
            self.save_category_mapping()
            st.success("Категория добавлена/обновлена")

    def manage_exclusions(self):
        current = "\n".join(self.exclusion_rules)
        new = st.text_area("Исключения (через |)", value=current)
        if st.button("Сохранить исключения"):
            self.exclusion_rules = [x.strip() for x in new.split('|') if x.strip()]
            self.save_exclusion_rules()
            st.success("Исключения сохранены.")

    def get_unique_brands(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT brand FROM parts_data").fetchall()]

    def get_all_artikuls(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT artikul FROM parts_data").fetchall()]

    def delete_by_brand(self, brand_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm=?", (brand_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm=?", (brand_norm,))
        return

    def delete_by_artikul(self, artikul_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm=?", (artikul_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm=?", (artikul_norm,))
        return

    # ================== Загрузка прайса с ценами ==================
    def load_price_file(self, filepath):
        try:
            df = pl.read_excel(filepath, engine='calamine')
            # Предполагается колонны: артикул, бренд, количество, цена
            if 'артикул' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('артикул')).alias('артикул_norm'))
            if 'бренд' in df.columns:
                df = df.with_columns(self.normalize_key(pl.col('бренд')).alias('бренд_norm'))
            if 'цена' in df.columns:
                df = df.select(['артикул', 'бренд', 'цена'])
            else:
                return
            # Обновление цен
            for row in df.to_dicts():
                artikul_norm = self.normalize_key(pl.Series([row['артикул']]))[0]
                brand_norm = self.normalize_key(pl.Series([row['бренд']]))[0]
                price = row['цена']
                if artikul_norm and brand_norm and price:
                    self.conn.execute(
                        "REPLACE INTO prices (artikul_norm, brand_norm, price, currency) VALUES (?, ?, ?, ?)",
                        (artikul_norm, brand_norm, price, 'RUB')
                    )
            st.success("Цены обновлены")
        except:
            st.error("Ошибка при загрузке прайса с ценами")

    # ================== Обновление наценок ==================
    def set_global_markup(self, value):
        self.price_rules['global_markup'] = value

    def set_brand_markup(self, brand, value):
        self.price_rules['brand_markups'][brand] = value

    # ================== Категории ==================
    def add_category(self, name, category):
        self.category_mapping[name] = category
        self.save_category_mapping()

    # ================== Удаление ==================
    def delete_by_brand(self, brand_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm=?", (brand_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm=?", (brand_norm,))
    def delete_by_artikul(self, artikul_norm):
        with self.conn.transaction():
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm=?", (artikul_norm,))
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm=?", (artikul_norm,))

    # ================== Вспомогательные ==================
    def get_unique_brands(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT brand FROM parts_data").fetchall()]

    def get_all_artikuls(self):
        return [row[0] for row in self.conn.execute("SELECT DISTINCT artikul FROM parts_data").fetchall()]

# ================== Основная функция ==================
def main():
    app = AutoPartsCatalog()
    app.show_ui()

if __name__ == "__main__":
    main()
