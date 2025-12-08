import polars as pl
import duckdb
import streamlit as st
import os
import time
import json
import warnings
from pathlib import Path
import io

warnings.filterwarnings('ignore')

EXCEL_ROW_LIMIT = 1_000_000  # Максимальное количество строк для Excel

class AutoPartsCatalog:
    def __init__(self):
        # Папка для хранения данных
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        # Путь к базе данных
        self.db_path = self.data_dir / "catalog.duckdb"
        # Подключение к базе данных
        self.conn = duckdb.connect(str(self.db_path))
        # Инициализация таблиц
        self.setup_database()
        # Загрузки настроек
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        # Стримлит интерфейс
        st.set_page_config(page_title="AutoParts Catalog 10M+", layout='wide', page_icon='🚗')

    def setup_database(self):
        """
        Создание таблиц, если еще не созданы.
        """
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
        """
        Создание индексов для быстрого поиска.
        """
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_oe_data_oe ON oe_data(oe_number_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_parts_data_keys ON parts_data(artikul_norm, brand_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)")

    def load_cloud_config(self):
        """
        Загрузка настроек облака.
        """
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
            except:
                return default
        else:
            path.write_text(json.dumps(default, indent=2, ensure_ascii=False))
            return default

    def save_cloud_config(self):
        """
        Сохранение настроек облака.
        """
        path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        path.write_text(json.dumps(self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_price_rules(self):
        """
        Загрузка правил ценообразования.
        """
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
            except:
                return default
        else:
            path.write_text(json.dumps(default, indent=2, ensure_ascii=False))
            return default

    def save_price_rules(self):
        """
        Сохранение правил ценообразования.
        """
        path = self.data_dir / "price_rules.json"
        path.write_text(json.dumps(self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_exclusion_rules(self):
        """
        Загрузка списка исключений.
        """
        path = self.data_dir / "exclusion_rules.txt"
        if path.exists():
            try:
                return [line.strip() for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except:
                return ["Кузов", "Стекла", "Масла"]
        else:
            path.write_text("Кузов\nСтекла\nМасла", encoding='utf-8')
            return ["Кузов", "Стекла", "Масла"]

    def save_exclusion_rules(self):
        """
        Сохранение списка исключений.
        """
        path = self.data_dir / "exclusion_rules.txt"
        path.write_text("\n".join(self.exclusion_rules), encoding='utf-8')

    def load_category_mapping(self):
        """
        Загрузка отображения категорий.
        """
        path = self.data_dir / "category_mapping.txt"
        default = {
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        }
        if path.exists():
            try:
                mapping = {}
                for line in path.read_text(encoding='utf-8').splitlines():
                    if '|' in line:
                        k, v = line.split('|', 1)
                        mapping[k.strip()] = v.strip()
                return mapping
            except:
                return default
        else:
            path.write_text("\n".join([f"{k}|{v}" for k, v in default.items()]), encoding='utf-8')
            return default

    def save_category_mapping(self):
        """
        Сохранение отображения категорий.
        """
        path = self.data_dir / "category_mapping.txt"
        path.write_text("\n".join([f"{k}|{v}" for k, v in self.category_mapping.items()]), encoding='utf-8')

    def get_total_records(self):
        """
        Получение количества уникальных позиций.
        """
        res = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)").fetchone()
        return res[0] if res else 0

    def get_export_query(self, selected_columns=None):
        """
        Формирует SQL-запрос для экспорта.
        """
        # Можно расширить для выбора колонок
        # В этом примере возвращается полный набор
        return """
        SELECT
            a.artikul AS "Артикул",
            a.brand AS "Бренд",
            a.name AS "Наименование",
            a.applicability AS "Применимость",
            a.description AS "Описание",
            a.category AS "Категория товара",
            a.multiplicity AS "Кратность",
            a.length AS "Длинна",
            a.width AS "Ширина",
            a.height AS "Высота",
            a.weight AS "Вес",
            CONCAT_WS("/", a.length, a.width, a.height) AS "Длинна/Ширина/Высота",
            o.oe_number AS "OE номер",
            "" AS "аналоги",
            a.image_url AS "Ссылка на изображение"
        FROM parts_data a
        LEFT JOIN oe_data o ON a.artikul_norm = o.oe_number_norm
        """

    # --- Методы для обработки данных --- #

    def read_and_prepare_file(self, file_path, file_type):
        """
        Загружает Excel файл и возвращает DataFrame.
        """
        # Чтение файла
        df = pl.read_excel(file_path)

        # Проверка наличия обязательных колонок
        # Можно добавить проверки
        return df

    def merge_all_data_parallel(self, file_paths):
        """
        Объединяет все файлы в DataFrames.
        """
        dataframes = {}

        # Обработка файла OE
        if 'oe' in file_paths:
            df_oe = self.read_and_prepare_file(file_paths['oe'])
            dataframes['oe'] = df_oe
        else:
            dataframes['oe'] = pl.DataFrame()

        # Обработка файла cross
        if 'cross' in file_paths:
            df_cross = self.read_and_prepare_file(file_paths['cross'])
            dataframes['cross'] = df_cross
        else:
            dataframes['cross'] = pl.DataFrame()

        # Аналогично для остальных файлов
        if 'barcode' in file_paths:
            df_barcode = self.read_and_prepare_file(file_paths['barcode'])
            dataframes['barcode'] = df_barcode
        else:
            dataframes['barcode'] = pl.DataFrame()

        if 'dimensions' in file_paths:
            df_dimensions = self.read_and_prepare_file(file_paths['dimensions'])
            dataframes['dimensions'] = df_dimensions
        else:
            dataframes['dimensions'] = pl.DataFrame()

        if 'images' in file_paths:
            df_images = self.read_and_prepare_file(file_paths['images'])
            dataframes['images'] = df_images
        else:
            dataframes['images'] = pl.DataFrame()

        if 'prices' in file_paths:
            df_prices = self.read_and_prepare_file(file_paths['prices'])
            dataframes['prices'] = df_prices
        else:
            dataframes['prices'] = pl.DataFrame()

        return dataframes

    def process_and_load_data(self, dataframes):
        """
        Обработка и загрузка данных из DataFrames.
        """
        # Обработка oe_data
        df_oe = dataframes.get('oe', pl.DataFrame())
        if not df_oe.is_empty():
            # Предположим, что df_oe содержит колонки: 'OE номер', 'Наименование', 'Применимость', 'Категория'
            df_oe = df_oe.with_columns([
                self.normalize_key(pl.col('OE номер')).alias('oe_number_norm'),
                pl.col('OE номер').alias('oe_number'),
                pl.col('Наименование').alias('name'),
                pl.col('Применимость').alias('applicability'),
                pl.col('Категория').alias('category')
            ])
            self.upsert_data('oe_data', df_oe.select([
                'oe_number_norm', 'oe_number', 'name', 'applicability', 'category'
            ]), ['oe_number_norm'])

        # Обработка cross_references
        df_cross = dataframes.get('cross', pl.DataFrame())
        if not df_cross.is_empty():
            # Предположим, что содержит 'OE номер', 'Артикул', 'Бренд'
            df_cross = df_cross.with_columns([
                self.normalize_key(pl.col('OE номер')).alias('oe_number_norm'),
                self.normalize_key(pl.col('Артикул')).alias('artikul_norm'),
                pl.col('Артикул').alias('artikul'),
                pl.col('Бренд').alias('brand')
            ])
            self.upsert_data('cross_references', df_cross.select([
                'oe_number_norm', 'artikul_norm', 'brand'
            ]), ['oe_number_norm', 'artikul_norm', 'brand'])

        # Обработка parts_data
        df_parts = dataframes.get('barcode', pl.DataFrame())
        if not df_parts.is_empty():
            # Предположим, что содержит 'Артикул', 'Бренд', 'Кратность', 'Штрихкод', 'Длинна', 'Ширина', 'Высота', 'Вес', 'Изображение', 'Описание', 'Категория'
            df_parts = df_parts.with_columns([
                self.normalize_key(pl.col('Артикул')).alias('artikul_norm'),
                self.normalize_key(pl.col('Бренд')).alias('brand_norm'),
                pl.col('Артикул').alias('artikul'),
                pl.col('Бренд').alias('brand'),
                pl.col('Кратность').cast(pl.Int32).fill_null(1),
                pl.col('Штрихкод').fill_null(""),
                pl.col('Длинна').cast(pl.Float64).fill_null(0.0),
                pl.col('Ширина').cast(pl.Float64).fill_null(0.0),
                pl.col('Высота').cast(pl.Float64).fill_null(0.0),
                pl.col('Вес').cast(pl.Float64).fill_null(0.0),
                pl.col('Изображение').fill_null(""),
                pl.col('Описание').fill_null(""),
                pl.col('Категория').fill_null("")
            ])
            # Создаем строку размеров
            df_parts = df_parts.with_columns(
                (pl.col('Длинна').cast(pl.Utf8) + "/" + pl.col('Ширина').cast(pl.Utf8) + "/" + pl.col('Высота').cast(pl.Utf8)).alias('dimensions_str')
            )
            self.upsert_data('parts_data', df_parts.select([
                'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode', 'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
            ]), ['artikul_norm', 'brand_norm'])

        # Обработка изображений
        df_images = dataframes.get('images', pl.DataFrame())
        if not df_images.is_empty():
            # Предположим, содержит 'Артикул', 'Бренд', 'Ссылка на изображение'
            df_images = df_images.with_columns([
                self.normalize_key(pl.col('Артикул')).alias('artikul_norm'),
                self.normalize_key(pl.col('Бренд')).alias('brand_norm'),
                pl.col('Ссылка на изображение').alias('image_url')
            ])
            # Обновляем таблицу parts_data с ссылками
            for row in df_images.iter_rows():
                self.conn.execute("""
                UPDATE parts_data SET image_url = ?
                WHERE artikul_norm = ? AND brand_norm = ?
                """, [row['image_url'], row['artikul_norm'], row['brand_norm']])

        # Обработка цен
        df_prices = dataframes.get('prices', pl.DataFrame())
        if not df_prices.is_empty():
            # Предположим, содержит 'Артикул', 'Бренд', 'Цена'
            df_prices = df_prices.with_columns([
                self.normalize_key(pl.col('Артикул')).alias('artikul_norm'),
                self.normalize_key(pl.col('Бренд')).alias('brand_norm'),
                pl.col('Цена').cast(pl.Float64).fill_null(0.0)
            ])
            for row in df_prices.iter_rows():
                self.conn.execute("""
                INSERT INTO prices (artikul_norm, brand_norm, price) VALUES (?, ?, ?)
                ON CONFLICT (artikul_norm, brand_norm) DO UPDATE SET price=excluded.price
                """, [row['artikul_norm'], row['brand_norm'], row['Цена']])

        # Обработка весогабаритных данных
        df_dim = dataframes.get('dimensions', pl.DataFrame())
        if not df_dim.is_empty():
            # Аналогично, предполагается, что содержит артикул, бренд и размеры
            # В этом примере пропускаем
            pass

    def export_to_csv(self, output_path, selected_columns=None):
        """
        Экспорт данных в CSV.
        """
        query = self.get_export_query(selected_columns)
        df = self.conn.execute(query).pl()

        # Обработка размеров
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
        csv_str = buf.getvalue()
        with open(output_path, "wb") as f:
            f.write(b'\xef\xbb\xbf')  # BOM для Excel
            f.write(csv_str.encode('utf-8'))

    def export_to_excel(self, output_path, selected_columns=None):
        """
        Экспорт в Excel, разбивая по чанкам.
        """
        query = self.get_export_query(selected_columns)
        df = self.conn.execute(query).pl()
        total_rows = len(df)
        chunks = []
        start = 0
        while start < total_rows:
            end = min(start + EXCEL_ROW_LIMIT, total_rows)
            chunks.append(df[start:end])
            start = end
        import pandas as pd
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            for idx, chunk in enumerate(chunks):
                chunk.to_pandas().to_excel(writer, sheet_name=f"Part_{idx+1}", index=False)

    def export_to_parquet(self, output_path, selected_columns=None):
        """
        Экспорт в Parquet.
        """
        query = self.get_export_query(selected_columns)
        df = self.conn.execute(query).pl()
        df.write_parquet(output_path)

    def show_export_interface(self):
        """
        Интерфейс для экспорта.
        """
        st.header("📤 Экспорт данных")
        total = self.get_total_records()
        st.info(f"Всего записей для экспорта: {total:,}")
        if total == 0:
            st.warning("Нет данных для экспорта")
            return
        options = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание", "Категория товара",
            "Кратность", "Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        selected = st.multiselect("Выберите колонки для экспорта (пусто = все)", options=options)
        export_format = st.radio("Формат экспорта", ["CSV", "Excel (.xlsx)", "Parquet"])
        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"auto_parts_export.{export_format.lower().replace(' ', '_')}"
            with st.spinner("Генерация файла..."):
                try:
                    if export_format == "CSV":
                        self.export_to_csv(str(output_path), selected if selected else None)
                    elif export_format == "Excel (.xlsx)":
                        self.export_to_excel(str(output_path), selected if selected else None)
                    elif export_format == "Parquet":
                        self.export_to_parquet(str(output_path), selected if selected else None)
                    st.success(f"Файл успешно сохранен: {output_path}")
                    with open(output_path, "rb") as f:
                        st.download_button("⬇️ Скачать файл", f, output_path.name)
                except Exception as e:
                    st.error(f"Ошибка при экспорте: {e}")

# --- Основной запуск --- #
def main():
    catalog = AutoPartsCatalog()

    st.title("🚗 AutoParts Catalog — Масштабируемая система для 10+ млн записей")
    st.sidebar.title("🧭 Навигация")
    menu = st.sidebar.radio("Выберите раздел:", ["Загрузка данных", "Экспорт", "Статистика", "Управление"])

    if menu == "Загрузка данных":
        catalog.show_data_upload()
    elif menu == "Экспорт":
        catalog.show_export_interface()
    elif menu == "Статистика":
        catalog.show_statistics()
    elif menu == "Управление":
        # Можно добавить управление ценами, исключениями, категориями и облаком
        st.header("🛠️ Управление системой")
        if st.checkbox("Настройки цен"):
            catalog.show_price_settings()
        if st.checkbox("Настройки исключений"):
            catalog.show_exclusion_settings()
        if st.checkbox("Настройки категорий"):
            catalog.show_category_mapping()
        if st.checkbox("Настройки облака"):
            catalog.show_cloud_sync()

if __name__ == "__main__":
    main()
