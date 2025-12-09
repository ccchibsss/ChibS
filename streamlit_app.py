# ==============================================================================
# 🚗 AutoParts Catalog — Полнофункциональная система с поиском и удалением
# Версия: 1.4
# Поддержка: 10M+ записей, динамические столбцы, наценки >100%, удаление
# ==============================================================================

import polars as pl
import duckdb
import streamlit as st
import os
import time
import logging
import json
from pathlib import Path
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_ROW_LIMIT = 1_048_576


class HighVolumeAutoPartsCatalog:
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

        st.set_page_config(
            page_title="🚗 AutoParts Catalog",
            layout="wide",
            page_icon="🚗",
            initial_sidebar_state="expanded"
        )

    # === 🛠️ ЗАГРУЗКА КОНФИГУРАЦИЙ ===
    def load_cloud_config(self) -> Dict[str, Any]:
        path = self.data_dir / "cloud_config.json"
        default = {"enabled": False, "provider": "s3", "bucket": "", "region": "", "sync_interval": 3600, "last_sync": 0}
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                return default
        path.write_text(json.dumps(default, indent=2, ensure_ascii=False), encoding='utf-8')
        return default

    def save_cloud_config(self):
        path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        path.write_text(json.dumps(self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_price_rules(self) -> Dict[str, Any]:
        path = self.data_dir / "price_rules.json"
        default = {"global_markup": 2.0, "brand_markups": {}, "min_price": 0.0, "max_price": 99999.0}
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                return default
        path.write_text(json.dumps(default, indent=2, ensure_ascii=False), encoding='utf-8')
        return default

    def save_price_rules(self):
        path = self.data_dir / "price_rules.json"
        path.write_text(json.dumps(self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_exclusion_rules(self) -> List[str]:
        path = self.data_dir / "exclusion_rules.txt"
        if path.exists():
            try:
                return [line.strip() for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                return []
        path.write_text("Кузов\nСтекла\nМасла", encoding='utf-8')
        return ["Кузов", "Стекла", "Масла"]

    def save_exclusion_rules(self):
        path = self.data_dir / "exclusion_rules.txt"
        path.write_text("\n".join(self.exclusion_rules), encoding='utf-8')

    def load_category_mapping(self) -> Dict[str, str]:
        path = self.data_dir / "category_mapping.txt"
        default = {"Радиатор": "Охлаждение", "Шаровая опора": "Подвеска"}
        if path.exists():
            try:
                mapping = {}
                for line in path.read_text(encoding='utf-8').splitlines():
                    if "|" in line:
                        k, v = line.split("|", 1)
                        mapping[k.strip()] = v.strip()
                return mapping
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                return default
        content = "\n".join(f"{k}|{v}" for k, v in default.items())
        path.write_text(content, encoding='utf-8')
        return default

    def save_category_mapping(self):
        path = self.data_dir / "category_mapping.txt"
        content = "\n".join(f"{k}|{v}" for k, v in self.category_mapping.items())
        path.write_text(content, encoding='utf-8')

    # === 🗃️ УПРАВЛЕНИЕ БАЗОЙ ===
    def setup_database(self):
        self._create_oe_data()
        self._create_cross_references()
        self._create_prices()
        self._create_parts_data_with_dynamic_schema()

    def _create_oe_data(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe_data (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR
            )
        """)

    def _create_cross_references(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm)
            )
        """)

    def _create_prices(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                price DOUBLE,
                currency VARCHAR DEFAULT 'RUB',
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)

    def _create_parts_data_with_dynamic_schema(self):
        base_sql = """
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
                dimensions_str VARCHAR,
                image_url VARCHAR,
                description VARCHAR
            )
        """
        self.conn.execute(base_sql)
        self.create_indexes()

    def add_missing_columns(self, df: pl.DataFrame, table_name: str):
        existing_cols = {r[0] for r in self.conn.execute(f"DESCRIBE {table_name}").fetchall()}
        for col in df.columns:
            if col not in existing_cols:
                dtype = df[col].dtype
                duckdb_type = "VARCHAR"
                if dtype in [pl.Int32, pl.Int64]: duckdb_type = "BIGINT"
                elif dtype in [pl.Float32, pl.Float64]: duckdb_type = "DOUBLE"
                elif dtype == pl.Boolean: duckdb_type = "BOOLEAN"
                try:
                    self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {duckdb_type}")
                    logger.info(f"✅ Добавлена колонка: {col} в {table_name}")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"⚠️ Не удалось добавить {col}: {e}")

    def create_indexes(self):
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_data_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_data_keys ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]
        for idx in indexes:
            try:
                self.conn.execute(idx)
            except Exception as e:
                logger.debug(f"Индекс: {e}")

    @staticmethod
    def normalize_key(s: pl.Series) -> pl.Series:
        return (s.fill_null("").cast(pl.Utf8)
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\\-\\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    @staticmethod
    def clean_values(s: pl.Series) -> pl.Series:
        return (s.fill_null("").cast(pl.Utf8)
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\\-\\s]", "")
                .str.strip_chars())

    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
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
        actual_lower = {col.lower(): col for col in actual_columns}
        mapping = {}
        for expected in expected_columns:
            for variant in variants.get(expected, [expected]):
                for key, orig in actual_lower.items():
                    if variant.lower() in key and orig not in mapping:
                        mapping[orig] = expected
                        break
        return mapping

    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        try:
            df = pl.read_excel(file_path, engine="calamine")
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
                        i += 1
                    seen.add(new_col)
                    new_names.append(new_col)
                df = df.rename(dict(zip(df.columns, new_names)))

        except Exception as e:
            logger.error(f"❌ Ошибка чтения {file_path}: {e}")
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected = schemas.get(file_type, [])
        mapping = self.detect_columns(df.columns, expected)
        df = df.rename(mapping)

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.normalize_key(pl.col(col)).alias(f"{col}_norm"))

        return df.unique()

    # === 📥 ЗАГРУЗКА ===
    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        if df.is_empty():
            return

        self.add_missing_columns(df, table_name)

        table_cols = [r[0] for r in self.conn.execute(f"DESCRIBE {table_name}").fetchall()]
        df = df.select([col for col in df.columns if col in table_cols])

        df = df.unique(subset=pk, keep="first")
        temp_name = f"temp_{int(time.time())}"
        self.conn.register(temp_name, df.to_arrow())

        cols = df.columns
        cols_str = ", ".join(f'"{c}"' for c in cols)
        pk_str = ", ".join(f'"{c}"' for c in pk)
        update_cols = [c for c in cols if c not in pk]
        action = "DO NOTHING"
        if update_cols:
            update_clause = ", ".join([f'"{c}" = excluded."{c}"' for c in update_cols])
            action = f"DO UPDATE SET {update_clause}"

        sql = f"""
            INSERT INTO {table_name} ({cols_str})
            SELECT {cols_str} FROM {temp_name}
            ON CONFLICT ({pk_str}) {action};
        """

        try:
            self.conn.execute(sql)
            logger.info(f"✅ UPSERT в {table_name}: {len(df)} записей")
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            st.error(f"Ошибка при загрузке в {table_name}")
        finally:
            self.conn.unregister(temp_name)

    def upsert_prices(self, price_df: pl.DataFrame):
        if price_df.is_empty():
            return
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
        st.info("🔄 Обработка данных...")

        if 'oe' in dataframes:
            df_oe = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_data = df_oe.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique()
            self.upsert_data('oe_data', oe_data, ['oe_number_norm'])
            cross = df_oe.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'cross' in dataframes:
            df_cross = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_data = df_cross.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_data, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'prices' in dataframes:
            self.upsert_prices(dataframes['prices'])

        part_updates = []
        for ft in ['barcode', 'dimensions', 'images']:
            if ft in dataframes and not dataframes[ft].is_empty():
                part_updates.append(dataframes[ft])

        if part_updates:
            final_df = pl.concat(part_updates).unique(subset=['artikul_norm', 'brand_norm'], keep='first')
            self.upsert_data('parts_data', final_df, ['artikul_norm', 'brand_norm'])

        st.success("✅ Данные загружены")

    # === 🔍 ПОИСК ===
    def show_search_interface(self):
        st.markdown("<h1 style='text-align: center;'>🔍 Поиск автозапчастей</h1>", unsafe_allow_html=True)
        col1, col2 = st.columns([3, 1])
        with col1:
            search_query = st.text_input("Артикул, бренд, OE, наименование:", value=st.session_state.get("search_query", ""))
        with col2:
            search_button = st.button("🔎 Найти", use_container_width=True)

        if search_button or search_query:
            st.session_state.search_query = search_query
            if not search_query.strip():
                st.info("Введите запрос.")
                return
            with st.spinner("🔍 Ищем..."):
                results = self.search_parts(search_query.strip())
            if results.is_empty():
                st.warning("❌ Ничего не найдено.")
            else:
                st.success(f"✅ Найдено: **{len(results):,}** товаров")
                self.display_search_results(results)

    def search_parts(self, query: str) -> pl.DataFrame:
        query_clean = "".join(filter(str.isalnum, query.strip().lower()))
        try:
            sql = f"""
            WITH PartDetails AS (
                SELECT cr.artikul_norm, cr.brand_norm,
                       STRING_AGG(DISTINCT o.oe_number, ', ') AS oe_list,
                       ANY_VALUE(o.name) AS name
                FROM cross_references cr
                LEFT JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
                GROUP BY cr.artikul_norm, cr.brand_norm
            )
            SELECT 
                p.artikul AS "Артикул",
                p.brand AS "Бренд",
                COALESCE(pd.name, 'Не указано') AS "Наименование",
                COALESCE(pd.oe_list, '') AS "OE номера",
                p.multiplicity AS "Кратность",
                p.barcode AS "Штрих-код",
                ROUND(COALESCE(pr.price, 0), 2) AS "Цена, ₽",
                p.length AS "Длина",
                p.width AS "Ширина",
                p.height AS "Высота",
                p.weight AS "Вес",
                p.dimensions_str AS "Весогабариты",
                p.image_url AS "Изображение"
            FROM parts_data p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN prices pr ON p.artikul_norm = pr.artikul_norm AND p.brand_norm = pr.brand_norm
            WHERE 
                LOWER(p.artikul) ILIKE '%{query_clean}%' OR
                LOWER(p.brand) ILIKE '%{query_clean}%' OR
                LOWER(COALESCE(pd.oe_list, '')) ILIKE '%{query_clean}%' OR
                LOWER(COALESCE(pd.name, '')) ILIKE '%{query_clean}%'
            ORDER BY pr.price NULLS LAST
            LIMIT 1000
            """
            return self.conn.execute(sql).pl()
        except Exception as e:
            st.error(f"❌ Ошибка поиска: {e}")
            return pl.DataFrame()

    def display_search_results(self, results: pl.DataFrame):
        st.dataframe(results.drop(["Изображение"]).to_pandas(), hide_index=True, use_container_width=True)
        if st.checkbox("Показать карточки"):
            for row in results.to_dicts():
                with st.container(border=True):
                    col_img, col_info = st.columns([1, 3])
                    with col_img:
                        if row["Изображение"]:
                            st.image(row["Изображение"], width=80)
                    with col_info:
                        st.markdown(f"**{row['Артикул']} — {row['Бренд']}**")
                        st.markdown(f"🏷️ {row['Наименование']}")
                        if row["OE номера"]:
                            st.markdown(f"🔢 OE: `{row['OE номера']}`")
                        st.markdown(f"💰 **{row['Цена, ₽']} ₽**")

    # === 🗑️ УДАЛЕНИЕ ===
    def show_delete_interface(self):
        st.header("🗑️ Удаление данных")
        action = st.radio("Что удалить?", ["По бренду", "По артикулу"])

        if action == "По бренду":
            self._delete_by_brand()
        else:
            self._delete_by_artikul()

    def _delete_by_brand(self):
        try:
            brands = self.conn.execute("SELECT DISTINCT brand FROM parts_data ORDER BY brand").fetch_df()["brand"].tolist()
        except:
            st.warning("Нет данных о брендах.")
            return
        selected = st.selectbox("Выберите бренд", brands)
        if selected:
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand = ?", [selected]).fetchone()[0]
            st.info(f"Будет удалено: **{count}** записей")
            if st.checkbox("Подтвердить удаление", key="confirm_brand"):
                if st.button("Удалить бренд", type="primary"):
                    deleted = self.delete_by_brand(selected)
                    st.success(f"✅ Удалено: {deleted} записей бренда `{selected}`")
                    st.rerun()

    def _delete_by_artikul(self):
        artikul = st.text_input("Введите артикул для удаления:")
        if artikul:
            norm = self.normalize_key(pl.Series([artikul]))[0]
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [norm]).fetchone()[0]
            st.info(f"Найдено: **{count}** записей")
            if count > 0:
                if st.checkbox("Подтвердить", key="confirm_art"):
                    if st.button("Удалить артикул", type="primary"):
                        deleted = self.delete_by_artikul(norm)
                        st.success(f"✅ Удалено: {deleted} записей артикула `{artikul}`")
                        st.rerun()

    def delete_by_brand(self, brand: str) -> int:
        with self.conn.transaction():
            deleted = self.conn.execute("DELETE FROM parts_data WHERE brand = ?", [brand]).rowcount
            self.conn.execute("DELETE FROM cross_references WHERE brand = ?", [brand])
            return deleted

    def delete_by_artikul(self, artikul_norm: str) -> int:
        with self.conn.transaction():
            deleted = self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).rowcount
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm])
            return deleted

    # === 💰 НАЦЕНКИ >100% ===
    def show_price_settings(self):
        st.header("💰 Настройка цен")
        markup = st.number_input("Общая наценка (%)", 0.0, 500.0, self.price_rules['global_markup'] * 100, step=1.0)
        self.price_rules['global_markup'] = markup / 100
        if st.button("Сохранить наценку"):
            self.save_price_rules()
            st.success("✅ Наценка сохранена")

    # === 📤 ЭКСПОРТ ===
    def show_export_interface(self):
        st.header("📤 Экспорт")
        total = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
        st.info(f"📦 {total:,} товаров")
        if st.button("Экспорт в CSV"):
            path = self.data_dir / "export.csv"
            query = """
            SELECT p.artikul, p.brand, pr.price, pd.oe_list
            FROM parts_data p
            LEFT JOIN prices pr ON p.artikul_norm = pr.artikul_norm AND p.brand_norm = pr.brand_norm
            LEFT JOIN (SELECT artikul_norm, brand_norm, STRING_AGG(oe_number, ', ') AS oe_list FROM cross_references cr JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm GROUP BY 1,2) pd
            ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            """
            df = self.conn.execute(query).pl()
            df.write_csv(str(path), separator=";")
            with open(path, "rb") as f:
                st.download_button("⬇️ Скачать", f, "export.csv", "text/csv")

    # === 📊 СТАТИСТИКА ===
    def show_statistics(self):
        st.header("📊 Статистика")
        try:
            parts = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
            brands = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data").fetchone()[0]
            st.metric("Уникальные товары", f"{parts:,}")
            st.metric("Бренды", f"{brands:,}")
        except Exception as e:
            st.error(f"Ошибка: {e}")

    def merge_all_data_parallel(self, file_paths: Dict[str, str]) -> Dict[str, pl.DataFrame]:
        results = {}
        with ThreadPoolExecutor() as ex:
            futures = {ex.submit(self.read_and_prepare_file, fp, ft): ft for ft, fp in file_paths.items() if fp}
            for fut in as_completed(futures):
                ft = futures[fut]
                try:
                    df = fut.result()
                    if not df.is_empty():
                        results[ft] = df
                except Exception as e:
                    logger.error(f"❌ {ft}: {e}")
        return results


# === 🏁 MAIN ===
def main():
    st.markdown("<style>.main-title {text-align: center; color: #1f77b4;}</style>", unsafe_allow_html=True)
    st.markdown("<h1 class='main-title'>🚗 AutoParts Catalog</h1>", unsafe_allow_html=True)
    st.markdown("---")

    catalog = HighVolumeAutoPartsCatalog()

    menu = st.sidebar.radio("🧭 Меню", [
        "🔍 Поиск",
        "📥 Загрузка",
        "🗑️ Удаление",
        "💰 Цены",
        "📤 Экспорт",
        "📊 Статистика"
    ])

    if menu == "🔍 Поиск":
        catalog.show_search_interface()
    elif menu == "📥 Загрузка":
        catalog.show_upload_interface()
    elif menu == "🗑️ Удаление":
        catalog.show_delete_interface()
    elif menu == "💰 Цены":
        catalog.show_price_settings()
    elif menu == "📤 Экспорт":
        catalog.show_export_interface()
    elif menu == "📊 Статистика":
        catalog.show_statistics()


if __name__ == "__main__":
    main()
