import polars as pl
import duckdb
import streamlit as st
import os
import time
import logging
import io
import zipfile
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import warnings

warnings.filterwarnings('ignore')

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Константы ---
EXCEL_ROW_LIMIT = 1_000_000

class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        # Конфиги
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        # База данных
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        self.setup_database()
        self.create_indexes()

        # Интерфейс
        st.set_page_config(page_title="AutoParts Catalog", layout="wide", page_icon="🚗", initial_sidebar_state="expanded")
        self.apply_custom_style()

    def apply_custom_style(self):
        st.markdown("""
        <style>
        .main { background-color: #f8f9fa; }
        .stButton>button { background-color: #007BFF; color: white; border: none; border-radius: 8px; padding: 10px 20px; font-size: 16px; }
        .stButton>button:hover { background-color: #0056b3; }
        .stButton>button[style="primary"] { background-color: #28a745; }
        .stButton>button[style="primary"]:hover { background-color: #20c997; }
        h1, h2, h3 { color: #003366; }
        .metric-card { background: white; padding: 15px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center; }
        .metric-label { font-size: 14px; color: #6c757d; }
        .metric-value { font-size: 24px; font-weight: bold; color: #007BFF; }
        .stAlert { border-radius: 8px; }
        .stProgress>div>div>div { background-color: #007BFF; }
        </style>
        """, unsafe_allow_html=True)

    # ----------- Загрузка и сохранение конфигов (БЕЗ encoding!) -----------

    def load_cloud_config(self) -> Dict[str, Any]:
        path = self.data_dir / "cloud_config.json"
        default = {"enabled": False, "provider": "s3", "bucket": "", "region": "", "sync_interval": 3600, "last_sync": 0}
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception as e:
                logger.error(f"Ошибка загрузки cloud_config.json: {e}")
                return default
        path.write_text(json.dumps(default, indent=2, ensure_ascii=False))
        return default

    def save_cloud_config(self):
        path = self.data_dir / "cloud_config.json"
        self.cloud_config['last_sync'] = int(time.time())
        path.write_text(json.dumps(self.cloud_config, indent=2, ensure_ascii=False))

    def load_price_rules(self) -> Dict[str, Any]:
        path = self.data_dir / "price_rules.json"
        default = {"global_markup": 0.2, "brand_markups": {}, "min_price": 0.0, "max_price": 99999.0}
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception as e:
                logger.error(f"Ошибка загрузки price_rules.json: {e}")
                return default
        path.write_text(json.dumps(default, indent=2, ensure_ascii=False))
        return default

    def save_price_rules(self):
        path = self.data_dir / "price_rules.json"
        path.write_text(json.dumps(self.price_rules, indent=2, ensure_ascii=False))

    def load_exclusion_rules(self) -> List[str]:
        path = self.data_dir / "exclusion_rules.txt"
        if path.exists():
            try:
                return [line.strip() for line in path.read_text().splitlines() if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка загрузки exclusion_rules.txt: {e}")
                return []
        path.write_text("Кузов\nСтекла\nМасла")
        return ["Кузов", "Стекла", "Масла"]

    def save_exclusion_rules(self):
        path = self.data_dir / "exclusion_rules.txt"
        path.write_text("\n".join(self.exclusion_rules))

    def load_category_mapping(self) -> Dict[str, str]:
        path = self.data_dir / "category_mapping.txt"
        default = {"Радиатор": "Охлаждение", "Шаровая опора": "Подвеска"}
        if path.exists():
            try:
                mapping = {}
                for line in path.read_text().splitlines():
                    if "|" in line:
                        k, v = line.split("|", 1)
                        mapping[k.strip()] = v.strip()
                return mapping
            except Exception as e:
                logger.error(f"Ошибка загрузки category_mapping.txt: {e}")
                return default
        path.write_text("\n".join([f"{k}|{v}" for k, v in default.items()]))
        return default

    def save_category_mapping(self):
        path = self.data_dir / "category_mapping.txt"
        content = "\n".join([f"{k}|{v}" for k, v in self.category_mapping.items()])
        path.write_text(content)

    # ----------- База данных -----------

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
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                price DOUBLE,
                currency VARCHAR DEFAULT 'RUB',
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

    def create_indexes(self):
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_art ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices ON prices(artikul_norm, brand_norm)"
        ]
        for sql in indexes:
            try:
                self.conn.execute(sql)
            except Exception as e:
                logger.warning(e)

    # ----------- Преобразование данных -----------

    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        return (series.fill_null("").cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars().str.to_lowercase())

    @staticmethod
    def clean_values(series: pl.Series) -> pl.Series:
        return (series.fill_null("").cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|подвеск|рычаг|Шаровая опора',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выпуск': 'глушитель|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        name_lower = name_series.str.to_lowercase()
        expr = pl.when(pl.lit(False)).then(pl.lit(None))
        for cat, pattern in categories_map.items():
            expr = expr.when(name_lower.str.contains(pattern)).then(pl.lit(cat))
        return expr.otherwise(pl.lit('Разное')).alias('category')

    def detect_columns(self, actual_cols: List[str], expected_cols: List[str]) -> Dict[str, str]:
        variant_map = {
            'oe_number': ['oe номер', 'oe', 'code'],
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand'],
            'name': ['наименование', 'название', 'name'],
            'applicability': ['применимость', 'автомобиль'],
            'barcode': ['штрих-код', 'barcode', 'ean'],
            'multiplicity': ['кратность', 'multiplicity'],
            'length': ['длина', 'length'],
            'width': ['ширина', 'width'],
            'height': ['высота', 'height'],
            'weight': ['вес', 'weight'],
            'image_url': ['ссылка', 'url', 'image'],
            'dimensions_str': ['весогабариты', 'размеры'],
            'price': ['цена', 'price'],
            'currency': ['валюта', 'currency']
        }
        mapping = {}
        actual_lower = {col.lower(): col for col in actual_cols}
        for expected in expected_cols:
            for variant in variant_map.get(expected, [expected]):
                v_lower = variant.lower()
                for act_lower, act_orig in actual_lower.items():
                    if v_lower in act_lower and act_orig not in mapping:
                        mapping[act_orig] = expected
                        break
        return mapping

    # ----------- Чтение и загрузка -----------

    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        try:
            import fastexcel
            df = pl.from_pandas(fastexcel.read_excel(file_path).load_sheet(0).to_pandas())
        except ImportError:
            try:
                df = pl.read_excel(file_path, engine='calamine')
            except Exception as e:
                logger.error(f"Не удалось прочитать {file_path}: {e}")
                return pl.DataFrame()
        if df.is_empty():
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['artikul', 'brand', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        col_mapping = self.detect_columns(df.columns, schemas.get(file_type, []))
        if not col_mapping:
            return pl.DataFrame()
        df = df.rename(col_mapping)

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))
        df = df.unique(keep='first')

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.normalize_key(pl.col(col)).alias(f"{col}_norm"))

        return df

    def upsert_data(self, table: str, df: pl.DataFrame, pk: List[str]):
        if df.is_empty():
            return
        df = df.unique(keep='first')
        temp = f"temp_{table}_{int(time.time())}"
        self.conn.register(temp, df.to_arrow())
        pk_str = ", ".join(f'"{c}"' for c in pk)
        update_cols = [c for c in df.columns if c not in pk]
        update_clause = ", ".join([f'"{col}" = excluded."{col}"' for col in update_cols])
        on_conflict = f"DO UPDATE SET {update_clause}" if update_cols else "DO NOTHING"
        sql = f"INSERT INTO {table} SELECT * FROM {temp} ON CONFLICT ({pk_str}) {on_conflict};"
        try:
            self.conn.execute(sql)
        finally:
            self.conn.unregister(temp)

    def upsert_prices(self, df: pl.DataFrame):
        if df.is_empty():
            return
        if 'artikul' in df.columns and 'brand' in df.columns:
            df = df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])
        if 'currency' not in df.columns:
            df = df.with_columns(pl.lit('RUB').alias('currency'))
        df = df.filter((pl.col('price') >= self.price_rules['min_price']) & (pl.col('price') <= self.price_rules['max_price']))
        self.upsert_data('prices', df, ['artikul_norm', 'brand_norm'])

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        st.info("🔄 Идёт загрузка и обновление...")
        if 'oe' in dataframes:
            oe = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_data = oe.select(['oe_number_norm', 'oe_number', 'name']).unique()
            if 'name' in oe_data.columns:
                oe_data = oe_data.with_columns(self.determine_category_vectorized(pl.col('name')))
            self.upsert_data('oe_data', oe_data, ['oe_number_norm'])
            cross = oe.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'cross' in dataframes:
            df = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross = df.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'prices' in dataframes:
            self.upsert_prices(dataframes['prices'])

        parts_df = None
        file_priority = ['oe', 'barcode', 'dimensions', 'images']
        key_files = {k: v for k, v in dataframes.items() if k in file_priority}
        if key_files:
            all_parts = pl.concat([df.select(['artikul', 'artikul_norm', 'brand', 'brand_norm']) for df in key_files.values() if 'artikul_norm' in df.columns]).unique()
            parts_df = all_parts
            for ftype in file_priority:
                if ftype not in key_files:
                    continue
                df = key_files[ftype]
                join_cols = [c for c in df.columns if c not in ['artikul', 'artikul_norm', 'brand', 'brand_norm']]
                if not join_cols:
                    continue
                df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique()
                parts_df = parts_df.join(df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)

            if 'multiplicity' not in parts_df.columns:
                parts_df = parts_df.with_columns(multiplicity=pl.lit(1).cast(pl.Int32))
            if 'dimensions_str' not in parts_df.columns:
                parts_df = parts_df.with_columns(dimensions_str=pl.lit(None).cast(pl.Utf8))
            if 'description' not in parts_df.columns:
                parts_df = parts_df.with_columns(
                    pl.concat_str([
                        pl.lit('Артикул: '), pl.col('artikul'),
                        pl.lit(', Бренд: '), pl.col('brand'),
                        pl.lit(', Кратность: '), pl.col('multiplicity').cast(pl.Utf8),
                        pl.lit(' шт.')
                    ], separator='').alias('description')
                )
            self.upsert_data('parts_data', parts_df, ['artikul_norm', 'brand_norm'])
        st.success("✅ Данные загружены")

    def merge_all_data_parallel(self, file_paths: Dict[str, str]) -> Dict:
        dataframes = {}
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.read_and_prepare_file, path, ftype): ftype for ftype, path in file_paths.items() if path}
            for future in as_completed(futures):
                ftype = futures[future]
                try:
                    df = future.result()
                    if not df.is_empty():
                        dataframes[ftype] = df
                        st.success(f"✅ {ftype}: {len(df):,} строк")
                    else:
                        st.warning(f"⚠️ {ftype} пуст")
                except Exception as e:
                    logger.error(f"Ошибка {ftype}: {e}")
        if dataframes:
            self.process_and_load_data(dataframes)
        return {"total_records": self.get_total_records(), "processing_time": time.time() - time.time()}

    def get_total_records(self) -> int:
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()
            return result[0] if result else 0
        except Exception:
            return 0

    # ----------- Экспорт -----------

    def build_export_query(self, cols: Optional[List[str]] = None, prices: bool = True, markup: bool = True) -> str:
        desc = "Высококачественные автозапчасти — надёжное решение."
        cols_map = {
            "Артикул бренда": 'r.artikul AS "Артикул бренда"',
            "Бренд": 'r.brand AS "Бренд"',
            "Наименование": 'COALESCE(r.name, "Без названия") AS "Наименование"',
            "Применимость": 'r.applicability AS "Применимость"',
            "Описание": 'CONCAT(r.description, dt.text) AS "Описание"',
            "Категория": 'r.category AS "Категория товара"',
            "Кратность": 'r.multiplicity AS "Кратность"',
            "OE номер": 'r.oe_list AS "OE номер"',
            "Цена": '"Цена"', "Валюта": '"Валюта"'
        }
        selected = [expr for name, expr in cols_map.items() if cols is None or name in cols]
        price_sql = f"pr.price * (1 + {self.price_rules['global_markup']})" if markup else "pr.price"
        price_join = "LEFT JOIN prices pr ON r.artikul_norm = pr.artikul_norm AND r.brand_norm = pr.brand_norm" if prices else ""
        exclusion = " AND ".join([f"r.name NOT ILIKE '%{ex}%'" for ex in self.exclusion_rules if ex]) or "TRUE"
        return f"""
        WITH dt AS (SELECT CHR(10)||CHR(10)||$${desc}$$ AS text),
        r AS (
            SELECT p.artikul, p.brand, p.description, p.multiplicity,
                   p.artikul_norm, p.brand_norm, ANY_VALUE(o.name) AS name,
                   ANY_VALUE(o.applicability) AS applicability,
                   ANY_VALUE(o.category) AS category,
                   STRING_AGG(DISTINCT o.oe_number, ', ') AS oe_list
            FROM parts_data p
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            LEFT JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY p.artikul, p.brand, p.artikul_norm, p.brand_norm, p.description, p.multiplicity
        )
        SELECT {price_sql} AS "Цена", 'RUB' AS "Валюта", {', '.join(selected)}
        FROM r CROSS JOIN dt {price_join}
        WHERE {exclusion}
        ORDER BY brand, artikul
        """

    def export_to_csv_optimized(self, path: str, cols, prices, markup) -> bool:
        df = self.conn.execute(self.build_export_query(cols, prices, markup)).pl()
        df.write_csv(path, separator=";")
        return True

    def export_to_excel(self, path: str, cols, prices, markup) -> bool:
        import pandas as pd
        df = pd.read_sql(self.build_export_query(cols, prices, markup), self.conn)
        with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        return True

    def show_export_interface(self):
        st.header("📤 Экспорт данных")
        total = self.get_total_records()
        if total == 0:
            st.warning("Нет данных.")
            return
        cols = ["Артикул бренда", "Бренд", "Наименование", "Категория", "OE номер"]
        if self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0] > 0:
            cols += ["Цена", "Валюта"]
        selected = st.multiselect("Столбцы", cols, default=cols)
        fmt = st.radio("Формат", ["CSV", "Excel (.xlsx)"])
        inc_prices = st.checkbox("Цены", value=True)
        apply_markup = st.checkbox("Наценка", value=True, disabled=not inc_prices)
        if st.button("🚀 Экспорт", type="primary"):
            out_path = self.data_dir / f"export.{fmt.lower()}"
            success = self.export_to_csv_optimized(out_path, selected, inc_prices, apply_markup) if fmt == "CSV" \
                else self.export_to_excel(out_path, selected, inc_prices, apply_markup)
            if success:
                with open(out_path, "rb") as f:
                    st.download_button("⬇️ Скачать", f, out_path.name)

    # ----------- Управление -----------

    def delete_by_brand(self, brand_norm: str) -> int:
        with self.conn.transaction():
            c = self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm]).rowcount
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm = ?", [brand_norm])
            return c

    def delete_by_artikul(self, artikul_norm: str) -> int:
        with self.conn.transaction():
            c = self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).rowcount
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm])
            return c

    def show_data_management(self):
        st.header("🗑️ Управление")
        tab1, tab2 = st.tabs(["Удаление", "Настройки"])
        with tab1:
            self._show_delete_by_brand()
            self._show_delete_by_artikul()
        with tab2:
            st.write("Настройки...")

    def _show_delete_by_brand(self):
        st.subheader("По бренду")
        brands = [b[0] for b in self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL").fetchall()]
        if not brands:
            st.info("Нет брендов.")
            return
        sel = st.selectbox("Бренд", brands)
        if sel:
            norm = self.conn.execute("SELECT brand_norm FROM parts_data WHERE brand = ? LIMIT 1", [sel]).fetchone()[0]
            cnt = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [norm]).fetchone()[0]
            st.write(f"Удалить {cnt} записей?")
            if st.button("Удалить", type="primary") and st.checkbox("Подтверди"):
                self.delete_by_brand(norm)
                st.success("Готово")
                st.rerun()

    def _show_delete_by_artikul(self):
        st.subheader("По артикулу")
        art = st.text_input("Артикул")
        if art:
            norm = self.normalize_key(pl.Series([art]))[0]
            cnt = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [norm]).fetchone()[0]
            st.write(f"Найдено: {cnt}")
            if st.button("Удалить") and st.checkbox("Подтверди"):
                self.delete_by_artikul(norm)
                st.success("Готово")
                st.rerun()

    # ----------- Статистика -----------

    def get_statistics(self):
        s = {k: self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t, k in [("parts_data", "parts"), ("oe_data", "oe"), ("prices", "prices")]}
        s['brands'] = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data").fetchone()[0]
        s['top'] = self.conn.execute("SELECT brand, COUNT(*) c FROM parts_data GROUP BY brand ORDER BY c DESC LIMIT 5").pl()
        return s

    def show_statistics(self):
        st.header("📈 Статистика")
        s = self.get_statistics()
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-card"><div class="metric-value">{s["parts"]:,}</div>Артикулов</div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><div class="metric-value">{s["brands"]}</div>Брендов</div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><div class="metric-value">{s["prices"]:,}</div>Цен</div>', unsafe_allow_html=True)
        st.subheader("🏆 Топ брендов")
        st.dataframe(s["top"].to_pandas(), use_container_width=True)


def main():
    st.title("🚗 AutoParts Catalog")
    cpu, mem = psutil.cpu_percent(), psutil.virtual_memory().percent
    c1, c2 = st.columns(2)
    c1.markdown(f'<div class="metric-card"><div class="metric-value">{cpu}%</div>CPU</div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card"><div class="metric-value">{mem}%</div>RAM</div>', unsafe_allow_html=True)

    catalog = HighVolumeAutoPartsCatalog()
    choice = st.sidebar.radio("Меню", ["Загрузка", "Экспорт", "Статистика", "Управление"])

    if choice == "Загрузка":
        st.header("📥 Загрузка")
        ftypes = ['oe', 'cross', 'prices']
        cols = st.columns(2)
        files = {}
        for i, t in enumerate(ftypes):
            with cols[i % 2]:
                files[t] = st.file_uploader(t.capitalize(), type=['xlsx'])
        if st.button("🚀 Загрузить"):
            paths = {}
            for k, v in files.items():
                if v:
                    p = catalog.data_dir / f"upload_{k}_{time.time()}.xlsx"
                    with open(p, "wb") as f: f.write(v.getbuffer())
                    paths[k] = str(p)
            catalog.merge_all_data_parallel(paths)

    elif choice == "Экспорт":
        catalog.show_export_interface()

    elif choice == "Статистика":
        catalog.show_statistics()

    elif choice == "Управление":
        catalog.show_data_management()


if __name__ == "__main__":
    main()
