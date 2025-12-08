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
            brands_result = self.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL ORDER BY brand").fetchall()
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
        new_exclusions = st.text_area(
            "Список исключений (по одному на строку):",
            value=current_exclusions,
            height=200,
            placeholder="Введите слова для исключения, например:\nКузов\nСтекла\nМасла"
        )

        if st.button("Сохранить правила исключения"):
            # Очистка и фильтрация ввода
            cleaned = [line.strip() for line in new_exclusions.splitlines() if line.strip()]
            if len(cleaned) != len(set(cleaned)):
                st.warning("Обнаружены дублирующиеся записи. Они будут автоматически удалены.")
            self.exclusion_rules = list(dict.fromkeys(cleaned))
            self.save_exclusion_rules()
            st.success("✅ Правила исключения сохранены")

    def show_category_mapping(self):
        """Интерфейс настройки пользовательских категорий"""
        st.header("🗂️ Управление категориями товаров")
        st.info("Настройте соответствие между названиями товаров и категориями")

        # Отображение текущих правил
        st.subheader("Текущие правила категоризации")
        if self.category_mapping:
            mapping_df = pl.DataFrame({
                "Название товара": list(self.category_mapping.keys()),
                "Категория": list(self.category_mapping.values())
            }).to_pandas()
            st.dataframe(mapping_df, use_container_width=True, hide_index=True)
        else:
            st.write("Нет пользовательских правил категоризации")

        # Добавление нового правила
        st.subheader("Добавить новое правило")
        col1, col2 = st.columns(2)
        with col1:
            name_pattern = st.text_input("Ключевое слово в названии:")
        with col2:
            category = st.text_input("Категория:")

        if st.button("➕ Добавить правило"):
            if name_pattern.strip() and category.strip():
                # Регистронезависимая проверка дублей
                normalized_key = name_pattern.strip().lower()
                existing_keys = {k.lower(): k for k in self.category_mapping.keys()}
                if normalized_key in existing_keys:
                    st.warning(f"Предупреждение: правило для '{existing_keys[normalized_key]}' будет обновлено")
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
                # Здесь должна быть интеграция с провайдером облака (boto3, google-cloud-storage и др.)
                time.sleep(1.5)  # Имитация задержки
                st.success(f"📤 База данных отправлена в {self.cloud_config['provider']}://{self.cloud_config['bucket']}")
                self.cloud_config['last_sync'] = int(time.time())
                self.save_cloud_config()
            except Exception as e:
                st.error(f"❌ Ошибка синхронизации: {str(e)}")

    def show_export_interface(self):
        """Интерфейс экспорта данных в CSV/Excel/Parquet"""
        st.header("📤 Экспорт данных")
        total_records = self.conn.execute("""
            SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)
        """).fetchone()[0]
        st.info(f"📦 Всего уникальных пар (артикул + бренд): **{total_records:,}**")
        if total_records == 0:
            st.warning("База данных пуста. Загрузите данные перед экспортом.")
            return

        # Доступные колонки
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

        # Параметры экспорта
        col1, col2 = st.columns(2)
        with col1:
            export_format = st.radio("Формат", ["CSV", "Excel (.xlsx)", "Parquet"])
        with col2:
            include_prices = st.checkbox("Включить цены", value=True)
            apply_markup = st.checkbox("Применить наценку", value=True, disabled=not include_prices)

        if st.button("🚀 Выполнить экспорт", type="primary"):
            output_path = self.data_dir / f"auto_parts_export.{export_format.lower().replace(' ', '_')}"
            with st.spinner("Формирование отчета..."):
                if export_format == "CSV":
                    success = self.export_to_csv_optimized(
                        str(output_path),
                        selected_columns if selected_columns else None,
                        include_prices,
                        apply_markup
                    )
                elif export_format == "Excel (.xlsx)":
                    success = self.export_to_excel_optimized(
                        str(output_path),
                        selected_columns if selected_columns else None,
                        include_prices,
                        apply_markup
                    )
                elif export_format == "Parquet":
                    success = self.export_to_parquet(
                        str(output_path),
                        selected_columns if selected_columns else None,
                        include_prices,
                        apply_markup
                    )
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
                    LEFT JOIN (
                        SELECT cr.artikul_norm, cr.brand_norm, ANY_VALUE(o.category) AS representative_category
                        FROM cross_references cr
                        LEFT JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
                        GROUP BY cr.artikul_norm, cr.brand_norm
                    ) pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
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
