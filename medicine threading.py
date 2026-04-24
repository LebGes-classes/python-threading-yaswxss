import pandas as pd
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os


class MedicalDevice:
    """Класс для медицинского оборудования"""

    STATUS_MAPPING = {
        'planned_installation': 'planned_installation',
        'планируется': 'planned_installation',
        'planned': 'planned_installation',
        'operational': 'operational',
        'op': 'operational',
        'ok': 'operational',
        'работает': 'operational',
        'working': 'operational',
        'service_scheduled': 'maintenance_scheduled',
        'maintenance_scheduled': 'maintenance_scheduled',
        'maintenance': 'maintenance_scheduled',
        'maint_sched': 'maintenance_scheduled',
        'запланировано то': 'maintenance_scheduled',
        'faulty': 'faulty',
        'broken': 'faulty',
        'неисправно': 'faulty',
        'не работает': 'faulty',
        'error': 'faulty',
        'needs_repair': 'faulty'
    }

    def __init__(self, device_data: pd.Series):
        """Инициализация устройства на основе строки данных"""
        self.device_id = device_data.get('device_id')
        self.clinic_id = device_data.get('clinic_id')
        self.clinic_name = device_data.get('clinic_name')
        self.city = device_data.get('city')
        self.department = device_data.get('department')
        self.model = device_data.get('model')
        self.serial_number = device_data.get('serial_number')
        self.install_date = self.parse_date(device_data.get('install_date'))
        self.status = self.normalize_status(device_data.get('status'))
        self.warranty_until = self.parse_date(device_data.get('warranty_until'))
        self.last_calibration_date = self.parse_date(device_data.get('last_calibration_date'))
        self.last_service_date = self.parse_date(device_data.get('last_service_date'))
        self.issues_reported_12mo = self.parse_numeric(device_data.get('issues_reported_12mo'))
        self.failure_count_12mo = self.parse_numeric(device_data.get('failure_count_12mo'))
        self.uptime_pct = self.parse_uptime(device_data.get('uptime_pct'))
        self.issues_text = device_data.get('issues_text', '')

    def parse_date(self, date_value):
        """Парсинг даты из различных форматов"""
        if pd.isna(date_value) or date_value is None:
            return None

        try:
            if isinstance(date_value, (datetime, pd.Timestamp)):
                return pd.to_datetime(date_value)
            elif isinstance(date_value, str):
                for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y', '%b %d, %Y',
                            '%Y-%m-%d %H:%M:%S']:
                    try:
                        return pd.to_datetime(datetime.strptime(date_value, fmt))
                    except ValueError:
                        continue
            return None
        except:
            return None

    def normalize_status(self, status):
        """Нормализация статуса устройства"""
        if pd.isna(status) or status is None:
            return 'unknown'

        status_str = str(status).lower().strip()
        return self.STATUS_MAPPING.get(status_str, 'unknown')

    def parse_numeric(self, value):
        """Парсинг числовых значений"""
        try:
            if pd.isna(value):
                return 0
            return int(float(value))
        except:
            return 0

    def parse_uptime(self, value):
        """Парсинг процента времени работы"""
        try:
            if pd.isna(value):
                return 0.0
            if isinstance(value, str) and '%' in value:
                return float(value.replace('%', '').strip()) / 100
            return float(value)
        except:
            return 0.0

    def is_under_warranty(self):
        """Проверка, находится ли устройство на гарантии"""
        if self.warranty_until is None:
            return False
        return self.warranty_until > datetime.now()

    def needs_calibration(self):
        """Проверка, требуется ли калибровка (более года назад)"""
        if self.last_calibration_date is None:
            return True
        days_since_calibration = (datetime.now() - self.last_calibration_date).days
        return days_since_calibration > 365


class DataLoader:
    """Класс для загрузки данных из файлов"""

    def __init__(self, file_paths):
        self.file_paths = file_paths
        self.devices = []
        self.lock = threading.Lock()

    def load_file(self, file_path):
        """Загрузка одного файла"""
        try:
            df = pd.read_excel(file_path)
            devices_batch = []
            for _, row in df.iterrows():
                try:
                    device = MedicalDevice(row)
                    devices_batch.append(device)
                except Exception as e:
                    print(f"Ошибка при обработке строки в файле {file_path}: {e}")

            with self.lock:
                self.devices.extend(devices_batch)

            print(f"Загружено {len(devices_batch)} устройств из {file_path}")
            return devices_batch
        except Exception as e:
            print(f"Ошибка при загрузке файла {file_path}: {e}")
            return []

    def load_all(self):
        """Загрузка всех файлов с использованием многопоточности"""
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(self.load_file, self.file_paths)

        end_time = time.time()
        print(f"Загружено всего {len(self.devices)} устройств за {end_time - start_time:.2f} сек")
        return self.devices


class WarrantyAnalyzer:
    """Анализ гарантийных данных"""

    @staticmethod
    def process(devices, output_file='reports/warranty_report.xlsx'):
        """Анализ гарантий - выполняется в отдельном потоке"""
        start_time = time.time()
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Начало анализа гарантий...")

        warranty_data = []
        for device in devices:
            warranty_data.append({
                'device_id': device.device_id,
                'clinic_name': device.clinic_name,
                'model': device.model,
                'warranty_until': device.warranty_until,
                'under_warranty': device.is_under_warranty(),
                'status': device.status
            })

        df_result = pd.DataFrame(warranty_data)

        os.makedirs('reports', exist_ok=True)
        df_result.to_excel(output_file, index=False)

        end_time = time.time()
        print(f"[{thread_name}] Завершено за {end_time - start_time:.2f} сек")
        print(f"  - Устройств на гарантии: {df_result['under_warranty'].sum()}")
        print(f"  - Устройств с истекшей гарантией: {(~df_result['under_warranty']).sum()}")

        return df_result


class ProblemClinicAnalyzer:
    """Анализ клиник с проблемами"""

    @staticmethod
    def process(devices, top_n=10, output_file='reports/problem_clinics_report.xlsx'):
        """Поиск клиник с наибольшим количеством проблем - выполняется в отдельном потоке"""
        start_time = time.time()
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Начало анализа проблем клиник...")

        clinic_problems = []
        for device in devices:
            problem_score = (device.issues_reported_12mo * 10 +
                             device.failure_count_12mo * 20 +
                             (50 if device.uptime_pct < 0.95 else 0) +
                             (100 if device.status == 'faulty' else 0))

            clinic_problems.append({
                'clinic_id': device.clinic_id,
                'clinic_name': device.clinic_name,
                'city': device.city,
                'problem_score': problem_score,
                'issues_count': device.issues_reported_12mo,
                'failures_count': device.failure_count_12mo,
                'devices_count': 1
            })

        df_problems = pd.DataFrame(clinic_problems)

        clinic_agg = df_problems.groupby(['clinic_id', 'clinic_name', 'city']).agg({
            'problem_score': 'sum',
            'issues_count': 'sum',
            'failures_count': 'sum',
            'devices_count': 'count'
        }).reset_index()

        clinic_agg = clinic_agg.sort_values('problem_score', ascending=False).head(top_n)

        os.makedirs('reports', exist_ok=True)
        clinic_agg.to_excel(output_file, index=False)

        end_time = time.time()
        print(f"[{thread_name}] Завершено за {end_time - start_time:.2f} сек")
        print(f"  - Топ-{top_n} клиник по проблемам")

        return clinic_agg


class CalibrationAnalyzer:
    """Анализ калибровки"""

    @staticmethod
    def process(devices, output_file='reports/calibration_report.xlsx'):
        """Построение отчёта по срокам калибровки - выполняется в отдельном потоке"""
        start_time = time.time()
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Начало анализа калибровки...")

        calibration_data = []
        for device in devices:
            calibration_data.append({
                'device_id': device.device_id,
                'clinic_name': device.clinic_name,
                'model': device.model,
                'last_calibration_date': device.last_calibration_date,
                'install_date': device.install_date,
                'needs_calibration': device.needs_calibration(),
                'days_since_calibration': (
                            datetime.now() - device.last_calibration_date).days if device.last_calibration_date else None,
                'status': device.status
            })

        df_calibration = pd.DataFrame(calibration_data)

        os.makedirs('reports', exist_ok=True)
        df_calibration.to_excel(output_file, index=False)

        end_time = time.time()
        print(f"[{thread_name}] Завершено за {end_time - start_time:.2f} сек")
        print(f"  - Всего устройств: {len(df_calibration)}")
        print(f"  - Требуют калибровки: {df_calibration['needs_calibration'].sum()}")
        print(f"  - Нет данных: {df_calibration['last_calibration_date'].isna().sum()}")

        return df_calibration


class PivotTableAnalyzer:
    """Создание сводной таблицы"""

    @staticmethod
    def process(devices, output_file='reports/pivot_table.xlsx'):
        """Составление сводной таблицы по клиникам и оборудованию - выполняется в отдельном потоке"""
        start_time = time.time()
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Начало создания сводной таблицы...")

        pivot_data = []
        for device in devices:
            pivot_data.append({
                'clinic_name': device.clinic_name,
                'city': device.city,
                'model': device.model,
                'status': device.status,
                'under_warranty': device.is_under_warranty(),
                'needs_calibration': device.needs_calibration(),
                'issues_reported': device.issues_reported_12mo,
                'failures': device.failure_count_12mo,
                'uptime_pct': device.uptime_pct,
                'device_id': device.device_id
            })

        df_pivot = pd.DataFrame(pivot_data)

        pivot_table = pd.pivot_table(
            df_pivot,
            values=['device_id', 'issues_reported', 'failures', 'uptime_pct'],
            index=['clinic_name', 'city'],
            columns=['model'],
            aggfunc={
                'device_id': 'count',
                'issues_reported': 'sum',
                'failures': 'sum',
                'uptime_pct': 'mean'
            },
            fill_value=0
        )

        pivot_table.columns = [f'{col[1]}_{col[0]}' for col in pivot_table.columns]

        os.makedirs('reports', exist_ok=True)
        pivot_table.to_excel(output_file, index=True)

        end_time = time.time()
        print(f"[{thread_name}] Завершено за {end_time - start_time:.2f} сек")
        print(f"  - Создана сводная таблица: {pivot_table.shape[0]} клиник, {pivot_table.shape[1]} показателей")

        return pivot_table


class CombinedReportGenerator:
    """Генерация объединённого отчёта"""

    @staticmethod
    def generate_combined_report(warranty_df, problems_df, calibration_df, pivot_df,
                                 output_file='reports/combined_report.xlsx'):
        """Сохранение всех отчётов в один файл с разными листами"""
        start_time = time.time()
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Сохранение объединённого отчёта...")

        os.makedirs('reports', exist_ok=True)
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            warranty_df.to_excel(writer, sheet_name='Гарантия', index=False)
            problems_df.to_excel(writer, sheet_name='Проблемные_клиники', index=False)
            calibration_df.to_excel(writer, sheet_name='Калибровка', index=False)
            pivot_df.to_excel(writer, sheet_name='Сводная_таблица')

        end_time = time.time()
        print(f"[{thread_name}] Объединённый отчёт сохранён за {end_time - start_time:.2f} сек")
        print(f"  - Файл: {output_file}")


def main():
    """Основная функция с использованием многопоточности"""

    file_paths = [
        'medical_diagnostic_devices_1.xlsx',
        'medical_diagnostic_devices_2.xlsx',
        'medical_diagnostic_devices_3.xlsx',
        'medical_diagnostic_devices_4.xlsx',
        'medical_diagnostic_devices_5.xlsx',
        'medical_diagnostic_devices_6.xlsx',
        'medical_diagnostic_devices_7.xlsx',
        'medical_diagnostic_devices_8.xlsx',
        'medical_diagnostic_devices_9.xlsx',
        'medical_diagnostic_devices_10.xlsx'
    ]

    available_files = [f for f in file_paths if os.path.exists(f)]
    if not available_files:
        print("Файлы с данными не найдены")
        return

    print(f"\nНайдено {len(available_files)} файлов для обработки")

    # Загрузка данных
    loader = DataLoader(available_files)
    devices = loader.load_all()

    if not devices:
        print("Не удалось загрузить данные")
        return

    print("ЗАПУСК АНАЛИТИЧЕСКИХ ЗАДАЧ В ПАРАЛЛЕЛЬНЫХ ПОТОКАХ")

    total_start_time = time.time()

    with ThreadPoolExecutor(max_workers=4) as executor:
        warranty_future = executor.submit(WarrantyAnalyzer.process, devices)
        problems_future = executor.submit(ProblemClinicAnalyzer.process, devices)
        calibration_future = executor.submit(CalibrationAnalyzer.process, devices)
        pivot_future = executor.submit(PivotTableAnalyzer.process, devices)

        warranty_df = warranty_future.result()
        problems_df = problems_future.result()
        calibration_df = calibration_future.result()
        pivot_df = pivot_future.result()

    CombinedReportGenerator.generate_combined_report(
        warranty_df, problems_df, calibration_df, pivot_df
    )

    total_end_time = time.time()

    print(f"ВЫПОЛНЕНЫ ЗА {total_end_time - total_start_time:.2f} СЕКУНД")
    print("  1. reports/warranty_report.xlsx - Отчёт по гарантии")
    print("  2. reports/problem_clinics_report.xlsx - Проблемные клиники")
    print("  3. reports/calibration_report.xlsx - Отчёт по калибровке")
    print("  4. reports/pivot_table.xlsx - Сводная таблица")
    print("  5. reports/combined_report.xlsx - Объединённый отчёт")


if __name__ == "__main__":
    main()