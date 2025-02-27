import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import lasio
import psycopg2
import datetime
import os
import random
from psycopg2 import sql
import numpy as np
import csv
import re

def load_mnemonic_mapping(csv_path):
    """
    Загружает словарь мнемоник из CSV-файла.
    """
    mapping = {}
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Пропускаем заголовок
        mapping = {row[0]: row[1] for row in reader}
    print("Словарь загружен из CSV.")
    return mapping

def replace_mnemonics_in_las(las_lines, mapping):
    """
    Заменяет мнемоники в строках LAS-файла согласно справочнику.
    """
    updated_lines = []
    for line in las_lines:
        for old, new in mapping.items():
            line = re.sub(rf'\b{re.escape(old)}\b', new, line)
        updated_lines.append(line)
    return updated_lines

class Mnemonic:
    def __init__(self, line):
        self.line = line
        self.mnem, self.units_data, self.data, self.description = self.parse_line()

    def parse_line(self):
        parts = self.line.split(":")
        description = parts[-1].strip() if len(parts) > 1 else ""
        data_part = parts[0].strip()
        if " " in data_part:
            last_space_index = data_part.rfind(" ")
            self.data = data_part[last_space_index + 1:].strip()  # Все после последнего пробела
            mnemonic_and_units = data_part[:last_space_index].strip()
        else:
            self.data = ""
            mnemonic_and_units = data_part.strip()
        
        if "." in mnemonic_and_units:
            mnem_parts = mnemonic_and_units.split(".")
            self.mnem = mnem_parts[0].strip()
            self.units_data = mnem_parts[1].strip() if len(mnem_parts) > 1 else ""
        else:
            self.mnem = mnemonic_and_units
            self.units_data = ""

        # Если описание пустое, заполняем его значением 'UNKNOWN'
        if not description:
            description = "UNKNOWN"

        return self.mnem, self.units_data, self.data, description

    def __str__(self):
        return f"{self.mnem}  .{self.units_data}  {self.data} : {self.description}"


def fix_file_encoding(file_path):
    import chardet
    with open(file_path, 'rb') as file:
        result = chardet.detect(file.read())
    encoding = result['encoding']
    if encoding == 'utf-8':
        return
    with open(file_path, 'r', encoding=encoding, errors='replace') as file:
        content = file.read()
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)
        
def extract_mnemonics_and_data(file_path):
    """Извлекает мнемоники и данные из файла LAS."""
    version_info = []
    well_info = []
    curve_info = []
    ascii_data = []
    section = None
    data_section = False

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()

            # Проверка на начало секции
            if line.lower().startswith("~version"):
                section = version_info
                data_section = False
            elif line.lower().startswith("~well"):
                section = well_info
                data_section = False
            elif line.lower().startswith("~curve"):
                section = curve_info
                data_section = False
            elif line.lower().startswith("~ascii"):
                section = ascii_data
                data_section = True  # Начало секции данных
            elif line.startswith("~"):
                section = None
                data_section = False

            if section is not None and not data_section:
                section.append(line)
            elif data_section:
                ascii_data.append(line)

    return version_info, well_info, curve_info, ascii_data


def clean_duplicate_header_lines(info):
    """Удаляет лишние строки, дублирующие заголовки секций."""
    if info and len(info) > 1:
        # Пропускаем первую строку заголовка, но удаляем строки, которые дублируют заголовок (например, 'Well Information Block')
        header = info[0].lower()
        cleaned_info = [info[0]]  # Всегда сохраняем первую строку
        for line in info[1:]:
            if line.lower().startswith(header):
                continue  # Пропускаем строки, дублирующие заголовок
            cleaned_info.append(line)
        return cleaned_info
    return info


def add_missing_version_mnemonics(version_info):
    """Проверяет наличие обязательных мнемоник и добавляет, если они отсутствуют."""
    # Проверяем наличие VERS и WRAP
    version_mnemonics = [line.split(".")[0].strip() for line in version_info if "." in line]
    
    if "WRAP" not in version_mnemonics:
        version_info.append("WRAP.      NO : One line per depth step")
    
    return version_info


def add_missing_well_mnemonics(well_info):
    """Добавляет обязательные мнемоники в секцию ~Well information, если они отсутствуют."""
    required_mnemonics = ["STRT", "STOP", "STEP", "NULL", "WELL", "FLD"]
    optional_mnemonics = ["COMP", "LOC", "SRVC", "PROV", "UWI", "API", "DATE"]

    mnemonic_dict = {line.split(".")[0].strip(): line for line in well_info if "." in line}

    # Добавляем обязательные мнемоники с дефолтными значениями, если их нет
    for mnemonic in required_mnemonics + optional_mnemonics:
        if mnemonic not in mnemonic_dict:
            well_info.append(f"{mnemonic}  .  0 : UNKNOWN")

    return well_info


def create_las_file(version_info, well_info, curve_info, ascii_data, output_file):
    """Создает новый LAS файл с правильным форматированием и обязательными мнемониками."""
    with open(output_file, 'w', encoding='utf-8') as file:
        # Запись секции версии без дублирования заголовка
        file.write("#==================================================================\n")
        file.write("~Version information\n")
        version_info = add_missing_version_mnemonics(version_info)  # Проверка обязательных мнемоник в версии
        version_info = clean_duplicate_header_lines(version_info)  # Удаление лишних строк
        for line in version_info[1:]:  # Пропускаем первый элемент (~Version information)
            file.write(line + "\n")

        # Проверяем, что секция ~Well information не дублируется
        if "~Well information" not in [line.lower() for line in well_info]:
            file.write("#==================================================================\n")
            file.write("~Well information\n")
            well_info = add_missing_well_mnemonics(well_info)  # Добавляем обязательные мнемоники
            well_info = clean_duplicate_header_lines(well_info)  # Удаление лишних строк
            for line in well_info:
                file.write(line + "\n")

        # Запись секции кривых
        file.write("#==================================================================\n")
        if curve_info:
            file.write("~Curve information\n")  # Пишем только один раз
            curve_info = clean_duplicate_header_lines(curve_info)  # Удаление лишних строк
            for line in curve_info[1:]:  # Пропускаем дублированный заголовок
                file.write(line + "\n")

        # Запись данных ASCII
        file.write("#==================================================================\n")
        file.write("~ASCII log data\n")
        for line in ascii_data[1:]:  # Пропускаем дублированный заголовок
            # Убедимся, что строка состоит из числовых данных и отформатируем ее
            values = line.split()
            if len(values) > 0 and all(v.replace('.', '', 1).isdigit() for v in values):
                formatted_line = '  '.join(f"{float(v):10.3f}" for v in values)
                file.write(formatted_line + "\n")
            else:
                file.write(line + "\n")


def preprocess_las_file(input_file_path, output_file_path, csv_path):
    """
    Основная функция для обработки LAS файла.
    """
    mapping = load_mnemonic_mapping(csv_path)
    
    with open(input_file_path, 'r', encoding='utf-8') as f:
        las_lines = f.readlines()
    
    updated_las = replace_mnemonics_in_las(las_lines, mapping)
    
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.writelines(updated_las)
    
    return output_file_path





def preprocess_interface():
    input_file_path = filedialog.askopenfilename(title="Выберите входной файл LAS", filetypes=[("LAS files", "*.las")])
    if not input_file_path:
        return

    output_file_path = filedialog.asksaveasfilename(title="Сохраните выходной файл LAS", defaultextension=".las", filetypes=[("LAS files", "*.las")])
    if not output_file_path:
        return

    csv_path = filedialog.askopenfilename(title="Выберите CSV-словарь", filetypes=[("CSV files", "*.csv")])
    if not csv_path:
        return
    
    try:
        result_file = preprocess_las_file(input_file_path, output_file_path, csv_path)
        messagebox.showinfo("Success", f"LAS файл успешно обновлен: {result_file}")
    except Exception as e:
        messagebox.showerror("Error", f"Ошибка при предобработке файла: {e}")

def browse_file():
    filename = filedialog.askopenfilename(title="Выберите входной файл LAS", filetypes=[("LAS files", "*.las")])
    if filename:
        entry_file_path.delete(0, tk.END)
        entry_file_path.insert(0, filename)

def browse_output_directory():
    directory = filedialog.askdirectory(title="Выберите директорию для экспорта")
    if directory:
        entry_output_dir.delete(0, tk.END)
        entry_output_dir.insert(0, directory)

def convert_numpy_types(value):
    """Преобразует numpy типы в стандартные типы Python."""
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64)):
        return float(value)
    elif isinstance(value, (np.datetime64)):
        return str(value)  # Преобразуем даты в строки
    return value

def validate_gis_date(gis_date):
    """Проверяет, является ли gis_date допустимой датой, если нет, возвращает '1000-01-01'."""
    try:
        # Пробуем преобразовать в дату
        if gis_date not in [None, '0', '', 0]:
            datetime.datetime.strptime(gis_date, '%Y-%m-%d')  # Проверка формата
            return gis_date
        else:
            return '1000-01-01'  # Если gis_date некорректна, заменяем на дефолтное значение
    except (ValueError, TypeError):
        return '1000-01-01'  # В случае ошибки возвращаем дефолтную дату

def load_las_to_db(las_file_path, db_config, author_name, comment):
    las = lasio.read(las_file_path)
    file_name = os.path.basename(las_file_path)
    
    # Чтение данных из файла LAS
    strt = las.well.get('STRT', 0).value if 'STRT' in las.well else 0  
    stop = las.well.get('STOP', 0).value if 'STOP' in las.well else 0  
    srvc = las.well.get('SRVC', '').value if 'SRVC' in las.well else '' 
    well_number = las.well.get('WELL', '').value if 'WELL' in las.well else 0
    field_name = las.well.get('FLD', '').value if 'FLD' in las.well else ''
    gis_date = las.well.get('DATE', '1000-01-01').value if 'DATE' in las.well else '1000-01-01'
    upload_date = datetime.date.today()
    well_id = las.well.get('UWI', 0).value if 'UWI' in las.well else 0

    # Преобразование всех значений в стандартные типы Python
    strt = convert_numpy_types(strt)
    stop = convert_numpy_types(stop)
    well_number = convert_numpy_types(well_number)
    well_id = convert_numpy_types(well_id)
    field_name = str(field_name) if not isinstance(field_name, str) else field_name

    # Проверяем значение даты и заменяем некорректные значения
    gis_date = validate_gis_date(str(gis_date))
    upload_date = str(upload_date)  # Преобразуем дату загрузки в строку

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Проверяем наличие поля в таблице "field"
    cursor.execute(sql.SQL("SELECT id FROM public.field WHERE name = %s"), (field_name,))
    field_result = cursor.fetchone()

    if field_result is None:
        # Добавляем новое поле, если оно отсутствует
        cursor.execute(sql.SQL("INSERT INTO public.field (name) VALUES (%s) RETURNING id"), (field_name,))
        field_id = cursor.fetchone()[0]
    else:
        # Используем уже существующее поле
        field_id = field_result[0]

    # Вставляем данные о скважине
    cursor.execute(sql.SQL("INSERT INTO public.well (well_number, field_id) VALUES (%s, %s) RETURNING id"),
                   (well_number, field_id))
    well_id = cursor.fetchone()[0]

    # Преобразуем все значения перед SQL-запросом, чтобы они были стандартными типами
    file_name = convert_numpy_types(file_name)
    las_file_path = convert_numpy_types(las_file_path)
    method = 'A'
    strt = convert_numpy_types(strt)
    stop = convert_numpy_types(stop)
    srvc = convert_numpy_types(srvc)
    gis_date = convert_numpy_types(gis_date)
    upload_date = convert_numpy_types(upload_date)
    author_name = convert_numpy_types(author_name)
    comment = convert_numpy_types(comment)
    well_id = convert_numpy_types(well_id)

    # Вставляем данные о GIS
    cursor.execute(sql.SQL(
        """INSERT INTO public."GIS" 
           (file_name, file_path, method, strt, stop, srvc, gis_date, upload_date, author_name, comment, well_id) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""),
           (file_name, las_file_path, method, strt, stop, srvc, gis_date, upload_date, author_name, comment, well_id))
    
    conn.commit()
    cursor.close()
    conn.close()
    messagebox.showinfo("Success", "Файл успешно загружен в базу данных.")



def export_las_from_db(db_config, output_dir, field_names=None, well_numbers=None, gis_methods=None, gis_intervals=None):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    

    query = sql.SQL("SELECT g.file_name, g.file_path, f.name AS field_name, w.well_number, g.method, g.strt, g.stop "
                   "FROM public.\"GIS\" g "
                   "JOIN public.well w ON g.well_id = w.id "
                   "JOIN public.field f ON w.field_id = f.id "
                   "WHERE 1=1 ")
    
    if field_names:
        query += sql.SQL(" AND f.name IN %s ").format(sql.Literal(tuple(field_names)))
    
    if well_numbers:
        query += sql.SQL(" AND w.well_number IN %s ").format(sql.Literal(tuple(well_numbers)))
    
    if gis_methods:
        query += sql.SQL(" AND g.method IN %s ").format(sql.Literal(tuple(gis_methods)))
    
    if gis_intervals:
        query += sql.SQL(" AND (g.strt BETWEEN %s AND %s OR g.stop BETWEEN %s AND %s) ").\
                 format(sql.Literal(gis_intervals[0]), sql.Literal(gis_intervals[1]),
                        sql.Literal(gis_intervals[0]), sql.Literal(gis_intervals[1]))
    
    cursor.execute(query)
    

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    exported_files = []  
    

    for file_name, file_path, field_name, well_number, method, strt, stop in cursor:
        output_file = os.path.join(output_dir, f"{field_name}_{well_number}_{method}.las")
        with open(output_file, 'w') as f:
            with open(file_path, 'r') as las_file:
                f.write(las_file.read())
        exported_files.append(output_file)
    
    cursor.close()
    conn.close()
    
    return exported_files  


def upload_file():
    las_file_path = entry_file_path.get()
    author_name = entry_author.get()
    comment = entry_comment.get()
    
    if not las_file_path or not author_name:
        messagebox.showwarning("Input Error", "Пожалуйста, заполните все поля.")
        return
    
    db_config = {
        'dbname': 'LAS',
        'user': 'postgres',
        'password': 'Azazinkreet228',
        'host': 'localhost',
        'port': '5432'
    }
    
    load_las_to_db(las_file_path, db_config, author_name, comment)



def export_files():
    output_dir = entry_output_dir.get()
    
    # Получение данных из базы данных
    db_config = {
        'dbname': 'LAS',
        'user': 'postgres',
        'password': 'Azazinkreet228',
        'host': 'localhost',
        'port': '5432'
    }
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Получение списка методов GIS
    cursor.execute("SELECT DISTINCT method FROM public.\"GIS\"")
    gis_methods = [row[0] for row in cursor.fetchall()]
    
    # Получение списка номеров скважин
    cursor.execute("SELECT DISTINCT well_number FROM public.well")
    well_numbers = [row[0] for row in cursor.fetchall()]
    
    # Получение списка имен полей
    cursor.execute("SELECT DISTINCT name FROM public.field")
    field_names = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    # Заполнение выпадающих списков
    gis_methods_listbox.delete(0, tk.END)  # Очищаем список
    for method in gis_methods:
        gis_methods_listbox.insert(tk.END, method)  # Добавляем методы GIS в список
    
    well_numbers_listbox.delete(0, tk.END)  # Очищаем список
    for number in well_numbers:
        well_numbers_listbox.insert(tk.END, number)  # Добавляем номера скважин в список
    
    field_names_listbox.delete(0, tk.END)  # Очищаем список
    for name in field_names:
        field_names_listbox.insert(tk.END, name)  # Добавляем имена полей в список

    # Получаем выбранные значения из выпадающих списков
    selected_gis_methods = [gis_methods[idx] for idx in gis_methods_listbox.curselection()]
    selected_well_numbers = [well_numbers[idx] for idx in well_numbers_listbox.curselection()]
    selected_field_names = [field_names[idx] for idx in field_names_listbox.curselection()]
    
    if not output_dir:
        messagebox.showwarning("Input Error", "Пожалуйста, выберите директорию для экспорта.")
        return
    
    # Преобразуем номера скважин и интервалы в нужный формат
    if selected_well_numbers:
        selected_well_numbers = [int(num) for num in selected_well_numbers]
        
    gis_intervals_str = entry_gis_intervals.get().strip()
    if gis_intervals_str:
        try:
            gis_intervals = [float(num.strip()) for num in gis_intervals_str.split(',')]
            if len(gis_intervals) != 2:
                messagebox.showwarning("Input Error", "Введите два значения для интервалов GIS.")
                return
        except ValueError:
            messagebox.showwarning("Input Error", "Введите корректные значения для интервалов GIS.")
            return
    else:
        gis_intervals = None
    exported_files = export_las_from_db(db_config, output_dir, selected_field_names, selected_well_numbers, selected_gis_methods, gis_intervals)
    if exported_files:
        messagebox.showinfo("Success", f"{len(exported_files)} файлов успешно экспортированы.")
    else:
        messagebox.showwarning("No Files Exported", "Не удалось экспортировать ни одного файла.")
# Создание основного окна
root = tk.Tk()
root.title("Загрузка, Экспорт и Предобработка LAS-файлов")

# Создание вкладок
tab_control = ttk.Notebook(root)
tab1 = ttk.Frame(tab_control)
tab2 = ttk.Frame(tab_control)
tab3 = ttk.Frame(tab_control)
tab_control.add(tab1, text='Предобработка')
tab_control.add(tab2, text='Загрузка')
tab_control.add(tab3, text='Экспорт')
tab_control.pack(expand=1, fill='both')

# Вкладка "Предобработка"
tk.Button(tab1, text="Предобработать LAS файл", command=preprocess_interface).pack(padx=10, pady=10)

# Вкладка "Загрузка"
tk.Label(tab2, text="Путь к LAS-файлу:").grid(row=0, column=0, padx=10, pady=10)
entry_file_path = tk.Entry(tab2, width=40)
entry_file_path.grid(row=0, column=1, padx=10, pady=10)
tk.Button(tab2, text="Обзор", command=browse_file).grid(row=0, column=2, padx=10, pady=10)

tk.Label(tab2, text="Ваше имя:").grid(row=1, column=0, padx=10, pady=10)
entry_author = tk.Entry(tab2)
entry_author.grid(row=1, column=1, padx=10, pady=10)

tk.Label(tab2, text="Комментарий:").grid(row=2, column=0, padx=10, pady=10)
entry_comment = tk.Entry(tab2)
entry_comment.grid(row=2, column=1, padx=10, pady=10)

tk.Button(tab2, text="Загрузить файл", command=upload_file).grid(row=3, column=1, padx=10, pady=20)

# Вкладка "Экспорт"
tk.Label(tab3, text="Выберите директорию для экспорта:").grid(row=0, column=0, padx=10, pady=10)
entry_output_dir = tk.Entry(tab3, width=40)
entry_output_dir.grid(row=0, column=1, padx=10, pady=10)
tk.Button(tab3, text="Обзор", command=browse_output_directory).grid(row=0, column=2, padx=10, pady=10)

# Создание выпадающего списка для методов GIS
tk.Label(tab3, text="Методы GIS:").grid(row=1, column=0, padx=10, pady=10)
gis_methods_listbox = tk.Listbox(tab3, width=40, height=5, selectmode='multiple')
gis_methods_listbox.grid(row=1, column=1, padx=10, pady=10)

# Создание выпадающего списка для номеров скважин
tk.Label(tab3, text="Номера скважин:").grid(row=2, column=0, padx=10, pady=10)
well_numbers_listbox = tk.Listbox(tab3, width=40, height=5, selectmode='multiple')
well_numbers_listbox.grid(row=2, column=1, padx=10, pady=10)

# Создание выпадающего списка для имен полей
tk.Label(tab3, text="Имена полей:").grid(row=3, column=0, padx=10, pady=10)
field_names_listbox = tk.Listbox(tab3, width=40, height=5, selectmode='multiple')
field_names_listbox.grid(row=3, column=1, padx=10, pady=10)

tk.Label(tab3, text="Интервалы GIS (через запятую):").grid(row=4, column=0, padx=10, pady=10)
entry_gis_intervals = tk.Entry(tab3)
entry_gis_intervals.grid(row=4, column=1, padx=10, pady=10)

tk.Button(tab3, text="Экспортировать файлы", command=export_files).grid(row=5, column=1, padx=10, pady=20)

# Запуск главного цикла
root.mainloop()