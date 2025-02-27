import csv
import re
import tkinter as tk
from tkinter import filedialog, messagebox

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

def preprocess_las_file(input_file_path, output_file_path, csv_path):
    """
    Основная функция для предобработки LAS файла с заменой мнемоник.
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

# GUI
root = tk.Tk()
root.title("Предобработка LAS-файлов с заменой мнемоник из CSV")

tk.Button(root, text="Предобработать LAS файл", command=preprocess_interface).pack(padx=10, pady=10)

root.mainloop()
