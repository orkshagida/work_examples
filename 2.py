import numpy as np
from scipy.ndimage import label
import matplotlib.pyplot as plt


file_path = "D:\\Desktop\\123\\матрица.csv"
with open(file_path, 'r', encoding='utf-8-sig') as file:
    lines = file.readlines()
    binary_matrix = np.array([list(map(int, line.strip().split(','))) for line in lines])
    binary_matrix = (binary_matrix == 76).astype(int)
print("Бинарная матрица загружена успешно:")
print(binary_matrix)

pixel_to_mm = 0.085
width, height = 200, 200
total_image_area_mm2 = (width * pixel_to_mm) * (height * pixel_to_mm)
labeled_array, num_features = label(binary_matrix)
areas_mm2 = []
sizes_mm = []   
for label_id in range(1, num_features + 1):
    pixel_count = (labeled_array == label_id).sum()
    area_mm2 = pixel_count * (pixel_to_mm ** 2)
    areas_mm2.append(area_mm2)
    size_mm = 2 * np.sqrt(area_mm2 / np.pi)  
    sizes_mm.append(size_mm)
average_area = np.mean(areas_mm2) if areas_mm2 else 0
average_size = np.mean(sizes_mm) if sizes_mm else 0
total_area_mm2 = sum(areas_mm2)
volume_fraction_percentage = (total_area_mm2 / total_image_area_mm2) * 100

# Вывод результатов

print(f"Площади включений (мм²): {areas_mm2}")
print(f"Линейные размеры включений (мм): {sizes_mm}")
print(f"Общее количество включений: {num_features}")
print(f"Средняя площадь включения: {average_area:.4f} мм²")
print(f"Средний линейный размер включения: {average_size:.4f} мм")
print(f"Объемная доля включений: {volume_fraction_percentage:.2f}%")
fig, axes = plt.subplots(1, 2, figsize=(10, 5))

axes[0].imshow(binary_matrix, cmap='gray')
axes[0].set_title("Исходная бинарная матрица")
axes[0].axis("off")

axes[1].imshow(labeled_array, cmap='nipy_spectral')
axes[1].set_title(f"Области (всего: {num_features})")
axes[1].axis("off")

plt.tight_layout()
plt.show()   



