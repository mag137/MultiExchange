import pstats

# Загружаем файл профиля
stats = pstats.Stats("profile.out")

# Убираем лишние пути (короткие имена файлов)
stats.strip_dirs()

# Сортировка по суммарному времени (cumulative time)
stats.sort_stats("cumulative")

# Печать топ-20 функций только из папки modules
stats.print_stats("modules/")  # фильтр по пути
stats.print_stats(20)  # первые 20 записей