#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
# import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)


def demo():
  for f in dfs.files():
    print("File {0} consists of fragments {1}".format(f.name, f.chunks))

  for c in dfs.chunk_locations():
    print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
  chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])


# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
    for file in dfs.files():
        if file.name == filename:
            for chunk in dfs.chunk_locations():
                if chunk.id in file.chunks:
                    for line in dfs.get_chunk_data(chunk.chunkserver, chunk.id):
                        yield line.strip()


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    sum = 0
    for key_line in get_file_content(keys_filename):
        for line in get_file_content("/partitions"):
            if not line:
                continue
            key_1, key_2, shard = line.split()
            if key_1 <= key_line and key_2 >= key_line:
                for chunk in get_file_content(shard):
                    if not chunk:
                        continue
                    key, val = chunk.split()
                    if key == key_line:
                        sum += int(val)
    return sum


# demo()

print(calculate_sum("/keys")