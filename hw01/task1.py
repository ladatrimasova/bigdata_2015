#!/usr/bin/python
# encoding: utf8

import http_dfs as dfs

files = dfs.files() # получаем содержимое /files чтобы не обращаться каждый раз к нему через dfs   
chunk_locations = dfs.chunk_locations() # получаем содержимое /chunk_locations чтобы не обращаться каждый раз к нему через dfs        

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
def get_file_content(chunkserver, chunk_name):
  for line in dfs.get_chunk_data(chunkserver, chunk_name):
      yield line      
    
def get_chunks(shard_name):
  for f in files:
      if f.name == shard_name[1:]: # shard_name начинается с символа '/'
          return f.chunks
  raise Exception("Chunk for shard: {0} not found".format(shard_name[1:]))       
         
def get_server_for_chunk(chunk):  
  for c in chunk_locations:
      if c.id == chunk:
          return c.chunkserver
  raise Exception("Chunkserver for chunk: {0} not found".format(chunk))                 
       
def calculate_sum(key_chunk_name, chunkserver):
  result = 0  
  for keys in get_file_content(chunkserver, key_chunk_name):
      key = keys[:-1] # удаляем символ перевода строки
      partitions = () 
      for f in files: # ищем где chunks для /partitions
          if f.name == "/partitions":
              partitions = f.chunks
              break
      for partition in partitions: # для каждого chunk файла /partitions ищем необходимый нам диапозон
          for c in chunk_locations:
              if c.id == partition:
                  for line in get_file_content(c.chunkserver, partition): 
                      if len(line[:-1]) == 0:
                          continue
                      (begin, end, shard_name) = line[:-1].split(' ')
                      if key >= begin and key <= end:
                          chunks = get_chunks(shard_name)
                          haveFoundKey = False # флаг что еще не нашли текущий ключ в одном из chunk-ов
                          for chunk in chunks:
                              server_name = get_server_for_chunk(chunk)
                              for line in get_file_content(server_name, chunk):
                                  if len(line[:-1]) == 0:
                                      continue
                                  (chunk_key, chunk_value) = line[:-1].split(' ')
                                  if key == chunk_key:
                                      result += int(chunk_value)
                                      haveFoundKey = True
                                      break
                              if haveFoundKey: # если уже нашли ключ, то не надо искать в остальных chunk-ах
                                  break
                          break # уже нашли нужный интервал
                  break # уже нашли нужную запись в chuck_location c.id == partition
  return result           
  
result_sum = 0
keys = ()
for f in files: # ищем где chunks для /keys
    if f.name == "/keys":
        keys = f.chunks
        break
for key in keys: # для каждого chunk файла /keys считаем    
    for c in chunk_locations:
        if c.id == key:
            result_sum += calculate_sum(key, c.chunkserver)
            break
print(result_sum)
