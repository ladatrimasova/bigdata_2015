#!/usr/bin/python
# encoding: utf8

import os.path
import re
import http_dfs as dfs
# import test_dfs as dfs

# Запросим содержание файлов files и chunk_locations только один раз
files = dfs.files()
chunk_locations = dfs.chunk_locations()

def get_file_chunks_ids(filename):
    '''
    Функция возращает массив айдишников чанков
    '''
    for record in files:
        if record.name == filename:
            return record.chunks


def get_chunks_info(filename):
    '''
    Функция возращает массив информации по всем чанкам файла,
    для того, чтобы знать на каком сервере находится каждый чанк
    '''
    file_chunks_ids = get_file_chunks_ids(filename)
    info = []
    for chunk_id in file_chunks_ids:
        for location in chunk_locations:
            if location.id == chunk_id:
                info.append(location)
    return info
        

def get_file_content(filename):
    '''
    Функция возращает генератор который проходит по всем строкам каждого 
    чанка файла друг за другом, тем сам получается проход по всему
    содержанию файла
    '''
    for chunk_info in get_chunks_info(filename):
        chunk_data = dfs.get_chunk_data(chunk_info.chunkserver, chunk_info.id)
        for line in chunk_data:
            yield line


def get_filenames_for_key(key):
    '''
    Поиск имен файлов, по диапазону ключей
    '''
    filenames = []
    for line in get_file_content('/partitions'):
        try:
            (min_key, max_key, filename) = line.split()
            if min_key <= key and max_key >= key:
                filenames.append(filename)
        except:
            pass
    return filenames


def calculate_sum(keys_filename):
    res = 0
    for key in get_file_content('/keys'):
        key = key[:-1]
        print('Counting for key %s' % (key))
        for filename in get_filenames_for_key(key):
            print('Looking in file %s' % (filename))
            for line in get_file_content(filename):
                # Проходим по содержанию файла и разбиваем на массив
                # из 2 значений (ключ, значение) каждую строку
                pair = line.split()
                if len(pair) == 2 and pair[0] == key:
                    res += int(pair[1])
    return res



def init():
    print('Total sum is: %s' % (calculate_sum('data/keys')))

init()
