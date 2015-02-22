#!/usr/bin/python
# encoding: utf8

# for local testing
# import test_dfs as dfs

# for work with real environment
import http_dfs as dfs

from collections import defaultdict


def get_file_content(filename):
    chunk_ids = [f.chunks for f in files_cache if f.name == filename]
    if not chunk_ids:
        raise Exception("file %s were not found" % filename)
    for chunk_id in chunk_ids[0]:
        chunk_server = [l.chunkserver for l in locations_cache if l.id == chunk_id][0]
        for line in dfs.get_chunk_data(chunk_server, chunk_id):
            if not line.isspace():
                yield line[:-1]


def get_filename(page_key):
    for line in partitions_cache:
        lo, hi, filename = line.split(' ')
        filename = filename.strip('/')
        if lo <= page_key <= hi:
            return filename
    raise Exception("page %s were not found" % page_key)


def get_visit_count(file, page_keys):
    ret = 0
    for line in get_file_content(file):
        curr_page_key, visit_count = line.split(' ')
        for page_key in page_keys:
            if page_key == curr_page_key:
                ret += int(visit_count)
    return ret


def calculate_sum(keys_filename):
    task_file_to_keys = defaultdict(list)
    keys = list(get_file_content(keys_filename))
    for page_key in keys:
        file = get_filename(page_key)
        task_file_to_keys[file].append(page_key)

    total = 0
    # todo: parallel
    for file in task_file_to_keys.keys():
        total += get_visit_count(file, task_file_to_keys[file])

    return total


def demo():
    print(calculate_sum("/keys"))


locations_cache = list(dfs.chunk_locations())
files_cache = list(dfs.files())
partitions_cache = list(get_file_content("/partitions"))
demo()