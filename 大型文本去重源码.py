import os
import heapq
import time
from tqdm import tqdm


# 读取大文件，分块处理去重并排序
def read_large_file_in_chunks(file_path, chunk_size=100000):
    with open(file_path, 'r', encoding='latin-1', errors='ignore') as file:
        lines = []
        for line in file:
            lines.append(line.strip())
            if len(lines) >= chunk_size:
                yield lines
                lines = []
        if lines:
            yield lines


# 去重并排序后写入临时文件
def process_chunk(lines, temp_dir, chunk_index):
    unique_lines = sorted(set(lines))
    temp_file_path = os.path.join(temp_dir, f'temp_chunk_{chunk_index}.txt')
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        temp_file.write('\n'.join(unique_lines) + '\n')
    return temp_file_path


# 归并排序临时文件并去重
def merge_sorted_files(temp_files, output_file_path):
    open_files = [open(file, 'r', encoding='utf-8') for file in temp_files]
    pq = []

    for file_index, file in enumerate(open_files):
        line = file.readline().strip()
        if line:
            heapq.heappush(pq, (line, file_index))

    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        prev_line = None
        while pq:
            current_line, file_index = heapq.heappop(pq)
            if current_line != prev_line:
                output_file.write(current_line + '\n')
                prev_line = current_line
            next_line = open_files[file_index].readline().strip()
            if next_line:
                heapq.heappush(pq, (next_line, file_index))

    for file in open_files:
        file.close()


def count_lines(file_path):
    with open(file_path, 'r', encoding='latin-1', errors='ignore') as file:
        return sum(1 for line in file)


def remove_duplicates(input_file_path, output_file_path, temp_dir, chunk_size=100000):
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    start_time = time.time()
    total_lines = count_lines(input_file_path)
    chunk_index = 0
    temp_files = []

    print("Reading and processing chunks...")
    with tqdm(total=total_lines, desc="Processing chunks") as pbar:
        for chunk in read_large_file_in_chunks(input_file_path, chunk_size):
            temp_file_path = process_chunk(chunk, temp_dir, chunk_index)
            temp_files.append(temp_file_path)
            chunk_index += 1
            pbar.update(len(chunk))

    print("Merging sorted chunks...")
    merge_start_time = time.time()
    merge_sorted_files(temp_files, output_file_path)

    for temp_file in temp_files:
        os.remove(temp_file)

    end_time = time.time()
    print(f"Process completed successfully in {end_time - start_time:.2f} seconds.")
    print(f"Time taken for merging: {end_time - merge_start_time:.2f} seconds.")


# 使用上述函数进行去重
input_file_path = r'D:\zd\small\input.txt'
output_file_path = r'D:\zd\small\output.txt'
temp_dir = r'D:\zd\small\temp'

remove_duplicates(input_file_path, output_file_path, temp_dir)
