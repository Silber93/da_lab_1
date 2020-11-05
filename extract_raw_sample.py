import random

FILE_MAIN = '/datashare/busFile'
SIZE = 237143372

proportion = 1 / float(1000)



sample_size = int(SIZE * proportion)
print(sample_size)

print("starting")

sampled_idx = sorted(random.sample(range(SIZE), sample_size))
print(sampled_idx)

sampled_lines = []
j = 0

with open(FILE_MAIN) as f:
    for i, line in enumerate(f):
        if i % 1000000 == 0:
            print(round(100 * (i / SIZE), 3), '%')
        if i == sampled_idx[j]:
            sampled_lines.append(line)
            j += 1

with open('sampled_random.txt', 'w') as f:
    for i, line in enumerate(sampled_lines):
        if i % 10000 == 0:
            print(round(100 * (i / sample_size), 3), '%')
        f.write(line)
