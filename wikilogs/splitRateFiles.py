import sys
from os.path import join

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: " + sys.argv[0] + " <data_path> <ticks_per_file>")
        sys.exit(1)

    data_path = sys.argv[1]
    ticks_per_file = int(sys.argv[2])

    tick = 0
    file_id = 1
    out = open(join(data_path, "rates-" + data_path + "-" + str(file_id).zfill(2) + ".txt"), 'w')
    with open(join(data_path, "rates-" + data_path + ".txt")) as f:
        for line in f:
            if tick >= ticks_per_file:
                out.close()
                tick = 0
                file_id += 1
                out = open(join(data_path, "rates-" + data_path + "-" + str(file_id).zfill(2) + ".txt"), 'w')

            out.write(line)
            tick += 1

        out.close()
        f.close()
