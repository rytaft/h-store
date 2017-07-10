import sys
from os.path import join

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: " + sys.argv[0] + " <data_path> <ticks_per_file> <initial_rate>")
        sys.exit(1)

    data_path = sys.argv[1]
    ticks_per_file = int(sys.argv[2])
    rate = float(sys.argv[3])

    tick = 0
    file_id = 1
    out = open(join(data_path, "rates-" + data_path + "-" + str(file_id).zfill(2) + ".txt"), 'w')
    out.write(str(rate) + "\n")
    with open(join(data_path, "rates-" + data_path + ".txt")) as f:
        for line in f:
            if tick >= ticks_per_file:
                out.close()
                tick = 0
                file_id += 1
                out = open(join(data_path, "rates-" + data_path + "-" + str(file_id).zfill(2) + ".txt"), 'w')
                out.write(str(rate) + "\n")
                
            out.write(line)
            rate *= float(line)
            tick += 1

        out.close()
        f.close()
