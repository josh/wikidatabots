# pyright: basic

import sys

from pyarrow_utils import fill_ids, read_table, write_feather

if __name__ == "__main__":
    input_filename = sys.argv[1] if len(sys.argv) > 1 else "/dev/stdin"
    output_filename = sys.argv[2] if len(sys.argv) > 2 else "/dev/stdout"

    table = read_table(input_filename)
    table = fill_ids(table)
    write_feather(table, output_filename)
