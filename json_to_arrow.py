# pyright: basic

import sys

from pyarrow_utils import read_json, write_feather

if __name__ == "__main__":
    input_filename = sys.argv[1] if len(sys.argv) > 1 else "/dev/stdin"
    output_filename = sys.argv[2] if len(sys.argv) > 2 else "/dev/stdout"
    write_feather(read_json(input_filename), output_filename)
