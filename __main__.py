import argparse
import os
import sys

parser = argparse.ArgumentParser(description="Run Wikidata bot.")
parser.add_argument("bot", action="store")
args = parser.parse_args()

filename = os.path.realpath(__file__)
dirname = os.path.dirname(filename)
bot_filename = os.path.join(dirname, "{}.py".format(args.bot))

os.execv(sys.executable, [sys.executable, bot_filename])
