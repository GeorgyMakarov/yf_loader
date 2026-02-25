import sys
import argparse

def read_args(arg_list, logger):
  parser = argparse.ArgumentParser(description="Process some command-line arguments.")
  for a in arg_list:
    parser.add_argument('--' + a, type=str)
  try:
    args = parser.parse_args()
    return args
  except SystemExit as e:
    raise ValueError(f"Error parsing arguments: {e}")
