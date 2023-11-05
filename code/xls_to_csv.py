"""
Convert data from xlsx to csv

Usage: python3 xls_to_csv.py <file.xlsx> <file2.xlsx> <file3.xlsx> ...
"""

import pandas as pd
import sys
import os

def read_write_to_csv(file):
    """
    Convert xls to csv
    """
    abspath = os.path.abspath(file)
    outname = abspath.replace('.xlsx', '.csv')
    pd.read_excel(file).to_csv(outname, index=False)

if __name__ == '__main__':
    files = sys.argv[1:]
    for file in files:
        read_write_to_csv(file)
    
