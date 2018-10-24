# PURPOSE: Using CLI, run simple spark script to print file contents

###########################
# FUNCTIONS
def print_funct(line):
    print(line)
    return line
###########################
# MODULES

import os
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
###########################
# PATHING

data_path = '/home/jovyan/work/data/'
data_filename = 'some_text.txt'

data_path_full = os.path.join(data_path, data_filename)
###########################
# DATA IMPORT

data = sc.textFile(data_path_full)

###########################
# PRINT RESULTS

data.map(lambda l: print_funct(l)).collect()