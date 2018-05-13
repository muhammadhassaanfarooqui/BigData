import matplotlib.pyplot as plt
import math
import numpy as np
from random import *
from scipy.spatial.distance import euclidean
from scipy import stats
from fastdtw import fastdtw
import sys

FILE_DIRECTORY_PREFIX = "../../Aggregated Data/"

def readFile(fname):
    try:
        with open(fname) as f:
            content = f.readlines()
    except:
        raise IOError("File not found")

    temp = []
    for x in content:
        val = x.strip().split('\t')
        if("2017-01" in val[0]):
            temp.append(val[1].split(','))
    # temp = [x.strip().split('\t')[1].split(',') for x in content]
    numColumns = len(temp[0])
    arrays = [np.array([])] * numColumns
    for i in range(numColumns):
        arrays[i] = np.array([float(x[i]) for x in temp])
    return arrays

def plot(fname1, fname2):
    file1Arrays = readFile(fname1)
    file2Arrays = readFile(fname2)
    for arr1 in file1Arrays:
        for arr2 in file2Arrays:
            plt.plot(stats.zscore(arr1.reshape(-1, 1)), color="red", linewidth=0.8)
            plt.plot(stats.zscore(arr2.reshape(-1, 1)), color="blue", linewidth=1.0)
            plt.show()


def usage():
    print("Give two files inside Aggregated Data as arguments e.g Weather/hourly-average-temperature/hourly-avg-temp.out\nInvalid number of files given also give labels for graph")


def main():
    if(len(sys.argv) == 3):
        fname1 = FILE_DIRECTORY_PREFIX + sys.argv[1]
        fname2 = FILE_DIRECTORY_PREFIX + sys.argv[2]
        # label1 = sys.argv[3]
        # label2 = sys.argv[4]
        plot(fname1, fname2)
    else:
        usage()


if __name__ == '__main__':
    main()
