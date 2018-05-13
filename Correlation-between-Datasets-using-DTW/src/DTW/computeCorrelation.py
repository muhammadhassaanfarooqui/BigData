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

    temp = [x.strip().split('\t')[1].split(',') for x in content]
    numColumns = len(temp[0])
    arrays = [np.array([])] * numColumns
    for i in range(numColumns):
        arrays[i] = np.array([float(x[i]) for x in temp])
    return arrays


def createArray(size):
    return np.array([randint(1, 10) for i in range(size)])


def createTimeSeries(size):
    ls = np.array([0])
    hours = 24
    for i in range(size-1):
        ls = np.append(ls, i%hours)
    return ls


def normalize(array):
    minVal = min(array)[0]
    maxVal = max(array)[0]
    temp = array
    if(minVal < 0):
        temp = array + abs(minVal)
        temp /= (abs(minVal) + maxVal)
        return temp
    else:
        return temp / maxVal

def createZeroArray(size):
    return np.array([0]*size).reshape(-1, 1)


def computeDistance(fname1, fname2):
    file1Arrays = readFile(fname1)
    file2Arrays = readFile(fname2)
    distances = []
    for arr1 in file1Arrays:
        for arr2 in file2Arrays:
            array1 = arr1.reshape(-1, 1)
            array2 = arr2.reshape(-1, 1)
            zeroArraySize = max(len(array2), len(array1))
            distanceXY, _ = fastdtw(stats.zscore(array1), stats.zscore(array2))
            distanceX0, _ = fastdtw(stats.zscore(array1), createZeroArray(zeroArraySize))
            distanceY0, _ = fastdtw(stats.zscore(array2), createZeroArray(zeroArraySize))
            distances.append((1 - (distanceXY/(distanceX0 + distanceY0))))
    return distances


def usage():
    print("Give two files inside Aggregated Data as arguments e.g Weather/hourly-average-temperature/hourly-avg-temp.out\nInvalid number of files given")


def main():
    if(len(sys.argv) == 3):
        fname1 = FILE_DIRECTORY_PREFIX + sys.argv[1]
        fname2 = FILE_DIRECTORY_PREFIX + sys.argv[2]
        print("Correlation among the two datasets is: %s on a range of 0 - 1" % (computeDistance(fname1, fname2)))
        # plt.plot([1, 2], [1, 2])
        # plt.show()
    else:
        usage()


if __name__ == '__main__':
    main()
