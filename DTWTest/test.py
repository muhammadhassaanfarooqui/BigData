from dtw import dtw
import math
import numpy as np
from random import *
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw


def readFile(fname):
    with open(fname) as f:
        content = f.readlines()
    temp = [x.strip().split('\t') for x in content]
    return np.array([float(x[1]) for x in temp])

def createArray(size):
    return np.array([randint(1, 10) for i in range(size)])

def createTimeSeries(size):
    ls = np.array([0])
    hours = 24
    for i in range(size-1):
        ls = np.append(ls, i%hours)
    return ls

def main():
    array1 = readFile("../alligned-files/hourly-taxi-trip-count/hourlyTripCountJan2016.out").reshape(-1, 1)
    array2 = readFile("../alligned-files/hourly-average-temperature/hourly-average-temperature-jan2016.out").reshape(-1, 1)
    distance, path = fastdtw(normalize(array1), normalize(array2), dist=lambda x, y: np.linalg.norm(x - y, ord=1))
    print(distance/len(array1))

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


main()
