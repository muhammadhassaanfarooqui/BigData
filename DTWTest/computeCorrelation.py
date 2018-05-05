from dtw import dtw
import math
import numpy as np
from random import *
from scipy.spatial.distance import euclidean
from scipy import stats
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
    array1 = readFile(fname1).reshape(-1, 1)
    array2 = readFile(fname2).reshape(-1, 1)
    distanceXY, _ = fastdtw(stats.zscore(array1), stats.zscore(array2), dist=lambda x, y: np.linalg.norm(x - y, ord=1))
    distanceX0, _ = fastdtw(stats.zscore(array1), createZeroArray(len(array1)), dist=lambda x, y: np.linalg.norm(x - y, ord=1))
    distance0Y, _ = fastdtw(createZeroArray(len(array2)), stats.zscore(array2), dist=lambda x, y: np.linalg.norm(x - y, ord=1))
    return (1 - (distanceXY/(distanceX0 + distance0Y)))

def main():
    hourlyTemperatureFile = "../alligned-files/hourly-average-temperature/hourly-average-temperature-jan2016.out"
    hourlyAvgTripDistFile = "../alligned-files/hourly-avg-trip-distance/hourly-avg-trip-distance-Jan2016.out"
    hourlyTripCountFile = "../alligned-files/hourly-taxi-trip-count/hourlyTripCountJan2016.out" 
    hourlyAvgFareFile = "../alligned-files/hourly-avg-fare/hourly-avg-fare-Jan2016.out"
    hourlyAvgTollFile = "../alligned-files/hourly-avg-toll/hourly-avg-toll-Jan2016.out"
    print("hourlyAvgTripDistance - hourlyAverageTemperature")
    print(computeDistance(hourlyAvgTripDistFile, hourlyTemperatureFile))
    print("hourlyTripCount - hourlyAverageTemperature")
    print(computeDistance(hourlyTripCountFile, hourlyTemperatureFile))
    print("hourlyAvgFare - hourlyAverageTemperature")
    print(computeDistance(hourlyAvgFareFile, hourlyTemperatureFile))
    print("hourlyAvgToll - hourlyAverageTemperature")
    print(computeDistance(hourlyAvgTollFile, hourlyTemperatureFile))
main()
