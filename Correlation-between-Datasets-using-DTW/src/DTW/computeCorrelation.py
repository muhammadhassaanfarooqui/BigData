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
    temp = [x.strip().split('\t')[1].split(',') for x in content]
    numColumns = len(temp[0])
    arrays = [np.array([])] * numColumns
    for i in range(numColumns):
        arrays[i] = np.array([float(x[i]) for x in temp])
    return arrays
    #return np.array([float(x[1]) for x in temp])

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
            distanceXY, _ = fastdtw(stats.zscore(array1), stats.zscore(array2))#, dist=lambda x, y: np.linalg.norm(x - y, ord=1))
            distanceX0, _ = fastdtw(stats.zscore(array1), createZeroArray(zeroArraySize))#, dist=lambda x, y: np.linalg.norm(x - y, ord=1))
            distanceY0, _ = fastdtw(stats.zscore(array2), createZeroArray(zeroArraySize))#, dist=lambda x, y: np.linalg.norm(x - y, ord=1))
            distances.append((1 - (distanceXY/(distanceX0 + distanceY0))))
    return distances

def main():
    hourlyTemperatureJanuaryFile = "../../Aggregated Data/Weather/hourly-average-temperature/hourly-average-temperature-jan2016.out"
    hourlyTemperatureFile = "../../Aggregated Data/Weather/hourly-average-temperature/hourly-avg-temp.out"
    hourlyAvgTripDistFile = "../../Aggregated Data/Taxi/hourly-avg-trip-distance/hourly-avg-trip-distance-Jan2016.out"
    hourlyTripCountFile = "../../Aggregated Data/Taxi/hourly-taxi-trip-count/hourlyTripCountJan2016.out"
    hourlyAvgFareFile = "../../Aggregated Data/Taxi/hourly-avg-fare/hourly-avg-fare-Jan2016.out"
    hourlyAvgTollFile = "../../Aggregated Data/Taxi/hourly-avg-toll/hourly-avg-toll-Jan2016.out"
    hourlyTaxiDataFile = "../../Aggregated Data/Taxi/hourly-taxi-data/hourly-avg-taxiData.out"
    workhoursFile = "../../Aggregated Data/Weather/hourly-average-temperature/hourly-workhours-avg-temp.out"
    taxitempFile = "../../Aggregated Data/hourly-taxi-data/hourly-workhours-avg-taxiData.out"
    print("hourlyAvgTripDistance - hourlyAverageTemperature")
    print(computeDistance(hourlyAvgTripDistFile, hourlyTemperatureJanuaryFile))
    print("hourlyTripCount - hourlyAverageTemperature")
    print(computeDistance(hourlyTripCountFile, hourlyTemperatureJanuaryFile))
    print("hourlyAvgFare - hourlyAverageTemperature")
    print(computeDistance(hourlyAvgFareFile, hourlyTemperatureJanuaryFile))
    print("hourlyAvgToll - hourlyAverageTemperature")
    print(computeDistance(hourlyAvgTollFile, hourlyTemperatureJanuaryFile))
    print("hourlyTaxiData - hourlyAverageTemperature")
    print(computeDistance(hourlyTaxiDataFile, hourlyTemperatureFile))
    # print("workhours - taxiTemp")
    # print(computeDistance(workhoursFile, taxitempFile))
main()
