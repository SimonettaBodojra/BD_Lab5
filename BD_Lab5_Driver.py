import findspark
import re

findspark.init()

from pyspark import SparkConf, SparkContext

def computeMax (u,v):
    if u > v:
        return u
    return v

conf = SparkConf().setAppName("ApplicationLa5")
sc = SparkContext(conf = conf)

inputRDD = sc.textFile("resGatewayAmazon.txt")

filteredRDD = inputRDD.filter(lambda line: str(line).startswith("ho"))
numberOfLines = filteredRDD.count()

frequenciesRDD = filteredRDD.map(lambda line: int(re.split("\\s", line)[1]))
#maxFrequency = frequenciesRDD.reduce(computeMax)
maxFrequency = frequenciesRDD.reduce(lambda n1, n2: max(n1,n2))

print(numberOfLines)
print(maxFrequency)

mostFrequentRDD = filteredRDD.filter(lambda line: int(re.split("\\s", line)[1]) > 0.8*maxFrequency)
numberOfLinesFrequent = mostFrequentRDD.count()
print(numberOfLinesFrequent)

mostFrequentWordsRDD = mostFrequentRDD.map(lambda line: str(re.split("\\s",line)[0]))
mostFrequentWordsRDD.saveAsTextFile("mostFrequentWord")


sc.stop()
