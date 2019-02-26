from pyspark import SparkConf, SparkContext
import collections

sc = SparkContext.getOrCreate()

lines = sc.textFile("/home/bjit-557/BigData/February/SparkCourses/18-02-2019/Section1/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
print(ratings);
result = ratings.countByValue()
print(result)
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
