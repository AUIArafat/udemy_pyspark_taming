from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

lines = sc.textFile("/home/bjit-557/BigData/February/SparkCourses/22-02-2019/Section3/ml-100k/u.data")
lines.foreach(lambda x:print(x))
print("---------------------------------------------")
movies = lines.map(lambda x: (int(x.split()[1]),1))
movies.foreach(lambda x: print(x))
print("---------------------------------------------")
movieCounts = movies.reduceByKey(lambda x,y:x+y)
movieCounts.foreach(lambda x:print(x))
print("---------------------------------------------")
flippedMovies = movieCounts.map(lambda x: (x[1], x[0]))
flippedMovies.foreach(lambda x: print(x))
print("---------------------------------------------")
sortedMovies = flippedMovies.sortByKey()
sortedMovies.foreach(lambda x: print(x))
print("---------------------------------------------")
results = sortedMovies.collect()
for result in results:
    print("Movie Id: {0} And Number Of Watched: {1}".format(result[1], result[0]))