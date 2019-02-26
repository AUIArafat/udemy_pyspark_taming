from pyspark import SparkContext, SparkConf


def loadMoviNames():
    movieNames = {}
    with open("/home/bjit-557/BigData/February/SparkCourses/22-02-2019/Section3/ml-100k/u.item", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMoviesNicer")
sc = SparkContext(conf = conf)
movieNames = sc.broadcast(loadMoviNames());

print(movieNames)

lines = sc.textFile("/home/bjit-557/BigData/February/SparkCourses/22-02-2019/Section3/ml-100k/u.data")
movies = lines.map(lambda x:(int(x.split()[1]),1))

movieCounts = movies.reduceByKey(lambda x,y: x+y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey();

finalMovies = sortedMovies.map(lambda x: (movieNames.value[x[1]], x[0]))
finalMovies.foreach(lambda x: print(x))