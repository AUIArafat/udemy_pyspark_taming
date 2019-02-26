from pyspark import  SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("MostPopularSuperhero")
sc = SparkContext(conf=conf)

def parseLine(lines):
    fields = lines.split("\"");
    return (int(fields[0]), fields[1])
def CountCoOccurence(lines):
    fields = lines.split()
    return (int(fields[0]), len(fields)-1)
names = sc.textFile("/home/bjit-557/BigData/February/SparkCourses/22-02-2019/Section3/Marvel-Names.txt")
namesRdd = names.map(parseLine)
lines = sc.textFile("/home/bjit-557/BigData/February/SparkCourses/22-02-2019/Section3/Marvel-Graph.txt")
pairings = lines.map(CountCoOccurence)
pairings.foreach(lambda x:print(x))
totalFriendsByHero = pairings.reduceByKey(lambda x,y:x+y)
totalFriendsByHero.foreach(lambda x:print(x))
flipped = totalFriendsByHero.map(lambda x:(x[1],x[0]))
flipped.foreach(lambda x:print(x))
mostPopular = flipped.max()
print(mostPopular)

mostPopularName = namesRdd.lookup(mostPopular[1])[0]
print(mostPopularName + " is most popular hero with " + str(mostPopular[0]) + " CoOccurancs")