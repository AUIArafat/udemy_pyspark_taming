from pyspark import  SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def parseLine(lines):
    fields = lines.split(",")
    customer_id = fields[0]
    amount = fields[2]
    return (int(customer_id), float(amount))

lines = sc.textFile("/home/bjit-557/BigData/February/SparkCourses/20-02-2019/Section2/Assignment1/customer-orders.csv")
parsedLine = lines.map(parseLine)
results = parsedLine.reduceByKey(lambda x,y: x+y)
resultsSorted = results.map(lambda x:(x[1], x[0])).sortByKey().map(lambda x:(x[1], x[0]))
resultsSorted = resultsSorted.collect()
for result in resultsSorted:
    print(result)