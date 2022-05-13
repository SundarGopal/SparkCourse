from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("AmountSpent")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

lines = sc.textFile("customer-orders.csv")
#customerAmount = lines.map(lambda x: '('+x.split(',')[0]+','+x.split(',')[2]+')')
#customerAmount = map(lambda x: (int(x.split(',')[0]),float(x.split(',')[2])),lines)
customerAmount = lines.map(extractCustomerPricePairs)
output = customerAmount.collect()
for p in output:
    print(p)
totalByCustomer = customerAmount.reduceByKey(lambda x, y:x+y)
totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()
output = totalByCustomerSorted.collect()
for p in output:
    print(p)
# results = totalByCustomer.collect();

# for r in results:
#     print(r)


