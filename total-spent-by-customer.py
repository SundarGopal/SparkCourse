from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
output = mappedInput.collect()
for p in output:
    print(p)
# totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

# results = totalByCustomer.collect();
# for result in results:
#     print(result)
