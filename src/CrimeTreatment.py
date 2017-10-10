from pyspark import SparkContext, SparkConf
 
conf = SparkConf().setAppName("prediction-poc-feuvert").setMaster("local")
sc = SparkContext(conf=conf)
 
data = sc.textFile("file:/app/data/crimes_chicago_2012_2015.csv")	
 
# Split to separate every column
dataset = data.map(lambda l: l.split(','))
 
# Count crime number by year
# First assign 1 to each year then reduceBy year and add number.
# It is more efficient than groupBy because it doesn't need to move data over the cluster
nbCrimesByYear = dataset.map(lambda row : (row[5], 1)).reduceByKey(lambda a,b:a+b)
print("nbCrimesByYear : " + str(nbCrimesByYear.collect()))
# nbCrimesByYear : [(u'2015', 119963), (u'2013', 296459), (u'2014', 264960)]
 
# Count crime number for crimes including vehicle
# First filter on description including pattern then count
nbCrimesTypeVehicle = dataset.filter(lambda row : "VEHICLE" in row[1])
print("nbCrimesTypeVehicle : " + str(nbCrimesTypeVehicle.count()))
# nbCrimesTypeVehicle : 25806
 
# Simple fonction to make an addition
def count(a, b):
	return a+b
 
# Search the district with more crimes
dangerousDistrictSorted = dataset.map(lambda row : (row[4], 1)).reduceByKey(count).sortBy(lambda row : row[1], ascending=False)
print("dangerousDistrictSorted : " + str(dangerousDistrictSorted.first()))
# dangerousDistrictSorted : (u'011', 49630)

print("All most dangerous districts")
for x in dangerousDistrictSorted.collect():
	print x