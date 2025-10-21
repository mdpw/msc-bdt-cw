from pyspark import SparkContext

sc = SparkContext("spark://spark-master:7077", "InDegreeComputation")

edges = sc.textFile("hdfs://namenode:9000/pokec/soc-pokec-relationships.txt")

# Assuming each line: source<TAB>destination
indegree = (
    edges
    .map(lambda line: line.split('\t')[1])
    .map(lambda dest: (dest, 1))
    .reduceByKey(lambda a, b: a + b)
)

indegree.saveAsTextFile("hdfs://namenode:9000/pokec/indegree-spark")
sc.stop()
