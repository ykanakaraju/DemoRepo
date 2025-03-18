# -*- coding: utf-8 -*-

import findspark
findspark.init()
from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "ExampleCode")

filePath = "C:\\PySpark\\data\\cars.tsv" 


def seq_fn(z, e):
    return (z[0] + e, z[1] + 1)

def comb_fn(a, b):
    return (a[0] + b[0], a[1] + b[1])


output = sc.textFile(filePath) \
         .map(lambda l: l.split("\t")) \
         .filter(lambda a: a[9] == 'American' ) \
         .map(lambda a: (a[0], int(a[6])) ) \
         .aggregateByKey( (0,0), seq_fn, comb_fn) \
         .mapValues(lambda sumCount: sumCount[0]/sumCount[1]) \
         .sortBy(lambda x: x[1], False) \
         .coalesce(1)

output.saveAsTextFile("C:\\PySpark\\carsout")

