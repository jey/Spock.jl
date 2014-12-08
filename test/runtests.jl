using Spock
using Base.Test
sc = SparkContext()
rdd1 = parallelize(sc, 1:10)
@assert count(rdd1) == 10
@assert sum(collect(rdd1)) == 55
rdd2 = map(rdd1, x -> x^2)
dump(collect(rdd2))
@assert sum(collect(rdd2)) == 385
