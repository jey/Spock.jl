using Spock
using Base.Test

sc = SparkContext()

rdd1 = parallelize(sc, 1:10)
@assert count(rdd1) == 10
@assert sum(collect(rdd1)) == 55
@assert reduce(+, rdd1) == 55

rdd2 = map(x -> x^2, rdd1)
@assert sum(collect(rdd2)) == 385
@assert reduce(+, rdd2) == 385

@assert "moo" == begin
  try
    collect(map(x->throw("woof"), rdd1))
    "oink"
  catch exc
    @assert contains(exc.msg, "JuliaException")
    "moo"
  end
end

println("Spock: all tests passed")
