using Spock
using Base.Test

sc = SparkContext()

rdd1 = parallelize(sc, 1:10)
@assert 10 == count(rdd1)
@assert 55 == sum(collect(rdd1))
@assert 55 == reduce(+, rdd1)

rdd2 = map(x -> x^2, rdd1)
@assert 385 == sum(collect(rdd2))
@assert 385 == reduce(+, rdd2)

@assert "moo" == begin
  try
    collect(map(x->throw("woof"), rdd1))
    "oink"
  catch exc
    @assert contains(exc.msg, "JuliaException")
    "moo"
  end
end

@assert 55 == reduce(rdd1) do x, y
  println("Ekke Ekke Ekke Ekke Ptangya Zoooooooom Boing Ni!")
  @assert eof(STDIN)
  x + y
end

println("Spock: all tests passed")
