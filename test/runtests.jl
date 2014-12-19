using Spock
using Base.Test

sc = SparkContext()

rdd1 = parallelize(sc, 1:10, 2)
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

let driverpid = getpid()
  @assert 55 == reduce(rdd1) do x, y
    if getpid() != driverpid
      println("Ekke Ekke Ekke Ekke Ptangya Zoooooooom Boing Ni!")
      @assert eof(STDIN)
    end
    x + y
  end

  rdd3 = map(x->getpid(), rdd1)
  @assert 2 == length(Set(collect(rdd3)))
  @assert driverpid == reduce(rdd3) do l, r
    mypid = getpid()
    @assert mypid == driverpid || (mypid == l && mypid == r)
    mypid
  end
end

println("Spock: all tests passed")
