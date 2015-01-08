using Spock
using Base.Test

sc = SparkContext()
rdd1 = parallelize(sc, 1:10, 2)
rdd2 = map(x -> x^2, rdd1)
rddx = parallelize(sc, fill("x", 10), 2)
allequal(val, rdd) = all(x -> x == val, collect(rdd))

# test basics
@assert 10 == count(rdd1)
@assert 55 == sum(collect(rdd1))
@assert 55 == reduce(+, rdd1)
@assert 385 == sum(collect(rdd2))
@assert 385 == reduce(+, rdd2)

# test propagation of exceptions from workers
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
  # test containment of worker I/O
  @assert 55 == reduce(rdd1) do x, y
    if getpid() != driverpid
      println("Ekke Ekke Ekke Ekke Ptangya Zoooooooom Boing Ni!")
      @assert eof(STDIN)
    end
    x + y
  end

  # test pipelining
  pids = map(x->getpid(), rdd1)
  @assert 2 == length(Set(collect(pids)))
  @assert driverpid == reduce(pids) do l, r
    mypid = getpid()
    @assert mypid == driverpid || (mypid == l && mypid == r)
    mypid
  end
end

# test mixing synchronous and asynchronous transforms
frob = (partid, iter) -> ["f$(x)" for x in collect(iter)]
brof = (partid, iter) -> begin
  Task() do
    for x in iter
      produce("b$(x)")
    end
  end
end
@assert allequal("fbfbx", transform(frob, transform(brof, transform(frob, transform(brof, rddx)))))
@assert allequal("bfbfx", transform(brof, transform(frob, transform(brof, transform(frob, rddx)))))
@assert allequal("ffbfx", transform(frob, transform(frob, transform(brof, transform(frob, rddx)))))
@assert allequal("fbbfx", transform(frob, transform(brof, transform(brof, transform(frob, rddx)))))
@assert allequal("bbfbx", transform(brof, transform(brof, transform(frob, transform(brof, rddx)))))
@assert allequal("bffbx", transform(brof, transform(frob, transform(frob, transform(brof, rddx)))))
partids = collect(transform((partid, iter) -> [partid], rddx))
@assert length(partids) == 2
@assert Set([0, 1]) == Set(partids)

println("Spock: all tests passed")
