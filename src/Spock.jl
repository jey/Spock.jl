include("worker.jl")

module Spock
  using SpockWorker
  using JavaCall
  import Base: Callable, map, collect, convert, count, reduce
  export SparkContext, RDD, parallelize, transform

  function compute_classpath()
    cp = get(ENV, "CLASSPATH", "")
    confdir = Pkg.dir("Spock", "conf")
    spockjar = Pkg.dir("Spock", "lib", "spock.jar")
    sparkjar = Pkg.dir("Spock", "lib", "spark.jar")
    "$cp:$confdir:$spockjar:$sparkjar"
  end

  JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$(compute_classpath())"])

  JClass = @jimport java.lang.Class
  JArrays = @jimport java.util.Arrays
  JList = @jimport java.util.List
  JFunction2 = @jimport org.apache.spark.api.java.function.Function2
  JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
  JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
  JJuliaFunction = @jimport edu.berkeley.bids.spock.JuliaFunction
  JJuliaObject = @jimport edu.berkeley.bids.spock.JuliaObject

  spockid() = "$(gethostname())[$(getipaddr())]/$(getpid())"

  type SparkContext
    jsc::JJavaSparkContext
  end

  function SparkContext(master::String="local", appname::String="Julia Spock app @ $(spockid())")
    SparkContext(JJavaSparkContext((JString, JString), master, appname))
  end

  abstract RDD

  type JavaRDD <: RDD
    jrdd::JJavaRDD
  end

  jrdd(rdd::RDD) = rdd.jrdd

  type TransformedRDD <: RDD
    parent::RDD
    task::Function
    jrdd::Union(Nothing,JJavaRDD)
  end

  TransformedRDD(parent, task) = TransformedRDD(parent, task, nothing)

  ispipelineable(rdd::TransformedRDD) = true

  function jrdd(rdd::TransformedRDD)
    if rdd.jrdd === nothing
      jfunc = JJuliaFunction((JJuliaObject,), jbox(rdd.task))
      rdd.jrdd = jcall(jrdd(rdd.parent), "mapPartitionsWithIndex", JJavaRDD, (JFunction2, jboolean), jfunc, false)
    end
    rdd.jrdd::JJavaRDD
  end

  function parallelize(sc::SparkContext, collection)
    jcoll = convert(JList, collect(map(jbox, collection)))
    JavaRDD(jcall(sc.jsc, "parallelize", JJavaRDD, (JList,), jcoll))
  end

  function parallelize(sc::SparkContext, collection, numparts)
    jcoll = convert(JList, collect(map(jbox, collection)))
    JavaRDD(jcall(sc.jsc, "parallelize", JJavaRDD, (JList, jint), jcoll, numparts))
  end

  function jbox(obj)
    buf = IOBuffer()
    serialize(buf, obj)
    JJuliaObject((Vector{jbyte},), takebuf_array(buf))
  end

  function junbox(jobj)
    jobj = convert(JJuliaObject, jobj)
    payload = uint8(jcall(jobj, "getPayload", Vector{jbyte}, ()))
    deserialize(IOBuffer(payload))
  end

  # Analogous to `mapPartitionsWithIndex`.
  #
  # `task` will be called once per input partition with arguments of
  # (partition_id, input_iter) and returns an iterable of new contents.
  function transform(task::Function, rdd::RDD)
    if isa(rdd, TransformedRDD) && ispipelineable(rdd)
      TransformedRDD(rdd.parent, pipetask(task, rdd.task))
    else
      TransformedRDD(rdd, task)
    end
  end

  function map(f::Callable, rdd::RDD)
    transform(maptask(f), rdd)
  end

  function reduce(f::Callable, rdd::RDD)
    reduce(f, collect(transform(reducetask(f), rdd)))
  end

  function collect(rdd::RDD)
    jlist = jcall(jrdd(rdd), "collect", JList, ())
    map(junbox, jcall(jlist, "toArray", Vector{JObject}, ()))
  end

  function count(rdd::RDD)
    jcall(jrdd(rdd), "count", jlong, ())
  end

  function convert(::Type{JList}, A::Array)
    jcall(JArrays, "asList", JList, (Vector{JObject},), A)
  end

  classname(jobj::JavaObject) = jcall(jcall(jobj, "getClass", JClass, ()), "toString", JString, ())
end
