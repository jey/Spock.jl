module Spock
  using JavaCall
  import Base: Callable, map, collect, convert, count
  export SparkContext, RDD, parallelize

  const classpath = get(ENV, "CLASSPATH", "")
  JavaCall.init(["-Xmx1024M", "-Djava.class.path=spock.jar:spark.jar:$(classpath)"])

  JClass = @jimport java.lang.Class
  JArrays = @jimport java.util.Arrays
  JList = @jimport java.util.List
  JFunction = @jimport org.apache.spark.api.java.function.Function
  JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
  JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
  JJuliaRDD = @jimport edu.berkeley.cs.amplab.spock.JuliaRDD
  JJuliaFunction = @jimport edu.berkeley.cs.amplab.spock.JuliaFunction
  JJuliaObject = @jimport edu.berkeley.cs.amplab.spock.JuliaObject

  spockid() = "$(gethostname())[$(getipaddr())]/$(getpid())"

  type SparkContext
    jsc::JJavaSparkContext
  end

  function SparkContext(master::String="local", appname::String="Julia Spock App @ $(spockid())")
    SparkContext(JJavaSparkContext((JString, JString), master, appname))
  end

  type RDD
    jrdd::JJavaRDD
  end

  function parallelize(sc::SparkContext, collection)
    arr = collect(map(wrap, collection))
    jlist = convert(JList, arr)
    RDD(jcall(sc.jsc, "parallelize", JJavaRDD, (JList,), jlist))
  end

  function wrap(obj)
    buf = IOBuffer()
    serialize(buf, obj)
    JJuliaObject((Vector{jbyte},), takebuf_array(buf)::Vector{Uint8})
  end

  function unwrap(jobj)
    jobj = convert(JJuliaObject, jobj)
    payload = jcall(jobj, "getPayload", Vector{jbyte}, ())
    deserialize(IOBuffer(uint8(payload)))
  end

  function map(rdd::RDD, f::Callable)
    jfunc = JJuliaFunction((JJuliaObject,), wrap(f))
    RDD(jcall(rdd.jrdd, "map", JJavaRDD, (JFunction,), jfunc))
  end

  function collect(rdd::RDD)
    jlist = jcall(rdd.jrdd, "collect", JList, ())
    map(unwrap, jcall(jlist, "toArray", Vector{JObject}, ()))
  end

  function count(rdd::RDD)
    jcall(rdd.jrdd, "count", jlong, ())
  end

  function convert(::Type{JList}, A::Array)
    jcall(JArrays, "asList", JList, (Vector{JObject},), A)
  end

  classname(jobj::JavaObject) = jcall(jcall(jobj, "getClass", JClass, ()), "toString", JString, ())
end
