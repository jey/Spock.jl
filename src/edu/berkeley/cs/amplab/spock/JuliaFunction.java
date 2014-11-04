package edu.berkeley.cs.amplab.spock;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class JuliaFunction implements Function<JuliaObject, JuliaObject> {
  final JuliaObject func;

  public JuliaFunction(JuliaObject func) {
    this.func = func;
  }

  public JuliaObject call(JuliaObject args) throws Exception {
    return args;
  }
}
