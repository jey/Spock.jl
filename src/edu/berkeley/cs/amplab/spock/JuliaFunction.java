package edu.berkeley.cs.amplab.spock;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.commons.io.output.TeeOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

public class JuliaFunction implements FlatMapFunction<Iterator<JuliaObject>, JuliaObject> {
  private static final long serialVersionUID = 1;
  final JuliaObject func;

  public JuliaFunction(JuliaObject func) {
    this.func = func;
  }

  @Override
  public Iterable<JuliaObject> call(Iterator<JuliaObject> args) throws Exception {
    // launch worker
    ProcessBuilder pb = new ProcessBuilder("julia", "src/worker.jl");
    pb.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process worker = pb.start();

    // send input
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(worker.getOutputStream()));
    func.write(out);
    while(args.hasNext()) {
      args.next().write(out);
    }
    out.writeInt(0);
    out.close();

    // start output reader
    final DataInputStream in = new DataInputStream(new BufferedInputStream(worker.getInputStream()));
    Collection<JuliaObject> results = Executors.newSingleThreadExecutor().submit(
      new Callable<Collection<JuliaObject>>() {
        @Override
        public LinkedList<JuliaObject> call() throws IOException, SpockException {
          LinkedList<JuliaObject> results = new LinkedList<JuliaObject>();
          while(true) {
            JuliaObject obj = JuliaObject.read(in);
            if(obj == null) break;
            results.add(obj);
          }
          return results;
        }
      }
    ).get();

    // finish up
    if(worker.waitFor() != 0) {
      throw new RuntimeException(String.format("Spock worker died with exitValue=%d", worker.exitValue()));
    } else {
      return results;
    }
  }
}
