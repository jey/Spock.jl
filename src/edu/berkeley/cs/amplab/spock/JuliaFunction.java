package edu.berkeley.cs.amplab.spock;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.commons.io.output.TeeOutputStream;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

public class JuliaFunction implements FlatMapFunction<Iterator<JuliaObject>, JuliaObject> {
  final JuliaObject func;

  public JuliaFunction(JuliaObject func) {
    this.func = func;
  }

  public Iterable<JuliaObject> call(Iterator<JuliaObject> args) throws Exception {
    // launch worker
    ProcessBuilder pb = new ProcessBuilder("julia", "src/worker.jl");
    pb.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process worker = pb.start();

    // send input
    FileOutputStream outf = new FileOutputStream("out.dump");
    DataOutputStream out =
      new DataOutputStream(new TeeOutputStream(outf, worker.getOutputStream()));
    func.write(out);
    while(args.hasNext()) {
      args.next().write(out);
    }
    out.close();

    // start output reader
    final DataInputStream in = new DataInputStream(new BufferedInputStream(worker.getInputStream()));
    final LinkedList<JuliaObject> results = new LinkedList<JuliaObject>();
    Thread reader = new Thread() {
      public boolean done = false;
      public void run() {
        try {
          while(true) {
            results.add(JuliaObject.read(in));
          }
        } catch(EOFException ex) {
          done = true;
        } catch(IOException ex) {
          System.err.println("FIXME WTF");
          System.exit(-1);
        }
      }
    };
    reader.start();

    // finish work
    worker.waitFor();
    reader.join();
    assert worker.exitValue() == 0;
    return results;
  }
}
