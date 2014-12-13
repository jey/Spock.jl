package edu.berkeley.cs.amplab.spock;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

class SpockException extends Exception {
  private static final long serialVersionUID = 1;
  JuliaObject exc;

  public SpockException(JuliaObject exc) {
    this.exc = exc;
  }
}

class JuliaException extends SpockException {
  private static final long serialVersionUID = 1;

  public JuliaException(JuliaObject exc) {
    super(exc);
  }
}

public class JuliaObject implements Serializable {
  private static final long serialVersionUID = 1;
  byte[] payload;

  public JuliaObject(byte[] serializedPayload) {
    assert serializedPayload.length > 0;
    this.payload = serializedPayload;
  }

  public byte[] getPayload() {
    return payload;
  }

  public static JuliaObject read(DataInputStream in) throws IOException, SpockException {
    int len = in.readInt();
    if(len == 0) {
      int oob = in.readInt();
      if(oob == 0) {
        return null;
      } else if(oob == 1) {
        throw new SpockException(readObj(in, in.readInt()));
      } else if(oob == 2) {
        throw new JuliaException(readObj(in, in.readInt()));
      } else {
        throw new RuntimeException(String.format("unknown OOB msg %d", oob));
      }
    } else {
      return readObj(in, len);
    }
  }

  public static JuliaObject readObj(DataInputStream in, int len) throws IOException {
    byte[] payload = new byte[len];
    in.read(payload);
    return new JuliaObject(payload);
  }

  public void write(DataOutputStream out) throws IOException {
    out.writeInt(payload.length);
    out.write(payload, 0, payload.length);
  }
}
