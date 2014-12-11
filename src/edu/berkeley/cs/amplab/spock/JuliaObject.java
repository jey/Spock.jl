package edu.berkeley.cs.amplab.spock;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class JuliaObject implements Serializable {
  byte[] payload;

  public JuliaObject(byte[] serializedPayload) {
    assert serializedPayload.length > 0;
    this.payload = serializedPayload;
  }

  public byte[] getPayload() {
    return payload;
  }

  public static JuliaObject read(DataInputStream in) throws IOException {
    int len = in.readInt();
    if(len == 0) return null;
    byte[] payload = new byte[len];
    in.read(payload);
    return new JuliaObject(payload);
  }

  public void write(DataOutputStream out) throws IOException {
    out.writeInt(payload.length);
    out.write(payload, 0, payload.length);
  }
}
