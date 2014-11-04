package edu.berkeley.cs.amplab.spock;
import java.io.Serializable;

public class JuliaObject implements Serializable {
  byte[] payload;

  public JuliaObject(byte[] serializedPayload) {
    this.payload = serializedPayload;
  }

  public byte[] getPayload() {
    return payload;
  }
}
