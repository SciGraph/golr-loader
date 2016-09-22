package org.monarch.golr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Set;

import com.tinkerpop.blueprints.Graph;


public class EvidenceGraphInfo implements Serializable {

  private static final long serialVersionUID = -3569876626659744588L;
  final byte[] graphBytes;
  final boolean emitEvidence;
  final Set<Long> ignoredNodes;

  public EvidenceGraphInfo(Graph graphPath, boolean emitEvidence, Set<Long> ignoredNodes)
      throws IOException {
    this.graphBytes = toByteArray(graphPath);
    this.emitEvidence = emitEvidence;
    this.ignoredNodes = ignoredNodes;
  }

  @Override
  public int hashCode() {
    int hashGraphPath = graphBytes != null ? graphBytes.hashCode() : 0;
    int hashIgnoredNodes = ignoredNodes != null ? ignoredNodes.hashCode() : 0;
    return (hashGraphPath + hashIgnoredNodes) * hashGraphPath + hashIgnoredNodes;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof EvidenceGraphInfo) {
      return graphBytes.equals(((EvidenceGraphInfo) other).graphBytes)
          && emitEvidence == ((EvidenceGraphInfo) other).emitEvidence
          && ignoredNodes == ((EvidenceGraphInfo) other).ignoredNodes;
    }

    return false;
  }

  @Override
  public String toString() {
    return "(" + graphBytes + ", " + emitEvidence + ", " + ignoredNodes + ")";
  }

  public synchronized static String getNewTmpDirForTinkerGraph() throws IOException {
    Path p = java.nio.file.Files.createTempDirectory("tinkergraph");
    File newTmpDirFile = p.toFile();
    String newTmpDir = newTmpDirFile.getAbsolutePath();
    newTmpDirFile.delete(); // TinkerGraph needs to create the directory
    return newTmpDir;
  }

  // We have to handle the serialization ourselves because of
  // https://github.com/tinkerpop/blueprints/issues/285
  public static byte[] toByteArray(Graph g) throws IOException {
    // Serialize data object to a byte array
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(g);
    out.close();

    // Get the bytes of the serialized object
    byte[] buf = bos.toByteArray();
    return buf;
  }

  public static Graph toGraph(byte[] b) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bis = new ByteArrayInputStream(b);
    ObjectInputStream in = new ObjectInputStream(bis);
    Graph g = (Graph) in.readObject();
    in.close();
    return g;
  }

}
