package org.monarch.golr;

import java.io.Serializable;
import java.util.Set;

public class EvidenceGraphInfo implements Serializable {

  private static final long serialVersionUID = -3569876626659744588L;
  final String graphPath;
  final boolean emitEvidence;
  final Set<Long> ignoredNodes;

  public EvidenceGraphInfo(String graphPath, boolean emitEvidence, Set<Long> ignoredNodes) {
    this.graphPath = graphPath;
    this.emitEvidence = emitEvidence;
    this.ignoredNodes = ignoredNodes;
  }

  @Override
  public int hashCode() {
    int hashGraphPath = graphPath != null ? graphPath.hashCode() : 0;
    int hashIgnoredNodes = ignoredNodes != null ? ignoredNodes.hashCode() : 0;
    return (hashGraphPath + hashIgnoredNodes) * hashGraphPath + hashIgnoredNodes;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof EvidenceGraphInfo) {
      return graphPath.equals(((EvidenceGraphInfo) other).graphPath) && emitEvidence == ((EvidenceGraphInfo) other).emitEvidence
          && ignoredNodes == ((EvidenceGraphInfo) other).ignoredNodes;
    }

    return false;
  }

  @Override
  public String toString() {
    return "(" + graphPath + ", " + emitEvidence + ", " + ignoredNodes + ")";
  }
}
