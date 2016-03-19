package org.monarch.golr;

import java.io.Serializable;

public class Pair implements Serializable {

  private static final long serialVersionUID = -3569876626659744588L;
  private final String first;
  private final String second;

  public Pair(String first, String second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public int hashCode() {
    int hashFirst = first != null ? first.hashCode() : 0;
    int hashSecond = second != null ? second.hashCode() : 0;
    return (hashFirst + hashSecond) * hashSecond + hashFirst;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Pair) {
      return first.equals(((Pair) other).first) && second.equals(((Pair) other).second);
    }

    return false;
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";

  }
}
