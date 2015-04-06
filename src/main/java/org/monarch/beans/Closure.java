package org.monarch.beans;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.MoreObjects;

public class Closure {

  private final List<String> curies = new ArrayList<>();
  private final List<String> labels = new ArrayList<>();

  public List<String> getCuries() {
    return curies;
  }

  public List<String> getLabels() {
    return labels;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("curies", curies)
        .add("labels", labels).toString();
  }

}
