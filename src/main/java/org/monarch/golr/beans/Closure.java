package org.monarch.golr.beans;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.MoreObjects;

/***
 * A convenience class encapsulating a closure pair.
 */
public class Closure {

  private String curie;
  private String label;
  private final List<String> curies = new ArrayList<>();
  private final List<String> labels = new ArrayList<>();

  public String getCurie() {
    return curie;
  }

  public void setCurie(String curie) {
    this.curie = curie;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

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
