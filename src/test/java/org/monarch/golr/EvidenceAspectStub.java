package org.monarch.golr;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

import edu.sdsc.scigraph.frames.CommonProperties;
import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.internal.GraphAspect;

public class EvidenceAspectStub implements GraphAspect {

  @Override
  public void invoke(Graph graph) {
    Vertex association = graph.addVertex(6);
    association.setProperty(CommonProperties.URI, "http://x.org/a_assn");
    association.setProperty(NodeProperties.LABEL, "assn");
    association.addEdge("subject", graph.getVertex(3));
    association.addEdge("object", graph.getVertex(4));
  }

}
