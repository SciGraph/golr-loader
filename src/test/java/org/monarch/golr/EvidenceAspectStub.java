package org.monarch.golr;

import io.scigraph.frames.CommonProperties;
import io.scigraph.frames.NodeProperties;
import io.scigraph.internal.GraphAspect;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

public class EvidenceAspectStub implements GraphAspect {

  @Override
  public void invoke(Graph graph) {
    Vertex association = graph.addVertex(9);
    association.setProperty(CommonProperties.IRI, "http://x.org/a_assn");
    association.setProperty(NodeProperties.LABEL, "assn");
    association.addEdge("association_has_subject", graph.getVertex(3));
    association.addEdge("association_has_object", graph.getVertex(4));
    // Add evidence
    Vertex evidence1 = graph.addVertex(11);
    evidence1.setProperty(CommonProperties.IRI, "http://x.org/a_evidence");
    evidence1.setProperty(NodeProperties.LABEL, "ev1");
    association.addEdge("RO_0002558", evidence1);
  }

}
