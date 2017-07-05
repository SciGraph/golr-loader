package org.monarch.golr;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.solr.common.SolrInputDocument;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import io.scigraph.neo4j.DirectedRelationshipType;
import io.scigraph.owlapi.OwlRelationships;

public class SolrDocUtil {
    
  static final String ID_SUFFIX = "";
  static final String ID_CLOSURE_SUFFIX = "_closure";
  static final String LABEL_SUFFIX = "_label";
  static final String LABEL_CLOSURE_SUFFIX = "_closure_label";
  static final String CLOSURE_MAP_SUFFIX = "_closure_map";

  private static final DirectedRelationshipType SUBCLASS =
      new DirectedRelationshipType(OwlRelationships.RDFS_SUBCLASS_OF, Direction.OUTGOING);
  private static final DirectedRelationshipType EQUIVALENT_CLASS =
      new DirectedRelationshipType(OwlRelationships.OWL_EQUIVALENT_CLASS, Direction.BOTH);
  private static final DirectedRelationshipType SAME_AS =
      new DirectedRelationshipType(OwlRelationships.OWL_SAME_AS, Direction.BOTH);
  private static final DirectedRelationshipType TYPE = 
      new DirectedRelationshipType(OwlRelationships.RDF_TYPE, Direction.OUTGOING);
  private static final DirectedRelationshipType SUBPROPERTY = 
      new DirectedRelationshipType(OwlRelationships.RDFS_SUB_PROPERTY_OF, Direction.OUTGOING);
  static final Collection<DirectedRelationshipType> DEFAULT_CLOSURE_TYPES =
      ImmutableSet.of(EQUIVALENT_CLASS, SUBCLASS, TYPE, SAME_AS, SUBPROPERTY);
  
  private final ObjectMapper mapper = new ObjectMapper();
  private final ClosureUtil closureUtil;
  
  @Inject
  SolrDocUtil(ClosureUtil closureUtil) {
    this.closureUtil = closureUtil;
  }
  
  
  void addNodes(String fieldName, Collection<Node> values,
      Collection<DirectedRelationshipType> types, SolrInputDocument solrDoc) throws IOException {
    List<Closure> closures = new ArrayList<>();
    for (Node node: values) {
      closures.add(closureUtil.getClosure(node, types));
    }
    writeQuint(fieldName, closures, solrDoc);
  }
  

  void addNodes(String fieldName, Collection<Node> values,
      SolrInputDocument solrDoc) throws IOException {
    addNodes(fieldName, values, DEFAULT_CLOSURE_TYPES, solrDoc);
  }
  
  void addClosure(String fieldName, List<String> closures, SolrInputDocument solrDoc) {
    Set<String> closureSet= new HashSet<String>();
    for (String closure : closures){
      closureSet.add(closure);
    }
    List<String> closureList = new ArrayList<String>(closureSet);
    solrDoc.addField(fieldName, closureList);
  }
  
  void writeQuint(String baseName, List<Closure> closures, SolrInputDocument solrDoc)
          throws IOException {
    addClosure(baseName + ID_SUFFIX, ClosureUtil.collectIds(closures), solrDoc);
    addClosure(baseName + LABEL_SUFFIX, ClosureUtil.collectLabels(closures), solrDoc);
    addClosure(baseName + ID_CLOSURE_SUFFIX,
        ClosureUtil.collectIdClosure(closures), solrDoc);
    addClosure(baseName + LABEL_CLOSURE_SUFFIX,
        ClosureUtil.collectLabelClosure(closures), solrDoc);
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, ClosureUtil.collectClosureMap(closures));
    solrDoc.addField(baseName + CLOSURE_MAP_SUFFIX, writer.toString());
  }
  
  

}
