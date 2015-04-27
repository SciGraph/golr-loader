package org.monarch.golr.beans;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.neo4j.Neo4jConfiguration;

public class GolrCypherQuery {

  private Neo4jConfiguration neo4jConfiguration;
  private String query;
  private Multimap<String, DirectedRelationshipType> types = HashMultimap.create();
  private Map<String, String> projection = new HashMap<>();

  GolrCypherQuery() {}

  public GolrCypherQuery(String query) {
    this.query = query;
  }

  public Neo4jConfiguration getNeo4jConfiguration() {
    return neo4jConfiguration;
  }

  public String getQuery() {
    return query;
  }

  public Multimap<String, DirectedRelationshipType> getTypes() {
    return types;
  }

  public Map<String, Collection<DirectedRelationshipType>> getCollectedTypes() {
    return types.asMap();
  }

  public Map<String, String> getProjection() {
    return projection;
  }

}
