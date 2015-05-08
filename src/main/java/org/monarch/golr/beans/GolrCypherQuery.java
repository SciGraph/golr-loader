package org.monarch.golr.beans;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;

public class GolrCypherQuery {

  private String query;
  private Multimap<String, DirectedRelationshipType> types = HashMultimap.create();
  private Map<String, String> projection = new HashMap<>();
  private Optional<String> solrServer = Optional.absent();
  private Optional<String> outputFile = Optional.absent();

  GolrCypherQuery() {}

  public GolrCypherQuery(String query) {
    this.query = query;
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

  public Optional<String> getSolrServer() {
    return solrServer;
  }

  public Optional<String> getOutputFile() {
    return outputFile;
  }

  public void setOutputFile(Optional<String> outputFile) {
    this.outputFile = outputFile;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("query", query)
        .add("types", types)
        .add("output", outputFile)
        .add("projection", projection)
        .toString();
  }

}
