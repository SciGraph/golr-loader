package org.monarch.golr.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.scigraph.neo4j.DirectedRelationshipType;

import java.util.Collection;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class GolrCypherQuery {
  private String query;
  @JsonProperty("subject_closure")
  private String subjectClosure;
  @JsonProperty("object_closure")
  private String objectClosure;
  @JsonProperty("relation_closure")
  private String relationClosure;
  @JsonProperty("evidence_closure")
  private String evidenceClosure;
  private Multimap<String, DirectedRelationshipType> types = HashMultimap.create();

  public GolrCypherQuery() { }

  public GolrCypherQuery(String query) {
    this();
    this.query = query;
  }
  public GolrCypherQuery(String query, String subject_closure) {
    this(query);
    this.subjectClosure = subject_closure;
  }
  public GolrCypherQuery(String query, String subject_closure, String object_closure) {
    this(query, subject_closure);
    this.objectClosure = object_closure;
  }
  public GolrCypherQuery(String query, String subject_closure, String object_closure, String relation_closure) {
    this(query, subject_closure, object_closure);
    this.relationClosure = relation_closure;
  }
  public GolrCypherQuery(String query, String subject_closure, String object_closure, String relation_closure, String evidence_closure) {
    this(query, subject_closure, object_closure, relation_closure);
    this.evidenceClosure = evidence_closure;
  }

  public String getQuery() {
    return query;
  }
  public String getSubjectClosure() {
    return subjectClosure;
  }
  public String getObjectClosure() {
    return objectClosure;
  }
  public String getRelationClosure() {
    return relationClosure;
  }
  public String getEvidenceClosure() {
    return evidenceClosure;
  }

  public void setSubjectClosure(String subjectClosure) {
    this.subjectClosure = subjectClosure;
  }
  public void setObjectClosure(String objectClosure) {
    this.objectClosure = objectClosure;
  }
  public void setRelationClosure(String relationClosure) {
    this.relationClosure = relationClosure;
  }
  public void setEvidenceClosure(String evidenceClosure) {
    this.evidenceClosure = evidenceClosure;
  }

  public Multimap<String, DirectedRelationshipType> getTypes() {
    return types;
  }

  public Map<String, Collection<DirectedRelationshipType>> getCollectedTypes() {
    return types.asMap();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).
            add("query", query).
            add("subject_closure", subjectClosure).
            add("object_closure", objectClosure).
            add("relation_closure", relationClosure).
            add("evidence_closure", evidenceClosure).
            add("types", types).
            toString();
  }

}
