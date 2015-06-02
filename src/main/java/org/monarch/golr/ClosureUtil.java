package org.monarch.golr;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.getFirst;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import edu.sdsc.scigraph.frames.CommonProperties;
import edu.sdsc.scigraph.frames.NodeProperties;
import edu.sdsc.scigraph.neo4j.DirectedRelationshipType;
import edu.sdsc.scigraph.neo4j.GraphUtil;
import edu.sdsc.scigraph.owlapi.OwlLabels;
import edu.sdsc.scigraph.owlapi.curies.CurieUtil;

class ClosureUtil {

  private final GraphDatabaseService graphDb;
  private final CurieUtil curieUtil;
  private final LoadingCache<ClosureKey, Closure> closureCache;

  @Inject
  ClosureUtil(GraphDatabaseService graphDb, CurieUtil curieUtil) {
    this.graphDb = graphDb;
    this.curieUtil = curieUtil;
    closureCache = CacheBuilder.newBuilder()
        .maximumSize(500_000)
        .build(new CacheLoader<ClosureKey, Closure>() {
          @Override
          public Closure load(ClosureKey source) throws Exception {
            return getUncachedClosure(source.node, source.types);
          }
        });
  }

  private String getCurieOrIri(Node node) {
    String iri = (String)checkNotNull(node).getProperty(CommonProperties.URI);
    return curieUtil.getCurie(iri).or(iri);
  }

  /*private String getCurieOrIri(Vertex vertex) {
    String iri = (String)checkNotNull(vertex).getProperty(CommonProperties.URI);
    return curieUtil.getCurie(iri).or(iri);
  }*/

  private String getLabelOrIri(Node node) {
    return getFirst(GraphUtil.getProperties(node, NodeProperties.LABEL, String.class), getCurieOrIri(node));
  }

  /*private String getLabelOrIri(Vertex vertex) {
    return getFirst(TinkerGraphUtil.getProperties(vertex, NodeProperties.LABEL, String.class), getCurieOrIri(vertex));
  }*/

  Closure getClosure(Node start, Collection<DirectedRelationshipType> types) {
    try {
      return closureCache.get(new ClosureKey(start, types));
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
  
  private Closure getUncachedClosure(Node start, Collection<DirectedRelationshipType> types) {
    Closure closure = new Closure();
    TraversalDescription description = graphDb.traversalDescription().depthFirst().uniqueness(Uniqueness.NODE_GLOBAL);
    for (DirectedRelationshipType type: checkNotNull(types)) {
      description = description.relationships(type.getType(), type.getDirection());
    }
    closure.setCurie(getCurieOrIri(checkNotNull(start)));
    closure.setLabel(getLabelOrIri(start));
    for (Path path: description.traverse(start)) {
      Node endNode = path.endNode();
      if (endNode.hasLabel(OwlLabels.OWL_ANONYMOUS)) {
        continue;
      }
      closure.getCuries().add(getCurieOrIri(endNode));
      closure.getLabels().add(getLabelOrIri(endNode));
    }
    return closure;
  }

  static Collection<String> collectLabels(Collection<Closure> closures) {
    return transform(closures, new Function<Closure, String>() {
      @Override
      public String apply(Closure closure) {
        return closure.getLabel();
      }
    });
  }

  static Collection<String> collectIds(Collection<Closure> closures) {
    return transform(closures, new Function<Closure, String>() {
      @Override
      public String apply(Closure closure) {
        return closure.getCurie();
      }
    });
  }

  static Collection<String> collectLabelClosure(Collection<Closure> closures) {
    Collection<String> labels = new ArrayList<>();
    for (Closure closure: closures) {
      labels.addAll(closure.getLabels());
    }
    return labels;
  }

  static Collection<String> collectIdClosure(Collection<Closure> closures) {
    Collection<String> ids = new ArrayList<>();
    for (Closure closure: closures) {
      ids.addAll(closure.getCuries());
    }
    return ids;
  }

  static final class ClosureKey {
    final Node node;
    final Collection<DirectedRelationshipType> types;

    ClosureKey(Node node, Collection<DirectedRelationshipType> types) {
      this.node = node;
      this.types = types;
    }

    @Override
    public int hashCode() {
      // TODO Auto-generated method stub
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ClosureKey)) {
        return false;
      }
      return Objects.equal(node, ((ClosureKey)obj).node) &&
             Objects.equal(types, ((ClosureKey)obj).types);
    }

  }

}
