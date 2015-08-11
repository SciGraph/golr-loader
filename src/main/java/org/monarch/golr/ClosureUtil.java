package org.monarch.golr;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Lists.transform;
import io.scigraph.frames.CommonProperties;
import io.scigraph.frames.NodeProperties;
import io.scigraph.neo4j.DirectedRelationshipType;
import io.scigraph.neo4j.GraphUtil;
import io.scigraph.owlapi.OwlLabels;
import io.scigraph.owlapi.curies.CurieUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.monarch.golr.beans.Closure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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
    String iri = (String)checkNotNull(node).getProperty(CommonProperties.IRI);
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
    description = description.evaluator(new Evaluator() {
      @Override
      public Evaluation evaluate(Path path) {
        Node node = path.endNode();
        if (node.hasLabel(OwlLabels.OWL_ANONYMOUS)) {
          return Evaluation.EXCLUDE_AND_PRUNE;
        } else {
          return Evaluation.INCLUDE_AND_CONTINUE;
        }
      }

    });
    closure.setCurie(getCurieOrIri(checkNotNull(start)));
    closure.setLabel(getLabelOrIri(start));
    for (Path path: description.traverse(start)) {
      Node endNode = path.endNode();
      closure.getCuries().add(getCurieOrIri(endNode));
      closure.getLabels().add(getLabelOrIri(endNode));
    }
    return closure;
  }

  static List<String> collectLabels(List<Closure> closures) {
    return transform(closures, new Function<Closure, String>() {
      @Override
      public String apply(Closure closure) {
        return closure.getLabel();
      }
    });
  }

  static List<String> collectIds(List<Closure> closures) {
    return transform(closures, new Function<Closure, String>() {
      @Override
      public String apply(Closure closure) {
        return closure.getCurie();
      }
    });
  }

  static List<String> collectLabelClosure(List<Closure> closures) {
    List<String> labels = new ArrayList<>();
    for (Closure closure: closures) {
      labels.addAll(closure.getLabels());
    }
    return labels;
  }

  static List<String> collectIdClosure(List<Closure> closures) {
    List<String> ids = new ArrayList<>();
    for (Closure closure: closures) {
      ids.addAll(closure.getCuries());
    }
    return ids;
  }

  static Map<String, String> collectClosureMap(List<Closure> closures) {
    Map<String, String> closureMap = new HashMap<>();
    List<String> idClosure = collectIdClosure(closures);
    List<String> labelClosure = collectLabelClosure(closures);
    checkState(idClosure.size() == labelClosure.size());
    Iterator<String> keyIter = idClosure.iterator();
    Iterator<String> valueIter = labelClosure.iterator();
    while (keyIter.hasNext() && valueIter.hasNext()) {
      closureMap.put(keyIter.next(), valueIter.next());
    }
    return closureMap;
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
      return Objects.hashCode(node, types);
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
