package org.monarch.golr;

import static java.lang.Math.toIntExact;
import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.EvidenceAspect;
import io.scigraph.internal.GraphAspect;
import io.scigraph.neo4j.Neo4jConfiguration;
import io.scigraph.neo4j.Neo4jModule;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Stopwatch;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class BenchmarkQueries {

  static final String queriesDirectory = "/home/jnguyenxuan/golrbenchmark/queriestest";
  static final String configurationPath = "src/test/resources/benchmarkconf.yaml";

  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    Neo4jConfiguration neo4jConfig = mapper.readValue(new File(configurationPath), Neo4jConfiguration.class);
    Injector i = Guice.createInjector(new GolrLoaderModule(), new Neo4jModule(neo4jConfig), new AbstractModule() {
      @Override
      protected void configure() {
        bind(GraphAspect.class).to(EvidenceAspect.class);
      }
    });

    GraphDatabaseService graphDb = i.getInstance(GraphDatabaseService.class);
    CypherUtil cypherUtil = i.getInstance(CypherUtil.class);
    GolrLoader loader = i.getInstance(GolrLoader.class);

    Files.walk(Paths.get(queriesDirectory)).forEach(filePath -> {
      if (Files.isRegularFile(filePath)) {
        GolrCypherQuery query;
        try {
          query = mapper.readValue(filePath.toFile(), GolrCypherQuery.class);
          System.out.println(filePath);
          Stopwatch sw = Stopwatch.createStarted();

          // int count = runCypherQuery(query, graphDb, cypherUtil);
        int count = runGolrQuery(query, loader, filePath.toFile());

        System.out.println(sw.stop());
        System.out.println(count);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  } );



    graphDb.shutdown();

  }

  public static int runGolrQuery(GolrCypherQuery query, GolrLoader loader, File file) throws IOException, ExecutionException {
    FileWriter writer = new FileWriter(new File(file.getAbsolutePath() + ".json"));
    int count = 0;
    count = toIntExact(loader.process(query, writer));
    return count;
  }


  public static int runCypherQuery(GolrCypherQuery query, GraphDatabaseService graphDb, CypherUtil cypherUtil) {
    int count = 0;
    try {

      try (Transaction tx = graphDb.beginTx()) {
        Result result = cypherUtil.execute(query.getQuery());
        while (result.hasNext()) {
          result.next();
          count++;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

}
