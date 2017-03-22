package org.monarch.golr;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.monarch.golr.beans.GolrCypherQuery;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.prefixcommons.CurieUtil;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

import io.scigraph.internal.CypherUtil;

public class QueriesSanityCheck {

  private final static ExecutorService timeoutExecutorService = Executors.newSingleThreadExecutor();
  private final static int timeout = 1; // in hours


  public static void main(String[] args)
      throws JsonParseException, JsonMappingException, IOException, InterruptedException {
    if (args.length < 3) {
      System.out.println("usage: [graphPath] [curiePath] [queryPath*]");
      System.exit(-1);
    } else {
      String graphPath = args[0];
      String curiePath = args[1];
      List<String> queryPaths = Arrays.asList(args).subList(2, args.length);
      // String graphPath = "/home/jnguyenxuan/workspace/SciGraph-playground/graph";
      // String curiePath = "curie_map.yaml";
      // List<String> queryPath =
      // "/home/jnguyenxuan/workspace/monarch-cypher-queries/src/main/cypher/golr-loader/";

      Set<String> failedQueries = Sets.newHashSet();

      GraphDatabaseService graphDb = new GraphDatabaseFactory()
          .newEmbeddedDatabaseBuilder(new File(graphPath)).newGraphDatabase();

      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      Map<String, String> curieMap = mapper.readValue(new File(curiePath), Map.class);
      CurieUtil curieUtil = new CurieUtil(curieMap);
      CypherUtil cypherUtil = new CypherUtil(graphDb, curieUtil);

      queryPaths.stream().forEach(queryPath -> {
        try {
          Files.walk(Paths.get(queryPath)).forEach(filePath -> {
            if (Files.isRegularFile(filePath)) {
              GolrCypherQuery query;
              try {
                query = mapper.readValue(filePath.toFile(), GolrCypherQuery.class);
                System.out.println(filePath);
                Stopwatch sw = Stopwatch.createStarted();

                int count = runCypherQueryWithTimeout(query, graphDb, cypherUtil, timeout);

                System.out.println(sw.stop());
                System.out.println(count);
              } catch (TimeoutException e) {
                failedQueries.add(filePath.getFileName().toString());
                System.out.println("Timeout!!");
              } catch (Exception e) {
                failedQueries.add(filePath.getFileName().toString());
                e.printStackTrace();
              }
            }
          });
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

      timeoutExecutorService.shutdown();
      if (failedQueries.isEmpty()) {
        System.exit(0);
      } else {
        System.out.println("==== FAILURES =====");
        System.out.println("The following queries have failed: ");
        failedQueries.stream().forEach(System.out::println);
        System.exit(-1);
      }
    }
  }

  public static int runCypherQuery(GolrCypherQuery query, GraphDatabaseService graphDb,
      CypherUtil cypherUtil) {
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

  public static int runCypherQueryWithTimeout(GolrCypherQuery query, GraphDatabaseService graphDb,
      CypherUtil cypherUtil, int timeout)
      throws ExecutionException, InterruptedException, TimeoutException {
    Future<Integer> future =
        timeoutExecutorService.submit(() -> runCypherQuery(query, graphDb, cypherUtil));
    return future.get(timeout, TimeUnit.HOURS);
  }

}
