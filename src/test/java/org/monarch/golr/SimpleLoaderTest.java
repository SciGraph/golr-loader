package org.monarch.golr;

import io.scigraph.internal.CypherUtil;
import io.scigraph.internal.GraphApi;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public class SimpleLoaderTest extends SimpleLoadSetup {

    SimpleLoader processor;
    StringWriter writer = new StringWriter();

    @Before
    public void setup() throws IOException {
        CypherUtil cypherUtil = new CypherUtil(graphDb, curieUtil);
        processor =
                new SimpleLoader(graphDb, graph, new CypherUtil(graphDb, curieUtil), curieUtil,
                        new GraphApi(graphDb, cypherUtil, curieUtil));
    }

    @Test
    public void testJSONDocument() throws Exception {
        Writer writer = new StringWriter();
        processor.generate(writer, eqCurieMap);
        JSONAssert.assertEquals(getFixture("fixtures/searchDoc.json"), writer.toString(), JSONCompareMode.NON_EXTENSIBLE);
    }
}
