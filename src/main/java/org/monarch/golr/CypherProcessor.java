package org.monarch.golr;

class CypherProcessor {

  static String injectWildcard(String query) {
    return query.replaceFirst("(?i)RETURN\\s*", "RETURN *, ");
  }

}
