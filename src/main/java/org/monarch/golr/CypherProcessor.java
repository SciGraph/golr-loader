package org.monarch.golr;

import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newHashSet;

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Function;
import com.google.common.base.Splitter;

/***
 * TODO: This should be done with some AST utility - not string manipulation.
 */
class CypherProcessor {

  final static String QUERY_REGEX = ".*RETURN\\s*(.*)";
  final static Pattern PATTERN = Pattern.compile(QUERY_REGEX, Pattern.CASE_INSENSITIVE);

  final static String SEPARATOR_REGEX = "\\s*AS\\s*";
  final static Pattern SEPARATOR_PATTERN = Pattern.compile(SEPARATOR_REGEX, Pattern.CASE_INSENSITIVE);

  static String injectWildcard(String query) {
    return query.replaceFirst("(?i)RETURN\\s*", "RETURN *, ");
  }

  static Collection<String> getProjection(String query) {
    Matcher matcher = PATTERN.matcher(query);
    matcher.find();
    String projection = matcher.group(1);
    return newHashSet(transform(Splitter.on(',').trimResults().split(projection), new Function<String, String>() {
      @Override
      public String apply(String projection) {
        return getLast(Splitter.on(SEPARATOR_PATTERN).trimResults().split(projection));
      }
    }));
  }

}
