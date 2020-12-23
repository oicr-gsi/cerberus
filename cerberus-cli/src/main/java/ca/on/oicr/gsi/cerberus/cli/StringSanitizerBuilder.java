package ca.on.oicr.gsi.cerberus.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;

/** @author mlaszloffy */
final class StringSanitizerBuilder {

  private final List<String> searchList;
  private final List<String> replacementList;

  public StringSanitizerBuilder() {
    searchList = new ArrayList<>();
    replacementList = new ArrayList<>();
  }

  public StringSanitizerBuilder add(String searchString, String replacementString) {
    searchList.add(searchString);
    replacementList.add(replacementString);
    return this;
  }

  public Function<String, String> build() {
    final String[] searchArr = searchList.toArray(new String[0]);
    final String[] replacementArr = replacementList.toArray(new String[0]);
    return s -> StringUtils.replaceEach(s, searchArr, replacementArr);
  }
}
