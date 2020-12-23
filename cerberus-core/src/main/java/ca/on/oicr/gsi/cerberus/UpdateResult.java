package ca.on.oicr.gsi.cerberus;

import java.util.List;

public abstract class UpdateResult<T> {
  public interface Visitor<T, R> {
    R incremental(List<T> items);

    R restart(List<T> items);
  }

  public static <T> UpdateResult<T> incremental(List<T> items) {
    return new UpdateResult<>() {

      @Override
      public <R> R visit(Visitor<T, R> visitor) {
        return visitor.incremental(items);
      }
    };
  }

  public static <T> UpdateResult<T> restart(List<T> items) {
    return new UpdateResult<T>() {

      @Override
      public <R> R visit(Visitor<T, R> visitor) {
        return visitor.restart(items);
      }
    };
  }

  private UpdateResult() {}

  public abstract <R> R visit(Visitor<T, R> visitor);
}
