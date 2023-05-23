package cn.edu.thssdb.exception;

public class TypeLengthMismatchException extends RuntimeException {
  private String key;

  private int maxLength;

  public TypeLengthMismatchException(String key, int maxLength) {
    super();
    this.key = key;
    this.maxLength = maxLength;
  }

  @Override
  public String getMessage() {
    return "Exception: String max length is" + maxLength + "\"!";
  }
}
