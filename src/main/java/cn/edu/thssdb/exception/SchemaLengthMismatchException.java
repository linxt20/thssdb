package cn.edu.thssdb.exception;

public class SchemaLengthMismatchException extends RuntimeException {
  private int expectedLen;
  private int realLen;

  public SchemaLengthMismatchException(int expected_length, int real_length) {
    super();
    this.expectedLen = expected_length;
    this.realLen = real_length;
  }

  @Override
  public String getMessage() {
    return "Exception: expected " + expectedLen + " columns, " + "but got " + realLen + " columns.";
  }
}
