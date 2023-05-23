package cn.edu.thssdb.exception;

public class NullValueException extends RuntimeException {
  private String key;

  public NullValueException() {
    super();
    this.key = null;
  }

  public NullValueException(String key) {
    super();
    this.key = key;
  }

  @Override
  public String getMessage() {
    if (key == null) return "Exception: null value!";
    else return "Exception: null value in column \"" + this.key + "\"!";
  }
}
