package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_DB,
    DROP_DB,
    USE_DB,
    CREATE_TABLE,
    SHOW_TABLE,
    SHOW_DATABASES,
    SHOW_DATABASE_META,
    DROP_TABLE,
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    BEGIN_TRANSACTION,
    COMMIT,
    AUTO_BEGIN_TRANSACTION,
    AUTO_COMMIT,
    ALTER_TABLE,
  }
}
