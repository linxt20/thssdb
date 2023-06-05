package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class AutoBeginTransactionPlan extends LogicalPlan {
  public AutoBeginTransactionPlan() {
    super(LogicalPlanType.AUTO_BEGIN_TRANSACTION);
  }

  @Override
  public String toString() {
    return "auto begin transaction";
  }
}
