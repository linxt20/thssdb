package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.CreateDatabasePlan;
import cn.edu.thssdb.plan.impl.DropDatabasePlan;
import cn.edu.thssdb.plan.impl.UseDatabasePlan;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    System.out.println("[DEBUG] sessionId: " + req.getSessionId());
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement);
    String name;
    switch (plan.getType()) {
      case CREATE_DB:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        CreateDatabasePlan createDatabasePlan = (CreateDatabasePlan) plan;
        name = createDatabasePlan.getDatabaseName();
        // 判断是否已经存在这个database，如果不存在则创建并返回success，否则返回fail
        if (Manager.getInstance().containDatabase(name)) {
          return new ExecuteStatementResp(StatusUtil.fail("Database already exists."), false);
        } else {
          Manager.getInstance().createDatabaseIfNotExists(name);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        }
      case DROP_DB:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        DropDatabasePlan dropDatabasePlan = (DropDatabasePlan) plan;
        name = dropDatabasePlan.getDatabaseName();
        // 判断是否已经存在这个database，如果不存在则返回fail，否则删除并返回success
        if (Manager.getInstance().containDatabase(name)) {
          Manager.getInstance().deleteDatabase(name);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } else {
          return new ExecuteStatementResp(StatusUtil.fail("Database does not exist."), false);
        }
      case USE_DB:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        UseDatabasePlan useDatabasePlan = (UseDatabasePlan) plan;
        name = useDatabasePlan.getDatabaseName();
        // 判断是否已经存在这个database，如果不存在则返回fail，否则设置manager中的currentDB后返回success
        if (Manager.getInstance().containDatabase(name)) {
          Manager.getInstance().setCurrentDB(name);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } else {
          return new ExecuteStatementResp(StatusUtil.fail("Database does not exist."), false);
        }
      default:
    }
    return null;
  }
}
