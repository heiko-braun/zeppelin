package com.nflabs.zeppelin.presto;

import com.nflabs.zeppelin.interpreter.Interpreter;
import com.nflabs.zeppelin.interpreter.InterpreterContext;
import com.nflabs.zeppelin.interpreter.InterpreterResult;
import com.nflabs.zeppelin.scheduler.Scheduler;
import com.nflabs.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Shell interpreter for Zeppelin.
 *
 * @author Leemoonsoo
 * @author anthonycorbacho
 *
 */
public class PrestoInterpreter extends Interpreter {
  private static final String TAB = "\t";
  private static final String NL = "\n";
  private Logger logger = LoggerFactory.getLogger(PrestoInterpreter.class);
  private int commandTimeOut = 600000;

  static {
    Interpreter.register("presto", PrestoInterpreter.class.getName());
  }

  public PrestoInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {}

  @Override
  public void close() {}


  @Override
  public InterpreterResult interpret(String sql, InterpreterContext context) {

    String url = (String) context.getConfig()
            .getOrDefault("url", "jdbc:presto://localhost:8180/");

    try  {
      Connection connection =  DriverManager.getConnection(url, "admin", null);
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery(sql);

      StringBuffer sb = new StringBuffer();
      ResultSetMetaData metaData = rs.getMetaData();
      int columns = metaData.getColumnCount();

      // columns names
      for (int i = 1; i <= columns; i++) {
        sb.append(metaData.getColumnName(i));
        if (i < columns)
          sb.append(TAB);
        else
          sb.append(NL);
      }

      // row data
      while (rs.next()) {
        int i = 1;
        while (i <= columns) {
          sb.append(rs.getString(i++));
          if (i < columns)
            sb.append(TAB);
          else
            sb.append(NL);
        }
      }

      return new InterpreterResult(InterpreterResult.Code.SUCCESS,
              InterpreterResult.Type.TABLE, sb.toString()
      );
    } catch (Throwable t) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, t.getMessage());
    }

  }

  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton()
            .createOrGetFIFOScheduler(PrestoInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }

}
