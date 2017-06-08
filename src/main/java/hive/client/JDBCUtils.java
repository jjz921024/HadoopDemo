package hive.client;

import java.sql.*;

/**
 * Created by Jun on 2017/5/19.
 */
public class JDBCUtils {

    private static String drive = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive://119.23.43.240:10000/default";

    //注册驱动
    static {
        try {
            Class.forName(drive);
        } catch (ClassNotFoundException e) {
            //抛出运行时异常
            throw new ExceptionInInitializerError(e);
        }
    }

    //获取连接
    public static Connection getConnection() {
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //释放资源
    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                //关闭时出现异常，交给gc回收
                conn = null;
            }
        }

        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                st = null;
            }
        }

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                rs = null;
            }
        }
    }
}
