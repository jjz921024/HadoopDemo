package hive.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Jun on 2017/5/19.
 */
public class HiveJDBC {

    public static void main(String[] args) {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        String hql = "select * from tb1"; //不需要分号


        //获取连接
        conn = JDBCUtils.getConnection();
        try {
            //创建运行环境
            st = conn.createStatement();
            //执行hql
            if (st.execute(hql)) {
                rs = st.getResultSet();
                while (rs.next()) {
                    String name = rs.getString(2);
                    System.out.println(name);
                }
            }
            /*rs = st.executeQuery(hql);
            while (rs.next()) {
                //索引从1开始
                String name = rs.getString(2);
                System.out.println(name);
            }*/


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.release(conn, st, rs);
        }
    }
}
