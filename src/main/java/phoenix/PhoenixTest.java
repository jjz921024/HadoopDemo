package phoenix;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * Created by Jun on 2017/6/1.
 */
public class PhoenixTest {
    private Connection conn;
    private Statement stmt;
    private ResultSet rs;

    @Before
    public void initResource() throws ClassNotFoundException, SQLException {
        Class.forName(ConfigUtil.getDriver());
        conn = DriverManager.getConnection(ConfigUtil.getUrl(), ConfigUtil.getUsername(), ConfigUtil.getPassword());
        stmt = conn.createStatement();
    }

    @Test
    public void testCreateTable() throws SQLException {
        String sql = "CREATE TABLE test(rk INTEGER NOT NULL PRIMARY KEY , col VARCHAR)";
        stmt.execute(sql);
        conn.commit();
    }

    @After
    public void closeResource() throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }


}
