package common;

import java.sql.*;

/**
 * Created by Administrator on 2019/11/13.
 */
public class JdbcUtil {

    public static Connection getConnection(){
        Connection connection=null;
        try {
            Class.forName(ConfigUtil.getProperties("jdbc.driverClassName"));
            connection= DriverManager.getConnection(ConfigUtil.getProperties("jdbc.url"),
                    ConfigUtil.getProperties("jdbc.username"),ConfigUtil.getProperties("jdbc.password"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;

    }
    public static ResultSet executeQuery(Connection connection, String sql){
        ResultSet rs=null;
        try {
            PreparedStatement statement=connection.prepareStatement(sql);
            rs=statement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static void executeUpdate(Connection connection,String sql){
        try {
            PreparedStatement statement=connection.prepareStatement(sql);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void closeConnection(Connection connection){
        try {
            if(connection!=null){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
