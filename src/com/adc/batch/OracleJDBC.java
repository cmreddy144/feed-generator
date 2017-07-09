package com.adc.batch;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Debopam
 */
public class OracleJDBC {

    public static Connection GetConnection() {
        Properties props = new Properties();
        String propsFile = "ConnConfig.properties";
        StringBuilder connectionString = new StringBuilder();
        Connection connection = null;

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propsFile);
            props.load(inputStream);
            connectionString.append(props.getProperty("conn.url"));
            connectionString.append(props.getProperty("oracle.serverName"));
            connectionString.append(":");
            connectionString.append(props.getProperty("oracle.portNumber"));
            connectionString.append(":");
            connectionString.append(props.getProperty("oracle.sid"));

            connection = DriverManager.getConnection(connectionString.toString(), props.getProperty("ptax.userName"), props.getProperty("ptax.pwd"));
        } catch (SQLException e) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, e);
        } catch (IOException e) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, e);
        } catch (ClassNotFoundException e) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            return connection;
        }
    }

    public static void CloseConnection(Connection con) {
        try {
            con.close();
        } catch (SQLException e) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, e);
        }
    }
}
