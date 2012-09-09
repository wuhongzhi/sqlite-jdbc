package org.sqlite;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;

//******************************************************************************
//**  SQLiteConnectionPoolDataSource
//******************************************************************************
/**
 *   A data source for connection pools. It is a factory for XAConnection
 *   and Connection objects. This class is usually registered in a JNDI naming
 *   service.
 * 
 *   Partial port of H2 JdbcDataSource:
 *   http://code.google.com/p/h2database/source/browse/trunk/h2/src/main/org/h2/jdbcx/JdbcDataSource.java?r=716
 *
 ******************************************************************************/

public class SQLiteConnectionPoolDataSource extends SQLiteDataSource implements javax.sql.ConnectionPoolDataSource {

    
    //private static final long serialVersionUID = 1288136338451857771L;

    private transient SQLiteDataSourceFactory factory;
    //private transient PrintWriter logWriter;
    //private int loginTimeout;
    //private String user = "";
    //private String password = "";
    //private String url = "";


  //**************************************************************************
  //** Constructor
  //**************************************************************************
  /** Creates a new instance of ConnectionPoolDataSource. */

    public SQLiteConnectionPoolDataSource() {
        super();
        initFactory();
    }

    public PooledConnection getPooledConnection() throws SQLException {
        //return new SQLitePooledConnection();
        return getXAConnection();
    }

    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        //return new SQLitePooledConnection();
        return getXAConnection(user, password);
    }

    public XAConnection getXAConnection() throws SQLException {
        //int id = getNextId(XA_DATA_SOURCE);
        return getXAConnection(null, null);
    }


    public XAConnection getXAConnection(String user, String password) throws SQLException {
        //int id = getNextId(XA_DATA_SOURCE);
        return new SQLitePooledConnection(factory, getJdbcConnection(user, password), this.getConfig().toProperties());
    }

    private void initFactory() {
        factory = new SQLiteDataSourceFactory();
    }


    /**
     * Open a new connection using the current URL, user name and password.
     *
     * @return the connection
     */
    public Connection getConnection() throws SQLException {
        //debugCodeCall("getConnection");
        return getJdbcConnection(null, null);
    }

    /**
     * Open a new connection using the current URL and the specified user name
     * and password.
     *
     * @param user the user name
     * @param password the password
     * @return the connection
     */
    public Connection getConnection(String user, String password) throws SQLException {
        //if (isDebugEnabled()) {
        //    debugCode("getConnection("+quote(user)+", "+quote(password)+");");
        //}
        return getJdbcConnection(user, password);
    }

    private Conn getJdbcConnection(String user, String password) throws SQLException {
        //if (isDebugEnabled()) {
        //    debugCode("getJdbcConnection("+quote(user)+", "+quote(password)+");");
        //}
        //Properties info = new Properties();
        //info.setProperty("user", user);
        //info.setProperty("password", password);

        String url = this.getUrl();
        String fileName = url.substring(JDBC.PREFIX.length());
        if (fileName.startsWith("//")) fileName = fileName.substring(2);

        
        return new Conn(url, fileName, this.getConfig().toProperties());
    }


}