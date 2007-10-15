//--------------------------------------
// sqlite-jdbc Project
//
// SQLiteJDBCLoaderTest.java
// Since: Oct 15, 2007
//
// $URL$ 
// $Author$
//--------------------------------------
package org.xerial.db.sql.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SQLiteJDBCLoaderTest
{

    @Before
    public void setUp() throws Exception
    {

    }

    @After
    public void tearDown() throws Exception
    {}

    @Test
    public void query() throws ClassNotFoundException
    {
        SQLiteJDBCLoader.initialize();
          
         // load the sqlite-JDBC driver into the current class loader
         Class.forName("org.sqlite.JDBC");
         
         Connection connection = null;
         try
         {
             // create a database connection
             connection = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement statement = connection.createStatement();
             statement.setQueryTimeout(30);  // set timeout to 30 sec.

             statement.executeUpdate("create table person ( id integer, name string)");
             statement.executeUpdate("insert into person values(1, 'leo')");
             statement.executeUpdate("insert into person values(2, 'yui')");
             
             ResultSet rs = statement.executeQuery("select * from person order by id");
             while(rs.next())
             {
                 // read the result set
                 int id = rs.getInt(1);
                 String name = rs.getString(2);
             }
         }
         catch(SQLException e)
         {
             // if e.getMessage() is "out of memory", it probably means no
                // database file is found
            
         }
         finally
         {
            try
            {
               if(connection != null)
                  connection.close();
            }
            catch(SQLException e)
            {
                // connection close failed.
            }
         }
      }
}