package org.sqlite;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import javax.sql.StatementEventListener;

//******************************************************************************
//**  SQLitePooledConnection
//******************************************************************************
/**
 *   This class provides support for distributed transactions. An application
 *   developer usually does not use this interface. It is used by the
 *   transaction manager internally.
 *
 *   Port from H2 JdbcXAConnection:
 *   http://code.google.com/p/h2database/source/browse/trunk/h2/src/main/org/h2/jdbcx/JdbcXAConnection.java
 *
 ******************************************************************************/

public class SQLitePooledConnection implements XAConnection, XAResource {


    private SQLiteDataSourceFactory factory;

    // this connection is kept open as long as the XAConnection is alive
    private Conn physicalConn;

    // this connection is replaced whenever getConnection is called
    private volatile Connection handleConn;
    private ArrayList<ConnectionEventListener> listeners = arrayList();
    private Xid currentTransaction;


    java.util.Properties config;


    //Port from TraceObject.java
    protected static final int XID = 15;
    protected static final int ARRAY = 16;
    private static final int LAST = ARRAY + 1;
    private static final int[] ID = new int[LAST];
    protected static int getNextId(int type) {
        return ID[type]++;
    }
    

    //Port from New.java
    public static <T> ArrayList<T> arrayList() {
        return new ArrayList<T>(4);
    }


  //**************************************************************************
  //** Constructor
  //**************************************************************************
  /** Creates a new instance of SQLitePooledConnection. */

    public SQLitePooledConnection(SQLiteDataSourceFactory factory, Conn physicalConn, java.util.Properties config) {
        this.factory = factory;
        //setTrace(factory.getTrace(), TraceObject.XA_DATA_SOURCE, id);
        this.physicalConn = physicalConn;
        this.config = config;
    }

    public XAResource getXAResource() {
        debugCodeCall("getXAResource");
        return this;
    }

    /**
     * Close the physical connection.
     * This method is usually called by the connection pool.
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        Connection lastHandle = handleConn;
        if (lastHandle != null) {
            listeners.clear();
            lastHandle.close();
        }
        if (physicalConn != null) {
            try {
                physicalConn.close();
            } finally {
                physicalConn = null;
            }
        }
    }


    /**
     * Get a connection that is a handle to the physical connection. This method
     * is usually called by the connection pool. This method closes the last
     * connection handle if one exists.
     *
     * @return the connection
     */
    public Connection getConnection() throws SQLException {
        debug("getConnection()");
        Connection lastHandle = handleConn;
        if (lastHandle != null) {
            lastHandle.close();
        }
        // this will ensure the rollback command is cached
        //physicalConn.rollback();
        handleConn = new PooledJdbcConnection(physicalConn, config);
        return handleConn;
    }
    /**
     * Get the list of prepared transaction branches.
     * This method is called by the transaction manager during recovery.
     *
     * @param flag TMSTARTRSCAN, TMENDRSCAN, or TMNOFLAGS. If no other flags are set,
     *  TMNOFLAGS must be used.
     *  @return zero or more Xid objects
     * @throws XAException
     */
    public Xid[] recover(int flag) throws XAException {
        //debugCodeCall("recover", quoteFlags(flag));
        checkOpen();
        Statement stat = null;
        try {
            stat = physicalConn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT ORDER BY TRANSACTION");
            ArrayList<Xid> list = arrayList();
            while (rs.next()) {
                String tid = rs.getString("TRANSACTION");
                int id = getNextId(XID);
                Xid xid = new SQLiteXid(factory, id, tid);
                list.add(xid);
            }
            rs.close();
            Xid[] result = new Xid[list.size()];
            list.toArray(result);
            return result;
        } catch (SQLException e) {
            XAException xa = new XAException(XAException.XAER_RMERR);
            xa.initCause(e);
            throw xa;
        } finally {
             closeSilently(stat);
        }
    }

    /**
     * Prepare a transaction.
     *
     * @param xid the transaction id
     * @return XA_OK
     * @throws XAException
     */
    public int prepare(Xid xid) throws XAException {
        //if (isDebugEnabled()) {
        //    debugCode("prepare("+JdbcXid.toString(xid)+");");
        //}
        checkOpen();
        if (!currentTransaction.equals(xid)) {
            throw new XAException(XAException.XAER_INVAL);
        }
        Statement stat = null;
        try {
            stat = physicalConn.createStatement();
            stat.execute("PREPARE COMMIT " + SQLiteXid.toString(xid));
        } catch (SQLException e) {
            throw convertException(e);
        } finally {
             closeSilently(stat);
        }
        return XA_OK;
    }

    /**
     * Forget a transaction.
     * This method does not have an effect for this database.
     *
     * @param xid the transaction id
     */
    public void forget(Xid xid) {
        //if (isDebugEnabled()) {
        //    debugCode("forget("+JdbcXid.toString(xid)+");");
        //}
    }

    /**
     * Roll back a transaction.
     *
     * @param xid the transaction id
     * @throws XAException
     */
    public void rollback(Xid xid) throws XAException {
        //if (isDebugEnabled()) {
        //    debugCode("rollback("+JdbcXid.toString(xid)+");");
        //}
        try {
            physicalConn.rollback();
            physicalConn.setAutoCommit(true);
            Statement stat = null;
            try {
                stat = physicalConn.createStatement();
                stat.execute("ROLLBACK TRANSACTION " + SQLiteXid.toString(xid));
            } catch (SQLException e) {
                // ignore (not a two phase commit)
            } finally {
                 closeSilently(stat);
            }
        } catch (SQLException e) {
            throw convertException(e);
        }
        currentTransaction = null;
    }


    /**
     * End a transaction.
     *
     * @param xid the transaction id
     * @param flags TMSUCCESS, TMFAIL, or TMSUSPEND
     * @throws XAException
     */
    public void end(Xid xid, int flags) throws XAException {
        //if (isDebugEnabled()) {
        //    debugCode("end("+JdbcXid.toString(xid)+", "+quoteFlags(flags)+");");
        //}
        // TODO transaction end: implement this method
        if (flags == TMSUSPEND) {
            return;
        }
        if (!currentTransaction.equals(xid)) {
            throw new XAException(XAException.XAER_OUTSIDE);
        }
    }

    /**
     * Start or continue to work on a transaction.
     *
     * @param xid the transaction id
     * @param flags TMNOFLAGS, TMJOIN, or TMRESUME
     * @throws XAException
     */
    public void start(Xid xid, int flags) throws XAException {
        //if (isDebugEnabled()) {
        //    debugCode("start("+JdbcXid.toString(xid)+", "+quoteFlags(flags)+");");
        //}
        if (flags == TMRESUME) {
            return;
        }
        if (flags == TMJOIN) {
            if (currentTransaction != null && !currentTransaction.equals(xid)) {
                throw new XAException(XAException.XAER_RMERR);
            }
        } else if (currentTransaction != null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            physicalConn.setAutoCommit(false);
        } catch (SQLException e) {
            throw convertException(e);
        }
        currentTransaction = xid;
    }

    /**
     * Commit a transaction.
     *
     * @param xid the transaction id
     * @param onePhase use a one-phase protocol if true
     * @throws XAException
     */
    public void commit(Xid xid, boolean onePhase) throws XAException {
        //if (isDebugEnabled()) {
        //    debugCode("commit("+JdbcXid.toString(xid)+", "+onePhase+");");
        //}
        Statement stat = null;
        try {
            if (onePhase) {
                physicalConn.commit();
            } else {
                stat = physicalConn.createStatement();
                stat.execute("COMMIT TRANSACTION " + SQLiteXid.toString(xid));
            }
            physicalConn.setAutoCommit(true);
        } catch (SQLException e) {
            throw convertException(e);
        } finally {
             closeSilently(stat);
        }
        currentTransaction = null;
    }

    /**
     * Register a new listener for the connection.
     *
     * @param listener the event listener
     */
    public void addConnectionEventListener(ConnectionEventListener listener) {
        debugCode("addConnectionEventListener(listener);");
        listeners.add(listener);
    }

    /**
     * Remove the event listener.
     *
     * @param listener the event listener
     */
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        debugCode("removeConnectionEventListener(listener);");
        listeners.remove(listener);
    }

    /**
     * INTERNAL
     */
    void closedHandle() {
        debugCode("closedHandle();");
        ConnectionEvent event = new ConnectionEvent(this);
        // go backward so that a listener can remove itself
        // (otherwise we need to clone the list)
        for (int i = listeners.size() - 1; i >= 0; i--) {
            ConnectionEventListener listener = listeners.get(i);
            listener.connectionClosed(event);
        }
        handleConn = null;
    }

    /**
     * Get the transaction timeout.
     *
     * @return 0
     */
    public int getTransactionTimeout() {
        debugCodeCall("getTransactionTimeout");
        return 0;
    }

    /**
     * Set the transaction timeout.
     *
     * @param seconds ignored
     * @return false
     */
    public boolean setTransactionTimeout(int seconds) {
        debugCodeCall("setTransactionTimeout", seconds);
        return false;
    }

    /**
     * Checks if this is the same XAResource.
     *
     * @param xares the other object
     * @return true if this is the same object
     */
    public boolean isSameRM(XAResource xares) {
        debugCode("isSameRM(xares);");
        return xares == this;
    }

    public void addStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }


    public void removeStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }

    
    private static XAException convertException(SQLException e) {
        XAException xa = new XAException(e.getMessage());
        xa.initCause(e);
        return xa;
    }


    private void checkOpen() throws XAException {
        if (physicalConn == null) {
            throw new XAException(XAException.XAER_RMERR);
        }
    }



    private static String getFileName(String url){
        String fileName = url.substring(JDBC.PREFIX.length());
        if (fileName.startsWith("//")) fileName = fileName.substring(2);

        return fileName;

    }
    
    /**
     * A pooled connection.
     */
    class PooledJdbcConnection extends Conn {

        private boolean isClosed;

        public PooledJdbcConnection(Conn conn, java.util.Properties config) throws SQLException {
            super(conn.url(), getFileName(conn.url()), config);
        }



        public synchronized void close() throws SQLException {
            if (!isClosed) {
                if (getAutoCommit()==false) rollback();
                //setAutoCommit(true); //<--Not sure why we need this line
                closedHandle();
                super.close(); //<--I had to add this line to explicitly close the connection...
                isClosed = true;
            }
        }

        public synchronized boolean isClosed() throws SQLException {
            return isClosed || super.isClosed();
        }

        protected synchronized void checkClosed(boolean write) {
            if (isClosed) {
                //throw DbException.get(ErrorCode.OBJECT_CLOSED);
            }



            //super.checkClosed(write);
        }

    } //PooledJdbcConnection


    /**
     * Close a statement without throwing an exception.
     *
     * @param stat the statement or null
     */
    public static void closeSilently(Statement stat) {
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }


    private static boolean debug = false;
    private static void debugCodeCall(String str, int seconds){
        if (debug) System.err.println(str);
    }
    private static void debugCodeCall(String str){
        if (debug) System.err.println(str);
    }
    private static void debugCode(String str){
        if (debug) System.err.println(str);
    }
    private static void debug(String str){
        if (debug) System.err.println(str);
    }
    private static boolean isDebugEnabled(){ return debug; }

}