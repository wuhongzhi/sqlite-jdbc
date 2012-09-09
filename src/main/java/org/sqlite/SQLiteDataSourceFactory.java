package org.sqlite;

//******************************************************************************
//**  SQLiteDataSourceFactory
//******************************************************************************
/**
 *   This class is used to create new DataSource objects. Port from 
 *   JdbcDataSourceFactory:
 * 
 *   http://code.google.com/p/h2database/source/browse/trunk/h2/src/main/org/h2/jdbcx/JdbcDataSourceFactory.java
 *
 ******************************************************************************/

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

//import org.h2.constant.SysProperties;
//import org.h2.engine.Constants;
//import org.h2.message.Trace;
//import org.h2.message.TraceSystem;

/**
 * This class is used to create new DataSource objects.
 * An application should not use this class directly.
 */
public class SQLiteDataSourceFactory implements ObjectFactory {

    //private static TraceSystem cachedTraceSystem;
    //private Trace trace;

    static {
        //org.h2.Driver.load();
    }

    /**
     * The public constructor to create new factory objects.
     */
    public SQLiteDataSourceFactory() {
        //trace = getTraceSystem().getTrace("JDBCX");
    }

    /**
     * Creates a new object using the specified location or reference
     * information.
     *
     * @param obj the reference (this factory only supports objects of type
     *            javax.naming.Reference)
     * @param name unused
     * @param nameCtx unused
     * @param environment unused
     * @return the new JdbcDataSource, or null if the reference class name is
     *         not JdbcDataSource.
     */
    public synchronized Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) {
        //if (trace.isDebugEnabled()) {
            //trace.debug("getObjectInstance obj={0} name={1} nameCtx={2} environment={3}", obj, name, nameCtx, environment);
        //}
        if (obj instanceof Reference) {
            Reference ref = (Reference) obj;
            if (ref.getClassName().equals(SQLiteDataSource.class.getName())) {
                SQLiteDataSource dataSource = new SQLiteDataSource();


                dataSource.setUrl((String) ref.get("url").getContent());

                try{
                String s = (String) ref.get("loginTimeout").getContent();
                dataSource.setLoginTimeout(Integer.parseInt(s));
                }
                catch(Exception e){}
                return dataSource;
            }
        }
        return null;
    }
}
