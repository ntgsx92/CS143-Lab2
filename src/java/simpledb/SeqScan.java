package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    private TransactionId m_tid;
    private int m_tableid;
    private String m_tableAlias;
    private DbFile m_dbfile;
    private DbFileIterator m_it;
   
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        m_tid = tid;
        m_tableid = tableid;
        m_tableAlias = tableAlias;
        m_dbfile = Database.getCatalog().getDatabaseFile(tableid);
        m_it = m_dbfile.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        //some code goes here
        return Database.getCatalog().getTableName(m_tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return m_tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        m_tableid = tableid;
        m_tableAlias = tableAlias;
        m_dbfile = Database.getCatalog().getDatabaseFile(tableid);
        m_it = m_dbfile.iterator(m_tid);

    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        if (m_dbfile == null || m_it ==null )
        {
            throw new TransactionAbortedException();
        }
        m_it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        String[] names = new String[m_dbfile.getTupleDesc().numFields()];
        Type[] types = new Type[m_dbfile.getTupleDesc().numFields()];
        
        for (int i = 0; i < m_dbfile.getTupleDesc().numFields(); i++)
        {
            names[i]=m_dbfile.getTupleDesc().getFieldName(i);
            types[i]=m_dbfile.getTupleDesc().getFieldType(i);
        }
        
        TupleDesc td = new TupleDesc(types, names);
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        try{
            return m_it.hasNext();
        }
        catch (TransactionAbortedException e){ return false;}
        
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
            return m_it.next();
        

        
    }

    public void close() {
        // some code goes here
        m_it.close();
        
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
            m_it.rewind();
    }
  
    
}
