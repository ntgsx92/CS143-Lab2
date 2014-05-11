package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId m_t;
    
    private DbIterator m_child;
    
    private int m_tableid;
    
    private TupleDesc m_td;
    
    private int num_records;
    
    private boolean isinvoked;
    private boolean tupleadded;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        m_t = t;
        m_child = child;
        m_tableid = tableid;
        /*if(this.m_child.getTupleDesc() != Database.getCatalog().getTupleDesc(m_tableid)){
        	throw new DbException("TupleDesc does not match");
        }*/
        m_td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"NUM INSERTED"});
        num_records = 0;
        isinvoked = false;
    }

    public TupleDesc getTupleDesc() {
    	return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	isinvoked = true;
    	super.open();
        m_child.open();
        tupleadded = false;
    }

    public void close() {
    	isinvoked = false;
        m_child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	isinvoked = true;
        m_child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(tupleadded == true){
    		return null;
    	}
    	m_child.open();
    	while(m_child.hasNext()){
    		try {
				Database.getBufferPool().insertTuple(m_t, m_tableid, m_child.next());
				num_records++;
			} catch (NoSuchElementException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	Tuple result = new Tuple(m_td);
    	Field num_inserted = new IntField(num_records);
    	result.setField(0,num_inserted);
    	tupleadded = true;
    	m_child.close();
    	return result;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[]{m_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	m_child = children[0];
    }
}
