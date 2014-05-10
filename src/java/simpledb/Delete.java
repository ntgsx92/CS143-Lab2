package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId m_t;
    
    private DbIterator m_child;
    
    private TupleDesc m_td;
    
    private int num_delete;
    
    private boolean isinvoked;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	m_t = t;
    	m_child = child;
    	m_td = m_child.getTupleDesc();
    	num_delete = 0;
    	isinvoked = false;
    }

    public TupleDesc getTupleDesc() {
    	return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
        m_child.open();
        super.open();
    }

    public void close() {
        m_child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        m_child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(isinvoked){
    		return null;
    	}
    	while(m_child.hasNext()){
    		try {
				Database.getBufferPool().deleteTuple(m_t, m_child.next());
				num_delete++;
			} catch (NoSuchElementException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	isinvoked = true;
    	Type []td_type = {Type.INT_TYPE};
    	String []td_name = {"Number of deleted records"};
    	TupleDesc delete_td = new TupleDesc(td_type,td_name);
    	Tuple result = new Tuple(delete_td);
    	result.setField(0, new IntField(num_delete));
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
