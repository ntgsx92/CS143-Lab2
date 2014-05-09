package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    
    private Predicate m_predicate;
    
    private DbIterator m_child;
            
    public Filter(Predicate p, DbIterator child) {
    	m_predicate = p;
    	m_child = child;
    }

    public Predicate getPredicate() {
    	return m_predicate;
    }

    public TupleDesc getTupleDesc() {
    	return m_child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
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
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	try{
        	Tuple cur_tuple = m_child.next();
        	while(true){
        		if(m_predicate.filter(cur_tuple)){
        			return cur_tuple;
        		}
        		cur_tuple = m_child.next();
        	}
    	}catch(NoSuchElementException e){
    		return null;
    	}
    }
    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { m_child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        m_child = children[0];
    }

}
