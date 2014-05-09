package simpledb;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	
	private int m_gbfield;
	
	private Type m_gbfieldtype;
	
	private int m_afield;
	
	private Op m_what;
	
	private HashMap<Field,Integer> agg_map; //hashmap between groupvalue and aggregatevalue
	
	private int no_grouping_agg;//integer  which stores aggregate value when no_grouping is specified
	
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        m_gbfield = gbfield;
        m_gbfieldtype = gbfieldtype;
        m_afield = afield;
        m_what = what;
        if(m_gbfield != Aggregator.NO_GROUPING){
        	agg_map = new HashMap<Field,Integer>();
        }
        else if(m_gbfield == Aggregator.NO_GROUPING){
        	no_grouping_agg = 0;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	//no grouping
    	if(m_gbfield == Aggregator.NO_GROUPING){
    	}
    	//with group by
    	else{

    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

}
