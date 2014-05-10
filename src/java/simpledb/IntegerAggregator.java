package simpledb;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
        else{
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
    	//with group by
    	if(m_gbfield != Aggregator.NO_GROUPING){
    		Field cur_field = tup.getField(m_gbfield);
    		//Aggregate field only if type matches
    		if(cur_field.getType() == m_gbfieldtype){
        		int cur_agg_value = ((IntField)tup.getField(m_afield)).getValue();
        		
        		//Aggregate AVG
        		if(m_what == Aggregator.Op.AVG){
        			if(agg_map.containsKey(cur_field)){
        				int cur_avg = agg_map.get(cur_field);
        				agg_map.put(cur_field, (cur_avg+cur_agg_value)/2);
        			}else{
        				agg_map.put(cur_field,cur_agg_value);
        			}
        		}
        		
        		//Aggregate MAX
        		else if(m_what == Aggregator.Op.MAX){
        			if(agg_map.containsKey(cur_field)){
        				int cur_max = agg_map.get(cur_field);
        				agg_map.put(cur_field, (cur_max>cur_agg_value ? cur_max : cur_agg_value));
        			}else{
        				agg_map.put(cur_field,cur_agg_value);
        			}
        		}
        		
        		//Aggregate MIN
        		else if(m_what == Aggregator.Op.MIN){
        			if(agg_map.containsKey(cur_field)){
        				int cur_min = agg_map.get(cur_field);
        				agg_map.put(cur_field, (cur_min<cur_agg_value ? cur_min : cur_agg_value));
        			}else{
        				agg_map.put(cur_field,cur_agg_value);
        			}
        		}
        		
        		//Aggregate SUM
        		else if(m_what == Aggregator.Op.SUM){
        			if(agg_map.containsKey(cur_field)){
        				int cur_sum = agg_map.get(cur_field);
        				agg_map.put(cur_field, cur_sum+cur_agg_value);
        			}else{
        				agg_map.put(cur_field,cur_agg_value);
        			}	
        		}
        		//Aggregate COUNT
        		else if(m_what == Aggregator.Op.COUNT){
        			if(agg_map.containsKey(cur_field)){
        				int cur_count = agg_map.get(cur_field);
        				agg_map.put(cur_field, cur_count+1);
        			}else{
        				agg_map.put(cur_field,cur_agg_value);
        			}
        		}	
    		}
    	}
    	//without group by
    	else{
    		int cur_agg_value = ((IntField)tup.getField(m_afield)).getValue();
    		
    		//Aggregate AVG
    		if(m_what == Aggregator.Op.AVG){
    			no_grouping_agg = (no_grouping_agg + cur_agg_value)/2;
    		}
    		
    		//Aggregate MAX
    		else if(m_what == Aggregator.Op.MAX){
    			no_grouping_agg = no_grouping_agg > cur_agg_value ? no_grouping_agg : cur_agg_value;
    		}
    		
    		//Aggregate MIN
    		else if(m_what == Aggregator.Op.MIN){
    			no_grouping_agg = no_grouping_agg < cur_agg_value ? no_grouping_agg : cur_agg_value;
    		}
    		
    		//Aggregate SUM
    		else if(m_what == Aggregator.Op.SUM){
    			no_grouping_agg += cur_agg_value;
    		}
    		//Aggregate COUNT
    		else if(m_what == Aggregator.Op.COUNT){
    			no_grouping_agg++;
    		}
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
    	if(m_gbfield == Aggregator.NO_GROUPING){
        	return new IntegerAggregatorIterator(no_grouping_agg);
    	}else{
        	return new IntegerAggregatorIterator(agg_map, m_gbfieldtype);
    	}
    }

}
