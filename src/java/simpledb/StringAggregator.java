package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int m_gbfield;
    
    private Type m_gbfieldtype;
    
    private int m_afield;
    
    private Op m_what;
    
	private HashMap<Field,Integer> agg_map; //hashmap between group value and aggregate value
	
	private int no_grouping_agg;//integer  which stores aggregate value when no_grouping is specified


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        m_gbfield = gbfield;
        m_gbfieldtype = gbfieldtype;
        m_afield = afield;
        m_what = what;
        if(m_what != Op.COUNT){
        	throw new IllegalArgumentException();
        }
        else{
            if(m_gbfield == Aggregator.NO_GROUPING){
            	no_grouping_agg = 0;
            }
            else{
            	agg_map = new HashMap<Field, Integer>();
            }         	
        }
  
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	//without group by
    	if(m_gbfield == Aggregator.NO_GROUPING){
    		//make sure aggregate field is not null
			if(tup.getField(m_afield) != null){
				no_grouping_agg++;
			}
		}
		//with group by
		else{
			Field cur_field= tup.getField(m_gbfield);
			//proceed to aggregate only if type matches
			if(cur_field.getType() == m_gbfieldtype){
				if(agg_map.containsKey(cur_field)){
					agg_map.put(cur_field, agg_map.get(cur_field) + 1);
				}
				else{
					agg_map.put(cur_field, 1);
				}				
			}
		}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	if(m_gbfield == Aggregator.NO_GROUPING){
        	return new StringAggregatorIterator(no_grouping_agg);
    	}else{
        	return new StringAggregatorIterator(agg_map, m_gbfieldtype);
    	}
    }

}
