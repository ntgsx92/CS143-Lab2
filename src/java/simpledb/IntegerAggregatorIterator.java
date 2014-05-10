package simpledb;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class IntegerAggregatorIterator implements DbIterator{
	
	private HashMap<Field,Integer> m_tuple_map;
	
	private int m_nogrouping_agg;
	
	private boolean isopen;
	
	private boolean group_by;
	
	private Iterator it;
		
	private TupleDesc no_gb_td;
	
	private TupleDesc gb_td;
	
	private boolean no_gb_it;
	
	public IntegerAggregatorIterator(HashMap<Field,Integer> map, Type groupby_type){
		group_by = true;
		m_tuple_map = map;
		it = m_tuple_map.entrySet().iterator();
		Type [] agg_type = {groupby_type, Type.INT_TYPE};
		gb_td = new TupleDesc(agg_type,null);	
	}
	
	public IntegerAggregatorIterator(int no_grouping_agg){
		group_by = false;
		m_nogrouping_agg = no_grouping_agg;
		Type[] agg_type = {Type.INT_TYPE};
		no_gb_td = new TupleDesc(agg_type,null);
		no_gb_it = false;
	}
	
	public void open(){
		isopen = true;
	}
	
	public void close(){
		isopen = false;
	}
	
	public void rewind(){
		if(group_by){
			it = m_tuple_map.entrySet().iterator();
		}else{
			no_gb_it = false;
		}
	}
	
	public Tuple next(){
		if(group_by){
			if(hasNext()){
				Tuple new_agg_tuple = new Tuple(gb_td);
				Map.Entry<Field, Integer> cur = (Map.Entry<Field, Integer>)it.next();
				Field cur_gb_value = cur.getKey();
				Field cur_agg_value = new IntField(cur.getValue());
				new_agg_tuple.setField(0, cur_gb_value);
				new_agg_tuple.setField(1, cur_agg_value);
				return new_agg_tuple;
			}
			else{
				throw new NoSuchElementException();
			}
		}
		else{
			no_gb_it = true;
			Tuple no_gb_agg_tuple = new Tuple(no_gb_td);
			no_gb_agg_tuple.setField(0, new IntField(m_nogrouping_agg));
			return no_gb_agg_tuple;
		}
	}
	
	public TupleDesc getTupleDesc() {
		if(group_by){
			return gb_td;
		}else{
			return no_gb_td;
		}
	}
	
	public boolean hasNext() throws NoSuchElementException{
		if(group_by){
			return it.hasNext();
		}else{
			return !no_gb_it;
		}
	}
}
