package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    
    /*
     * private member which represents the record id of a tuple
     */
    private RecordId m_recordid;
    
    /*
     * private member which maintains Field objects
     */
    private Field m_fields[];
    
    /*
     * private member which represents a tupledesc of a tuple
     */
    private TupleDesc m_td;
    
    public Tuple(TupleDesc td) {
        m_td = td;
        m_fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
    	return m_td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
    	return m_recordid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        m_recordid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
       if(i >= m_td.numFields()){
    	   return;
       }
       m_fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
    	if(i >= m_td.numFields()){
    		return null;
    	}
    	return m_fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	String result = new String();
    	for(int i = 0; i < m_td.numFields(); i++){
    		if( i == m_td.numFields() - 1){
    			result += m_fields[i].toString() + "\n";
    		}
    		else{
    			result += m_fields[i].toString() + "\t";
    		}
    	}
        return result;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
    	List<Field> arrAsList = Arrays.asList(m_fields);
    	Iterator<Field> it = arrAsList.iterator();
        return it;
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
    	m_td =td;
    }
}
