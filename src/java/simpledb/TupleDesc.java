package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return null;
    }

    private static final long serialVersionUID = 1L;
    
    /*
     * private member which stores number of fields
     */
    private int fields_num;
    
    /*
     * private member which stores array of all the TDItems
    */
    private TDItem[] td_items;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        fields_num = typeAr.length;
    	td_items = new TDItem[fields_num];
        for(int i = 0; i < fields_num; i++){
        	td_items[i] = new TDItem(typeAr[i],fieldAr[i]);
        }       
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	fields_num = typeAr.length;
    	td_items = new TDItem[fields_num];
    	for(int i = 0; i < fields_num; i++){
    		td_items[i] = new TDItem(typeAr[i],null);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fields_num;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i >= fields_num){
        	throw new NoSuchElementException();
        }
        return td_items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i >= fields_num){
    		throw new NoSuchElementException();
    	}
        return td_items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if(name == null){
    		throw new NoSuchElementException();
    	}
        for(int i = 0; i < fields_num; i++){
        	if(td_items[i].fieldName!=null && td_items[i].fieldName.equals(name)){
        		return i;
        	}
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int totalsize = 0;
    	for(int i = 0; i < fields_num; i++){
    		if(td_items[i].fieldType == Type.INT_TYPE){
    			totalsize += Type.INT_TYPE.getLen();
    		}
    		else if(td_items[i].fieldType == Type.STRING_TYPE){
    			totalsize += Type.STRING_TYPE.getLen();
    		}
    	}
    	return totalsize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int new_num_fields = td1.numFields() + td2.numFields();
    	Type[] new_typeAr = new Type[new_num_fields];
    	String[] new_stringAr = new String[new_num_fields];
    	for(int i = 0; i < td1.numFields(); i++){
    		new_typeAr[i] = td1.td_items[i].fieldType;
    		new_stringAr[i] = td1.td_items[i].fieldName;
    	}
    	for(int j = td1.numFields(); j < new_num_fields; j++){
    		new_typeAr[j] = td2.td_items[j-td1.numFields()].fieldType;
    		new_stringAr[j] = td2.td_items[j-td1.numFields()].fieldName;
    	}
    	return new TupleDesc(new_typeAr,new_stringAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if(o == null || o.getClass() != TupleDesc.class){
    		return false;
    	}
    	if((((TupleDesc)o).numFields() != this.numFields())){
    		return false;
    	}
    	for(int i = 0; i < this.numFields(); i++){
    		if(((TupleDesc)o).td_items[i].fieldType != this.td_items[i].fieldType){
    			return false;
    		}
    	}
    	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String output = new String();
        for(int i = 0; i < this.numFields(); i++){
        	output += td_items[i].fieldType + "(" + td_items[i].fieldName + ")";
        }
        return output;
    }
}
