package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple>{
	
	private HeapPage m_heappage;
	
	private int m_cur_tuple;
	
	private int m_size;
	
	public HeapPageIterator(HeapPage heap_page){
		m_heappage = heap_page;
		m_size =heap_page.getNumOccupySlots();
		m_cur_tuple = 0;
	}
	
	public boolean hasNext(){
		return (m_cur_tuple < m_size);
	}
	
	public Tuple next(){
		return m_heappage.tuples[m_cur_tuple++];
	}
	
	public void remove() throws UnsupportedOperationException{
		throw new UnsupportedOperationException("Not implemented!");
	}
	
	
}
