package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator{
	private TransactionId m_transid;
	
	private HeapFile m_heapf;
	
	private int cur_pgno;
	
	private Iterator<Tuple> tuple_it;
	
	private boolean isopen;
	
	
	public HeapFileIterator(TransactionId trans_id, HeapFile heap_f) throws DbException, TransactionAbortedException{
		m_heapf = heap_f;
		m_transid = trans_id;
		cur_pgno = 0;
		tuple_it = get_iter(cur_pgno);
		boolean isopen = false;
	}
	
    private Iterator<Tuple> get_iter(int pg_no) throws DbException, TransactionAbortedException{
        HeapPageId pid = new HeapPageId(m_heapf.getId(), pg_no);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(m_transid, pid, Permissions.READ_ONLY);
        return page.iterator();
    }
	
	public Tuple next() throws NoSuchElementException, TransactionAbortedException,DbException {
		if(hasNext()){
			return tuple_it.next();
		}
		else{
			throw new NoSuchElementException();
		}
		
	}
	
	public boolean hasNext() throws DbException, TransactionAbortedException{
		if(!isopen){
			return false;
		}
		if(tuple_it.hasNext()){
			return true;
		}
		else if(cur_pgno < m_heapf.numPages() - 1 ){
			cur_pgno++;
			tuple_it = get_iter(cur_pgno);
			return tuple_it.hasNext();
		}
		else{
			return false;
		}
		
	}
	
    public void rewind() throws TransactionAbortedException, DbException {
        cur_pgno = 0;
        tuple_it = get_iter(0);
    }
	
	public void open() throws DbException, TransactionAbortedException{
		isopen = true;
	}
	
	public void close(){
		isopen = false;
	}
}
