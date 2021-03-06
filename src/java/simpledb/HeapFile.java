package simpledb;

import java.io.*;
import java.util.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.channels.ByteChannel;
import java.nio.ByteBuffer;


/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private File m_file;
    private TupleDesc m_td;
	private HashMap<Integer, Boolean> m_free;
	private FileChannel m_channel;
    
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        m_file = f;
        m_td = td;
        m_free = new HashMap<Integer, Boolean>();
        try{
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        m_channel = file.getChannel();
    	int i = 0;
       	while(i < numPages()) {
        	m_free.put(i, false);
        	i++;
        }
        }
        catch(IOException e){
        	e.printStackTrace();
            System.exit(0);
        }

        
    }
    public File getFile() {
        return m_file;
    }

    private synchronized HeapPage nextFreePg(TransactionId tid)
    	throws DbException, TransactionAbortedException {
    	assert (numPages() == m_free.size());
    	int i = 0;
    	while( i < this.numPages()){
    		if (m_free.get(i)) {
    			HeapPageId pid = new HeapPageId(this.getId(), i);
    	    	BufferPool bp = Database.getBufferPool();
    			return (HeapPage) bp.getPage(tid,  pid, Permissions.READ_ONLY);
    		} 
    		i++;
    	}
    	
    	HeapPage newPage = null;
    	HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
    	byte[] data = HeapPage.createEmptyPageData();
    	try {
    		newPage = new HeapPage(pid, data);
    		writePage(newPage);
    	} catch (IOException e) {
    		 throw new DbException("Can not create new HeapPage");
    	}
    	m_free.put(newPage.getId().pageNumber(), true);
    	return newPage;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return m_file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return m_td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
        byte[] b = new byte[BufferPool.PAGE_SIZE];
        try{
            InputStream is = new FileInputStream(m_file);
            is.skip(offset);
            is.read(b, 0, BufferPool.PAGE_SIZE);
            is.close();
            return new HeapPage((HeapPageId)pid, b);
        }catch(IOException ioe){
            ioe.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	int num = page.getId().pageNumber();
    	boolean free;
    	if (((HeapPage) page).getNumEmptySlots() > 0)
    		free = true;
    	else free = false;
    	m_free.put(num,free);
    	try {
    		ByteBuffer b = ByteBuffer.wrap(page.getPageData());
    		int offset = num* BufferPool.PAGE_SIZE;
    		m_channel.write(b, offset);
    	}catch(IOException e){
            e.printStackTrace();
            System.exit(0);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    try {
		int pageCount = (int) Math.ceil(this.m_channel.size() / BufferPool.PAGE_SIZE);
		return pageCount;
	} catch (IOException e)
	{
    	e.printStackTrace();
        System.exit(0);
        return -1;
    }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	BufferPool pool = Database.getBufferPool();
    	HeapPage freePage = (HeapPage) pool.getPage(tid, nextFreePg(tid).getId(), Permissions.READ_WRITE);
    	
    	if (freePage.getNumEmptySlots()>0)
    	{
    		freePage.insertTuple(t);
    		m_free.put(nextFreePg(tid).getId().pageNumber(), freePage.hasFreeSlots());
    		ArrayList<Page> modifiedPages = new ArrayList<Page>();
    		freePage.markDirty(true,  tid);
    		modifiedPages.add(freePage);
    		return modifiedPages;
    	}
    	throw new DbException("No space to insert tuple");
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    		RecordId record = t.getRecordId();
    		BufferPool bp = Database.getBufferPool();
    		HeapPage pg = (HeapPage) bp.getPage(tid,  record.getPageId(), Permissions.READ_WRITE);
    		pg.markDirty(true,  tid);
    		boolean free;
    		if(pg.getNumEmptySlots()> 0)
    			free = true;
    		else free = false;
    		m_free.put(pg.getId().pageNumber(), free);
    		pg.deleteTuple(t);
    		ArrayList<Page> dirtyPgs = new ArrayList<Page>();
    		dirtyPgs.add(pg);
    		return dirtyPgs;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    /**
     * Helper class that implements the Java Iterator for tuples on a HeapFile
     */
    class HeapFileIterator extends AbstractDbFileIterator {

    	/**
    	 * An iterator to tuples for a particular page.
    	 */
        Iterator<Tuple> m_tupleIt;
       
        /**
         * The current number of the page this class is iterating through.
         */
        int m_currentPageNumber;

        /**
         * The transaction id for this iterator.
         */
        TransactionId m_tid;
        
        /**
         * The underlying heapFile.
         */
        HeapFile m_heapFile;

        /**
         * Set local variables for HeapFile and Transactionid
         * @param hf The underlying HeapFile.
         * @param tid The transaction ID.
         */
        public HeapFileIterator(HeapFile hf, TransactionId tid) {            
        	m_heapFile = hf;
            m_tid = tid;
        }

        /**
         * Open the iterator, must be called before readNext.
         */
        public void open() throws DbException, TransactionAbortedException {
            m_currentPageNumber = -1;
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            
        	// If the current tuple iterator has no more tuples.
        	if (m_tupleIt != null && !m_tupleIt.hasNext()) {	
                m_tupleIt = null;
            }

        	// Keep trying to open a tuple iterator until we find one of run out of pages.
            while (m_tupleIt == null && m_currentPageNumber < m_heapFile.numPages() - 1) {
                m_currentPageNumber++;		// Go to next page.
                
                // Get the iterator for the current page
                HeapPageId currentPageId = new HeapPageId(m_heapFile.getId(), m_currentPageNumber);
                                
                HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(m_tid,
                        currentPageId, Permissions.READ_ONLY);
                m_tupleIt = currentPage.iterator();
                
                // Make sure the iterator has tuples in it
                if (!m_tupleIt.hasNext())
                    m_tupleIt = null;
            }

            // Make sure we found a tuple iterator
            if (m_tupleIt == null)
                return null;
            
            // Return the next tuple.
            return m_tupleIt.next();
        }

        /**
         * Rewind closes the current iterator and then opens it again.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Close the iterator, which resets the counters so it can be opened again.
         */
        public void close() {
            super.close();
            m_tupleIt = null;
            m_currentPageNumber = Integer.MAX_VALUE;
        }
    }

}


