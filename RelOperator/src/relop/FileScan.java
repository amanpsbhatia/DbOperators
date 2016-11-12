package relop;

import java.io.File;

import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
	
	private HeapFile file;
	private HeapScan scan;
	private RID lastRID;
	
  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
	  super();
	  this.schema = schema;
	  scan = file.openScan();
	  this.file = file;
	  lastRID = new RID(new PageId(GlobalConst.INVALID_PAGEID), -1);
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  scan.close();
	  scan = file.openScan();
	  lastRID = new RID(new PageId(GlobalConst.INVALID_PAGEID), -1);
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return !(scan == null);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  if (scan != null)
		  scan.close();
	  scan = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {	  
	  if (scan == null)
		  return false;
	  return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext()throws IllegalStateException {
	  if (scan == null)
		  return null;	 
	  lastRID = new RID();
	  byte[] data = scan.getNext(lastRID);
	  return new Tuple(schema, data);
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
	  return lastRID;
  }

} // public class FileScan extends Iterator
