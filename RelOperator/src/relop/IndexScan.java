package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

	private HashIndex index;
	private HeapFile file;
	private BucketScan scan;
	
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  super();
	  this.schema = schema;
		this.index = index;
		this.file = file;
		scan = index.openScan();
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
	  close();
	  scan = index.openScan();
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
  public Tuple getNext() {
	  if (scan == null)
		  return null;	 
	  RID rid = scan.getNext();
	  byte[] data = file.selectRecord(rid);
	  return new Tuple(schema, data);
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  if (scan == null)
		  return null;
	  return scan.getLastKey();    
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  return scan.getNextHash();
  }

} // public class IndexScan extends Iterator
