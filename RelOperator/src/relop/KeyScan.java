package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

		private HashIndex index;
		private SearchKey key;
		private HeapFile file;
		private HashScan scan;
		
	
  /**
   * Constructs an index scan, given the hash index and schema.
   */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		  super();
		this.schema = schema;
		this.index = index;
		this.key = key;
		this.file = file;
		scan = index.openScan(key);
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
    scan = index.openScan(key);
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
	  RID rid = scan.getNext();
	  byte[] data = file.selectRecord(rid);
	  return new Tuple(schema, data);
  }

} // public class KeyScan extends Iterator
