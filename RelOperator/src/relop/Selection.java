package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

	private Iterator iterator;
	private Predicate[] predicates;

	// boolean variable to indicate whether the pre-fetched tuple is consumed or not
	private boolean nextTupleIsConsumed;
	
	// pre-fetched tuple
	private Tuple nextTuple;
	
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {	  
	  this.iterator = iter;
	  this.predicates = preds;
	  nextTuple = null;
	  nextTupleIsConsumed = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
//    TODO : Not sure what to do here?
	  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  iterator.restart();
	  nextTuple = null;
	  nextTupleIsConsumed = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iterator.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  iterator.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if (!nextTupleIsConsumed)
			return true;

	  while(iterator.hasNext()){
		  nextTuple = iterator.getNext();
		  for (int i = 0; i < predicates.length; i++){
				if (predicates[i].evaluate(nextTuple)) {
					nextTupleIsConsumed = false;
					return true;
				}		
		  }
	  }
	  return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if (!nextTupleIsConsumed || hasNext()){
		  nextTupleIsConsumed = true;
		  return nextTuple;
	  }
	  throw new IllegalStateException("No more tuples");
  }

} // public class Selection extends Iterator
