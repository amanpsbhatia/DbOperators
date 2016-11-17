package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	  private Iterator iterator;
	  private Integer[] fields;
	  
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
	  this.iterator = iter;
	  this.fields = fields;
	  schema = new Schema(fields.length);
	  Schema oldSchema = iter.schema;
	  for (int i=0; i<fields.length; i++ ){
		  int fieldId = fields[i];
		  schema.initField(i, oldSchema, fieldId);
	  }
	  
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {

	  for (int i=0; i <= depth; i++){
		  System.out.print("\t");
	  }
	  System.out.print("Projection");
	  System.out.println();
	  iterator.explain(depth+1);
	  
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  iterator.restart();
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
	  return iterator.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext()throws IllegalStateException {
	  Tuple tuple, newTuple;	  
	  tuple = iterator.getNext();
	  newTuple = new Tuple(schema);
	  for (int i=0; i<fields.length; i++ ){
		  newTuple.setField(i, tuple.getField(fields[i]));
	  }
	  return newTuple;	 	  
  }

} // public class Projection extends Iterator
