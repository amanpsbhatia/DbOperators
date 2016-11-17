package relop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;

public class HashJoin extends Iterator {

	private static int DEFAULAT_HASH_VALUE = -1;
	private Iterator outer;
	private Iterator inner;
	private HeapFile outerTempFile;
	private HeapFile innerTempFile;
	private HashIndex outerTempIndex;
	private HashIndex innerTempIndex;
	private Integer lcol;
	private Integer rcol;
	private IndexScan outerScan;
	private IndexScan innerScan;
	private HashTableDup hashTable;
	
	private int currentHash = -1;
	private LinkedList<Tuple> matchingOuterTuples;
	private Tuple innerTuple;
	private Tuple nextTuple;
	private boolean nextTupleIsConsumed;
	

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public HashJoin(Iterator left, Iterator right, java.lang.Integer lcol, java.lang.Integer rcol){

		this.outer = left;
		this.inner = right;
		this.lcol = lcol;
		this.rcol = rcol;
		this.schema = Schema.join(left.schema, right.schema);

		prepareOuterIndexScan();
		prepareInnerIndexScan();		
		currentHash = DEFAULAT_HASH_VALUE;
		try {
			loadNextBucket();
		} catch (IllegalStateException e) {
			// The outer hash is empty
			
		}
		
		nextTupleIsConsumed = true;		
		
	}
	
	private void loadNextBucket()throws IllegalStateException{
				
		while(outerScan.hasNext() && outerScan.getNextHash() == currentHash){
			outerScan.getNext();
		}		
		if (!outerScan.hasNext()){
			throw new IllegalStateException("No more elements present to read");
		}
		currentHash = outerScan.getNextHash();
		hashTable = new HashTableDup();
		
		while(outerScan.hasNext()){
			int nextHash = outerScan.getNextHash();
			if (nextHash != currentHash){
				break;
			}			
			Tuple next = outerScan.getNext();
			SearchKey key = outerScan.getLastKey();
			hashTable.add(key, next);			
		}
	}

	private void prepareOuterIndexScan(){	
		Iterator iter = outer;
		Integer col = lcol;
		if ((IndexScan.class).isInstance(iter)){
			outerScan = (IndexScan)iter;
		}
		else if ((FileScan.class).isInstance(iter)){
			FileScan fileScan = (FileScan)iter;
			outerTempIndex = new HashIndex("temp_outer_"+fileScan.getFile().toString());			
			fileScan.restart();
			while(fileScan.hasNext()){
				Tuple tuple = fileScan.getNext();
				SearchKey key = new SearchKey(tuple.getField(col));
				outerTempIndex.insertEntry(key, fileScan.getLastRID());
			}
			outerScan = new IndexScan(iter.schema, outerTempIndex, fileScan.getFile());
		}
		else {
			outerTempFile = new HeapFile("temp_"+iter.hashCode());
			outerTempIndex = new HashIndex("temp_"+outerTempFile.toString());
			iter.restart();
			while(iter.hasNext()){
				Tuple tuple = iter.getNext();
				SearchKey key = new SearchKey(tuple.getField(col));
				RID rid = outerTempFile.insertRecord(tuple.getData());
				outerTempIndex.insertEntry(key, rid);
			}
			outerScan = new IndexScan(outer.schema, outerTempIndex, outerTempFile);
		}
		
	}
	
	private void prepareInnerIndexScan(){
		Iterator iter = inner;
		Integer col = rcol;
		if ((IndexScan.class).isInstance(iter)){
			innerScan = (IndexScan)iter;
		}
		else if ((FileScan.class).isInstance(iter)){
			FileScan fileScan = (FileScan)iter;
			innerTempIndex = new HashIndex("temp_inner_"+fileScan.getFile().toString());			
			fileScan.restart();
			while(fileScan.hasNext()){
				Tuple tuple = fileScan.getNext();
				SearchKey key = new SearchKey(tuple.getField(col));
				innerTempIndex.insertEntry(key, fileScan.getLastRID());
			}
			innerScan = new IndexScan(iter.schema, innerTempIndex, fileScan.getFile());
		}
		else {
			innerTempFile = new HeapFile("temp_"+iter.hashCode());
			innerTempIndex = new HashIndex("temp_"+innerTempFile.toString());
			iter.restart();
			while(iter.hasNext()){
				Tuple tuple = iter.getNext();
				SearchKey key = new SearchKey(tuple.getField(col));
				RID rid = innerTempFile.insertRecord(tuple.getData());
				innerTempIndex.insertEntry(key, rid);
			}
			innerScan = new IndexScan(inner.schema, innerTempIndex, innerTempFile);
		}
	}
	
	@Override
	public void explain(int depth) {
		for (int i=0; i <= depth; i++){
			  System.out.print("\t");
		  }
		  System.out.print("HashJoin");
		  System.out.println();
		  outer.explain(depth+1);
		  inner.explain(depth+1);				
	}

	@Override
	public void restart() {
		close();
		prepareOuterIndexScan();
		prepareInnerIndexScan();		
		try {
			loadNextBucket();
		} catch (IllegalStateException e) {
			// The outer hash is empty
			
		}		
	}

	@Override
	public boolean isOpen() {
		if (outerScan.isOpen())
			return true;
		return false;
	}

	@Override
	public void close() {
		if (outer != null && outer.isOpen())
			outer.close();
		if (inner != null && inner.isOpen())
			inner.close();
		if (outerScan != null && outerScan.isOpen())
			outerScan.close();
		if (innerScan != null && innerScan.isOpen())
			innerScan.close();
		if (innerTempIndex != null){
			innerTempIndex.deleteFile();
			innerTempIndex = null;
		}
		if (outerTempIndex != null){
			outerTempIndex.deleteFile();
			outerTempIndex = null;
		}
		if (innerTempFile != null){
			innerTempFile.deleteFile();
			innerTempFile = null;
		}
		if (outerTempFile != null){
			outerTempFile.deleteFile();
			outerTempFile = null;
		}
		
	}

	@Override
	public boolean hasNext() {
		if (!nextTupleIsConsumed)
			return true;
		nextTuple = prefetchNextTuple();
		if (nextTuple == null)
			return false;
		else{
			nextTupleIsConsumed = false;
			return true;
		}
	}

	@Override
	public Tuple getNext() {
		if (!nextTupleIsConsumed || hasNext()){
			  nextTupleIsConsumed = true;
			  return nextTuple;
		  }
		  throw new IllegalStateException("No more tuples"); 
	}
	
	
	public Tuple prefetchNextTuple() {
		Tuple nextTuple = null;		
		if (matchingOuterTuples != null && !matchingOuterTuples.isEmpty()){
			Tuple outerTuple = matchingOuterTuples.getFirst();
			matchingOuterTuples.removeFirst();
			nextTuple = Tuple.join(outerTuple, innerTuple, this.schema);
			return nextTuple;
		}
		
		while(innerScan.hasNext()){
			int nextHash = innerScan.getNextHash();			
			if (nextHash == currentHash){
				//Same bucket
				innerTuple = innerScan.getNext();
				SearchKey key = innerScan.getLastKey();		
				if (!hashTable.containsKey(key)){
					innerScan.getNext();
					continue;
				}					
				matchingOuterTuples = new LinkedList<Tuple>(Arrays.asList(hashTable.getAll(key)));
				Tuple outerTuple = matchingOuterTuples.getFirst();
				matchingOuterTuples.removeFirst();
				nextTuple = Tuple.join(outerTuple, innerTuple, schema);
				return nextTuple;
			}
			else if (nextHash < currentHash){
				//The outer file does not contain the hash with value nextHash, so omit and get the next record
				innerScan.getNext();
				continue;
			}
			else {
				//The outerScan should load the next bucket
				//Note we are not moving the inner loop here. So the loop will fetch the same value again. It will break when outer scan has no buckets.				
				try{
					loadNextBucket();	
				}
				catch (IllegalStateException e) {
					break;
				}
								
			}
		}
		return nextTuple;
		
	}	
}
