package zingg.block;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.shaded.com.google.common.hash.HashCode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import org.apache.spark.sql.Dataset;

import zingg.client.FieldDefinition;
import zingg.client.util.ListMap;
import zingg.hash.HashFunction;
import zingg.client.util.ColName;

import scala.collection.JavaConverters;
import scala.collection.Seq;


public class Canopy implements Serializable, Comparable<Canopy> {

	public static final Log LOG = LogFactory.getLog(Canopy.class);

	// created by function edge leading from parent to this node
	HashFunction function;
	// aplied on field
	FieldDefinition context;
	// list of duplicates passed from parent
	List<Row> dupeN;
	// number of duplicates eliminated after function applied on fn context
	long elimCount;
	// hash of canopy
	Object hash;
	// training set
	Dataset<Row> training;
	// duplicates remaining after function is applied
	List<Row> dupeRemaining;
	long trainingSize = -1;

	int index;

	public Canopy() {
	}

	public Canopy(Dataset<Row> training, List<Row> dupeN) {
		setTraining(training);

		this.dupeN = dupeN;
	}

	public Canopy(Dataset<Row> training, List<Row> dupeN, HashFunction function,
			FieldDefinition context) {
		this(training, dupeN);
		this.function = function;
		this.context = context;
		// prepare();
	}

	/**
	 * @return the function
	 */
	public HashFunction getFunction() {
		return function;
	}

	/**
	 * @param function
	 *            the function to set
	 */
	public void setFunction(HashFunction function) {
		this.function = function;
	}

	/**
	 * @return the context
	 */
	public FieldDefinition getContext() {
		return context;
	}

	/**
	 * @param context
	 *            the context to set
	 */
	public void setContext(FieldDefinition context) {
		this.context = context;
	}

	

	/**
	 * @return the dupeN
	 */
	public List<Row> getDupeN() {
		return dupeN;
	}

	/**
	 * @param dupeN
	 *            the dupeN to set
	 */
	public void setDupeN(List<Row> dupeN) {
		this.dupeN = dupeN;
	}

	/**
	 * @return the elimCount
	 */
	public long getElimCount() {
		return elimCount;
	}

	/**
	 * @param elimCount
	 *            the elimCount to set
	 */
	public void setElimCount(long elimCount) {
		this.elimCount = elimCount;
	}

	/**
	 * @return the hash
	 */
	public Object getHash() {
		return hash;
	}

	/**
	 * @param hash
	 *            the hash to set
	 */
	public void setHash(Object hash) {
		this.hash = hash;
	}

	/**
	 * @return the training
	 */
	public Dataset<Row> getTraining() {
		return training;
	}

	/**
	 * @param training
	 *            the training to set
	 */
	public void setTraining(Dataset<Row> training) {
		this.training = training.cache();
		//this.trainingSize = training.count();
	}

	public List<Canopy> getCanopies() {
		//long ts = System.currentTimeMillis();
		/*
		List<Row> newTraining = function.apply(training, context.fieldName, ColName.HASH_COL).cache();
		LOG.debug("getCanopies0" + (System.currentTimeMillis() - ts));
		List<Canopy> returnCanopies = new ArrayList<Canopy>();
		//first find unique hashes
		//then split the training into per hash
		List<Row> uniqueHashes = newTraining.select(ColName.HASH_COL).distinct().collectAsList();
		LOG.debug("getCanopies1" + (System.currentTimeMillis() - ts));
		for (Row row : uniqueHashes) {
			Object key = row.get(0);
			List<Row> tupleList = newTraining.filter(newTraining.col(ColName.HASH_COL).equalTo(key))
					.cache();
			tupleList = tupleList.drop(ColName.HASH_COL);
			Canopy can = new Canopy(tupleList, dupeRemaining);
			//LOG.debug(" canopy size is " + tupleList.count() + " for  hash "
			//		+ key);
			can.hash = key;
			returnCanopies.add(can);
		}
		LOG.debug("getCanopies2" + (System.currentTimeMillis() - ts));
		return returnCanopies;*/
		/*
		ListMap<Object, Row> hashes = new ListMap<Object, Row>();
		List<Canopy> returnCanopies = new ArrayList<Canopy>();
		
		for (Row r : training) {
			hashes.add(function.apply(r, context.fieldName), r);
		}
		for (Object o: hashes.keySet()) {
			Canopy can = new Canopy(hashes.get(o), dupeRemaining);
			can.hash = o;
			returnCanopies.add(can);
		}
		hashes = null;
		//LOG.debug("getCanopies2" + (System.currentTimeMillis() - ts));
		return returnCanopies;
		*/
		List<Canopy> returnCanopies = new ArrayList<Canopy>();
		Dataset<Row> newTraining = training.withColumn(ColName.HASH_COL, functions.callUDF(function.getName(), 
			training.col(context.fieldName))).cache();
		//List<Row> uniqueHashes = newTraining.select(ColName.HASH_COL).distinct().collectAsList();
		List<Row> uniqueHashes = newTraining.select(ColName.HASH_COL).groupBy(ColName.HASH_COL).agg(
			functions.count(ColName.HASH_COL)
			.alias("count")).collectAsList();
			//.filter("count>8").collectAsList();
		for (Row row : uniqueHashes) {
			Object key = row.get(0);
			Dataset<Row> tupleList = newTraining.filter(newTraining.col(ColName.HASH_COL).equalTo(key))
				.drop(ColName.HASH_COL);
			tupleList = tupleList.drop(ColName.HASH_COL);
			Canopy can = new Canopy(tupleList, dupeRemaining);
			//LOG.debug(" canopy size is " + tupleList.count() + " for  hash "
			//		+ key);
			can.hash = key;
			can.trainingSize = (long) row.get(1);
			returnCanopies.add(can);
		}
		return returnCanopies;	
	}
	
	

	public long getTrainingSize() {
		if (trainingSize == -1) {
			trainingSize = training.count();
		}
		return trainingSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String str = "";
		if (context != null) {
			str = "Canopy [function=" + function + ", context=" + context.fieldName
				+ ", elimCount=" + elimCount + ", hash=" + hash;
		}
		else {
			str = "Canopy [function=" + function + ", context=" + context
				+ ", elimCount=" + elimCount + ", hash=" + hash;
		}
		/*if (training != null) {
			str += ", training=" + trainingSize;
		}*/
		str += "]";
		return str;
	}

	

	public void estimateElimCount() {
		//long ts = System.currentTimeMillis();																																																																				
		//the function is applied to both columns
		//if hash is equal, they are not going to be eliminated
		//filter on hash equal and count 
		//LOG.debug("Applying " + function.getName() + " to " + context.fieldName);
		dupeRemaining = new ArrayList<Row>();
		for(Row r: dupeN) {
			Object hash1 = function.apply(r, context.fieldName);
			Object hash2 = function.apply(r, ColName.COL_PREFIX + context.fieldName);
			//LOG.debug("hash1 " + hash1);		
			//LOG.debug("hash2 " + hash2);
			if (hash1 == null && hash2 ==null) {
				dupeRemaining.add(r);
			}
			else if (hash1 != null && hash2 != null && hash1.equals(hash2)) {
				dupeRemaining.add(r);
				//LOG.debug("NOT eliminatin " );	
			}
			else {
				//LOG.debug("eliminatin " + r);		
			}
		}			
		elimCount = dupeN.size() - dupeRemaining.size();
		//LOG.debug("estimateElimCount" + (System.currentTimeMillis() - ts));
	}
	
	/*public ListMap<Object, Row> getHashForDupes(List<Row> d) {
		//dupeRemaining = new ArrayList<Row>();
		ListMap<Object, Row> returnMap = new ListMap<Object, Row>();
		for (Row pair : d) {
			Tuple first = pair.getFirst();
			Tuple second = pair.getSecond();
			Object hash1 = apply(first);
			Object hash2 = apply(second);
			if (hash1 == null) {
				if (hash2 != null) {
					//do nothing
				}
			} else {
				if (!hash1.equals(hash2)) {
					// LOG.info("Elimniation of " + pair);
					//do nothing
				} else {
					returnMap.add(hash1, pair);
				}
			}
		}
		return returnMap;
	}*/

	public Canopy copyTo(Canopy copyTo) {
		copyTo.function = function;
		copyTo.context = context;
		// list of duplicates passed from parent
		copyTo.dupeN = dupeN;
		// number of duplicates eliminated after function applied on fn context
		copyTo.elimCount = elimCount;
		// hash of canopy
		copyTo.hash = hash;
		// training set
		copyTo.training = training;
		// duplicates remaining after function is applied
		copyTo.dupeRemaining = dupeRemaining;
		return copyTo;
	}

	/**
	 * We will call this canopy's clear function to remove dupes, training and
	 * remaining data before we persist to disk this method is to be called just
	 * before
	 */
	public void clearBeforeSaving() {
		this.training = null;
		// this.elimCount = null;
		this.dupeN = null;
		this.dupeRemaining = null;
	}

	public int compareTo(Canopy other) {
		return (int) (this.elimCount - other.elimCount);
	}


	public static Canopy estimateCanopies(Dataset<Row> t, Map<Canopy, Long> canopies) {
		/*Collections.sort(canopies);
		//t.show(true);
		Row r = t.takeAsList(1).get(0);
		for (int i=0; i < canopies.size(); ++i) {
			if (((long) r.getAs("approx_count_distinct(" + ColName.HASH_COL + i + ")")) > 1) return canopies.get(i);
		}
		return null;		
		//return 2;
		*/
		List<Canopy> sortedCanopies = new ArrayList<Canopy>();
		sortedCanopies.addAll(canopies.keySet());
		Collections.sort(sortedCanopies);
		for (Canopy c: sortedCanopies) {
			LOG.debug("estimating for " + c + " with index " + c.index);
			int index = c.index;
			Dataset<Row> r = t.select(ColName.HASH_COL + index).groupBy(ColName.HASH_COL + index).agg(functions.count(ColName.HASH_COL+index)
				.alias("count"+index));
			if (r.count() > 0) {
				return c;
			}
		}
		return null;	

		/*
		newTraining.select(ColName.HASH_COL).groupBy(ColName.HASH_COL).agg(
			functions.count(ColName.HASH_COL)
			.alias("count"))
			.filter("count>100").collectAsList();*/
	}

	public static Dataset<Row> apply(Dataset<Row> t, Map<Integer, Canopy> canopies) {
		for (Integer i: canopies.keySet()) {	
			Canopy c = canopies.get(i);
			t = t.withColumn(ColName.HASH_COL + i, functions.callUDF(c.function.getName(), 
					t.col(c.context.fieldName)));
		}	
		return t;
	}

	

}
