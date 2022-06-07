package zingg.distBlock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import org.apache.spark.sql.functions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;
import scala.collection.Seq;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.util.SchemaUtils;

import zingg.block.Tree;
import zingg.client.FieldDefinition;
import zingg.client.util.ColName;
import zingg.client.util.ListMap;
import zingg.hash.HashFunction;

public class BTreeBuilder {

    long maxSize;

    public BTreeBuilder(long maxSize) {
        this.maxSize = maxSize;
    }

    public static final Log LOG = LogFactory.getLog(BTreeBuilder.class);

    public Map<Integer, Fn> getAllFunctionsList(List<FieldDefinition> fieldsOfInterest, ListMap<DataType, HashFunction> functionsMap) {
        int i = 0;
        Map<Integer, Fn> fnToTry = new HashMap<Integer, Fn>();
        for (FieldDefinition field : fieldsOfInterest) {
            // applicable functions
            List<HashFunction> functions = functionsMap.get(field.getDataType());
            if (functions != null) {				
                for (HashFunction function : functions) {
                    Fn trial = new Fn(i, field, function);
                    fnToTry.put(i++, trial);
                }
            }
        }
        return fnToTry;
    }

    public Dataset<Row> apply(Dataset<Row> t, Map<Integer, Fn> fns) {
		for (Integer i: fns.keySet()) {	
			Fn fn = fns.get(i);
			t = t.withColumn(ColName.HASH_COL + i, functions.callUDF(fn.function.getName(), 
					t.col(fn.getField().fieldName)));
		}	
		return t.cache();
	}

    public Tree<BFn> getBlockingTree(List<FieldDefinition> fieldsOfInterest, ListMap<DataType, HashFunction> functionsMap, Context context) throws Exception {
			
        Map<Integer, Fn> fnToTry =  getAllFunctionsList(fieldsOfInterest, functionsMap);           
        Dataset<Row> functionsAppliedDS = apply(context.getDataSample(), fnToTry);
        context.setDataSample(functionsAppliedDS);
        return getBlockingTree(null, null, new BFn(), fnToTry, context);
    }

	public static StructType appendHashCol(StructType s) {
		StructType retSchema = SchemaUtils.appendColumn(s, ColName.HASH_COL, DataTypes.IntegerType, false);
		LOG.debug("returning schema after step 1 is " + retSchema);
		return retSchema;
	}


	/** bfn equality, move there */
	public boolean checkFunctionInNode(BFn node, Fn function) {
		if (node.getFunction() != null && function.getFunction() != null && node.getFunction().getName().equals(function.getFunction().getName())
				&& node.getField().fieldName.equals(function.getField().fieldName)) {
			return true;
		}
		return false;
	}

    public boolean isFunctionUsed(Tree<BFn> tree, BFn node, Fn function) {
		// //LOG.debug("Tree " + tree);
		// //LOG.debug("Node  " + node);
		// //LOG.debug("Index " + index);

		boolean isUsed = false;
		if (node == null || tree == null)
			return false;
		if (checkFunctionInNode(node, function))
			return true;
		Tree<BFn> nodeTree = tree.getTree(node);
		if (nodeTree == null)
			return false;

		Tree<BFn> parent = nodeTree.getParent();
		if (parent != null) {
			BFn head = parent.getHead();
			while (head != null) {
				// check siblings of node
				/*for (Tree<Canopy> siblings : parent.getSubTrees()) {
					Canopy sibling = siblings.getHead();
					if (checkFunctionInNode(sibling, index, function))
						return true;
				}*/
				// check parent of node
				return isFunctionUsed(tree, head, function);
			}
		}
		return isUsed;
	}

    public BFn getBestNode(Tree<BFn> tree, BFn parent, BFn node,
        Map<Integer, Fn> fnsToTry, Context context) throws Exception {
		FnResult least = new FnResult(Long.MAX_VALUE, null, -1);
		BFn best = null;
		for (Integer j : fnsToTry.keySet()) {
			Fn fn = fnsToTry.get(j);
			if (!isFunctionUsed(tree, node, fn)) {
						BFn bFnToTry = new BFn(fn, new FnResult());
						bFnToTry.estimateElimCount(context);
						bFnToTry.estimateChildren(context);
						//LOG.debug("Comparing " + bFnToTry.result  + " with  " + least);	
						if (bFnToTry.getResult().approxChildren >= 1) {
							if (bFnToTry.getResult().compareTo(least) > 0){		
								//LOG.debug(" new is better");	
								best = bFnToTry; 
								least = bFnToTry.getResult();
								//greedy, how much better can it get
								if (bFnToTry.getResult().getElimCount() == 0 ) return bFnToTry;
							}
							/*else {
								LOG.debug(" old is better ");
							}*/
						}//childess is of no use
											
			}
		}
		return best;
	}

    public Tree<BFn> getBlockingTree(Tree<BFn> tree, BFn parent,
				BFn node, Map<Integer, Fn> fnToTry, Context context) throws Exception {
			long size = context.getDataSampleSize();
			if (LOG.isDebugEnabled()) {
				LOG.debug("Size, maxSize " + size + ", " + maxSize);
			}
			if (context.getMatchingPairs()  != null && context.getMatchingPairsSize() > 0 && size > maxSize ) {
				//LOG.debug("Size is bigger ");
				BFn best = getBestNode(tree, parent, node, fnToTry, context);
				if (best != null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(" HashFunction is " + best + " and node is " + node);
					}
					node.copyFrom(best);
					if (tree == null && parent == null) {
						LOG.debug("building new tree ");
						tree = new Tree<BFn>(node);
					}
					
					List<Pair<BFn, Context>> canopies = getChildren(node, context);
					for (Pair<BFn, Context> n : canopies) {
						LOG.debug("adding child " + n.first.result.hash + " to " + node);
						tree.addLeaf(node, n.first);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Tree so far: ");
							LOG.debug(tree);			
							LOG.debug(" Finding for " + n.first);
						}							
						getBlockingTree(tree, node, n.first, fnToTry, n.second);
					}
				}
			} else {
				if ((context.getMatchingPairs()  == null) || (context.getMatchingPairs().size() == 0)) {
					LOG.warn("Ran out of training at size " + size + " for node " + node);
				}
				else {
					LOG.debug("Min size reached " + size + " for node " + node);
				}				
			}			
			LOG.debug(" Tree is ");
			LOG.debug(tree);
			return tree;
		}

		public List<Pair<BFn, Context>> getChildren(BFn fn, Context c) {
			
			List<Pair<BFn, Context>> returnCanopies = new ArrayList<Pair<BFn, Context>>();
			//c.getDataSample().show();
			//List<Row> uniqueHashes = newTraining.select(ColName.HASH_COL).distinct().collectAsList();
			List<Row> uniqueHashes = c.getDataSample().select(ColName.HASH_COL + fn.index).distinct().collectAsList();
				//.filter("count>8").collectAsList();
			for (Row row : uniqueHashes) {
				Object key = row.get(0);
				Dataset<Row> tupleList = c.getDataSample().filter(c.getDataSample().col(ColName.HASH_COL + fn.index).equalTo(key));
				Context can = new Context(tupleList, fn.estimateElimCount(c));
				FnResult result = new FnResult();
				result.hash = key;
				BFn baby = new BFn(result);
				returnCanopies.add(new Pair<BFn, Context> (baby, can));
			}
			return returnCanopies;	
		}

		public static StringBuilder applyTree(Row tuple, Tree<BFn> tree,
			BFn root, StringBuilder result) {
		if (root.function != null) {
			Object hash = root.function.apply(tuple, root.getField().fieldName);
			
			result = result.append("|").append(hash);
			for (BFn c : tree.getSuccessors(root)) {
				// LOG.info("Successr hash " + c.getHash() + " and our hash "+
				// hash);
				if (c != null) {
					// //LOG.debug("c.hash " + c.getHash() + " and our hash " + hash);
					if ((c.getResult().getHash() != null)) {
						//LOG.debug("Hurdle one over ");
						if ((c.getResult().getHash().equals(hash))) {
							// //LOG.debug("Hurdle 2 start " + c);
							applyTree(tuple, tree, c, result);
							// //LOG.debug("Hurdle 2 over ");
						}
					}
				}
			}
		}
		//LOG.debug("apply first step clustering result " + result);
		return result;
	}

	public static void printTree(Tree<BFn> tree,
			BFn root) {
		
		for (BFn c : tree.getSuccessors(root)) {
			printTree(tree, c);
		}			
	}
	
	public static class BlockFunction implements MapFunction<Row, Row> {
		
		Tree<BFn> tree;
		public BlockFunction(Tree<BFn> tree) {
			this.tree = tree;
		}
		
		@Override
		public Row call(Row r) {
			StringBuilder bf = new StringBuilder();
			bf = BTreeBuilder.applyTree(r, tree, tree.getHead(), bf);
			Seq<Object> s = r.toSeq();
			List<Object> seqList = JavaConversions.seqAsJavaList(s);
			List<Object> returnList = new ArrayList<Object>(seqList.size()+1);
			returnList.addAll(seqList);
			returnList.add(bf.toString().hashCode());
					
			return RowFactory.create(returnList.toArray());			
		}

	}

		

    

    
}
