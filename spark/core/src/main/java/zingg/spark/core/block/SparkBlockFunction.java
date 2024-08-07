package zingg.spark.core.block;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.collection.JavaConversions;
import scala.collection.Seq;
import zingg.common.core.block.BlockFunction;
import zingg.common.core.block.Canopy;
import zingg.common.core.block.Tree;

public class SparkBlockFunction extends BlockFunction<Row> implements MapFunction<Row, Row>{
   

    public SparkBlockFunction(Tree<Canopy<Row>> tree) {
        super(tree);
    }
   
    @Override
    public List<Object> getListFromRow(Row r) {
        Seq<Object> sObj = r.toSeq();
        List<Object> seqList = JavaConversions.seqAsJavaList(sObj);
        //the abstract list returned here does not support adding a new element, 
        //so an ugly way is to create a new list altogether (!!)
        //see in perf - maybe just iterate over all the row elements and add the last one?
        List<Object> returnList = new ArrayList<Object>(seqList.size()+1);
		returnList.addAll(seqList);
        return returnList;
    }

    @Override
    public Row getRowFromList(List<Object> lob) {
        return RowFactory.create(lob.toArray());		
    }
}

