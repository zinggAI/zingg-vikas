package zingg.distBlock;

import java.util.ArrayList;
import java.util.Objects;

import org.apache.spark.sql.Row;

import zingg.client.FieldDefinition;
import zingg.client.util.ColName;
import zingg.hash.HashFunction;

public class BFn extends Fn{

    protected FnResult result = new FnResult();

    public BFn() {
        
    }

    public BFn(Fn f, FnResult result) {
        this(f.index, f.field, f.function, result);
    }

    public BFn(FnResult result) {
        this.result = result;
    }


    public BFn(int index, FieldDefinition field, HashFunction function, FnResult result) {
        super(index, field, function);
        this.result = result;
        //TODO Auto-generated constructor stub
    }


    public FnResult getResult() {
        return this.result;
    }

    public void setResult(FnResult result) {
        this.result = result;
    }

    public void copyFrom(BFn best) {
        //parent hash has to be preserved
        Object hash = this.getResult().getHash();
        this.setResult(best.getResult());
        this.getResult().setHash(hash);
        this.setFunction(best.getFunction());
        this.setField(best.getField());
        this.setIndex(best.getIndex());
    }



    @Override
    public String toString() {
        return "{" +
        " function='" + getFunction() + "'" +
        ", field='" + getField() + "'" +
        ", index='" + getIndex() + "'" +
        ", result='" + getResult() + "'" +
            "}";
    }

    protected ArrayList<Row> estimateElimCount(Context ctx) {
        ArrayList<Row> dupeRemaining = new ArrayList<Row>();
		for(Row r: ctx.getMatchingPairs()) {
			Object hash1 = function.apply(r, field.fieldName);
			Object hash2 = function.apply(r, ColName.COL_PREFIX + field.fieldName);
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
        this.result.setElimCount(ctx.getMatchingPairs().size() - dupeRemaining.size());
        return dupeRemaining;
    }

    public void estimateChildren(Context ctx) {
		result.approxChildren = ctx.getDataSample().select(ColName.HASH_COL + index).distinct().count();
        //result.approxChildren = 100;
	}

    

    

    
}
