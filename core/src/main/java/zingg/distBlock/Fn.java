package zingg.distBlock;

import java.util.ArrayList;
import java.util.Objects;

import org.apache.spark.sql.Row;

import zingg.client.FieldDefinition;
import zingg.client.util.ColName;
import zingg.hash.HashFunction;

public class Fn {

    HashFunction function;
	// aplied on field
	FieldDefinition field;
    int index;

    public Fn(int index, FieldDefinition field, HashFunction function) {
        this.index = index;
        this.field = field;
        this.function = function; 
    }
    

    public Fn() {
    }

    public HashFunction getFunction() {
        return this.function;
    }

    public void setFunction(HashFunction function) {
        this.function = function;
    }

    public FieldDefinition getField() {
        return this.field;
    }

    public void setField(FieldDefinition field) {
        this.field = field;
    }

    public int getIndex() {
        return this.index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Fn function(HashFunction function) {
        setFunction(function);
        return this;
    }

    public Fn field(FieldDefinition field) {
        setField(field);
        return this;
    }

    public Fn index(int index) {
        setIndex(index);
        return this;
    }

    protected long estimateElimCount(Context ctx) {
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
		return ctx.getMatchingPairs().size() - dupeRemaining.size();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Fn)) {
            return false;
        }
        Fn fn = (Fn) o;
        return Objects.equals(function, fn.function) && Objects.equals(field, fn.field) && index == fn.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, field, index);
    }

    @Override
    public String toString() {
        return "{" +
            " function='" + getFunction() + "'" +
            ", field='" + getField() + "'" +
            ", index='" + getIndex() + "'" +
            "}";
    }

}
