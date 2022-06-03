package zingg.distBlock;

import java.util.ArrayList;

import org.apache.spark.sql.Row;

import zingg.client.FieldDefinition;
import zingg.client.util.ColName;
import zingg.hash.HashFunction;

public class BFn extends Fn{

    protected FnResult result;

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



    @Override
    public String toString() {
        return "{" +
        " function='" + getFunction() + "'" +
        ", field='" + getField().fieldName + "'" +
        ", index='" + getIndex() + "'" +
        ", result='" + getResult() + "'" +
            "}";
    }
    

    

    
}
