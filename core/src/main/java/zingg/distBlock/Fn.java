package zingg.distBlock;

import java.io.Serializable;
import java.util.Objects;


import zingg.client.FieldDefinition;
import zingg.hash.HashFunction;

public class Fn implements Serializable {

    HashFunction function;
	// aplied on field
	FieldDefinition field;
    int index = -1;

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
    public String toString() {
        return "{" +
            " function='" + getFunction() + "'" +
            ", field='" + getField() + "'" +
            ", index='" + getIndex() + "'" +
            "}";
    }

}
