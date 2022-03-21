package zingg.client;

import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

//Dataset, Row, column
public class SparkFrame implements ZFrame<Dataset<Row>, Row, Column> {

    Dataset<Row> df;

    public SparkFrame(Dataset<Row> df) {
        this.df = df;
    }
    
    public ZFrame<Dataset<Row>, Row, Column> cache() {
        return new SparkFrame(df.cache());
    }

    public String[] columns() {
        return df.columns();
    }

    public ZFrame<Dataset<Row>, Row, Column> select(Column... cols) {
        return new SparkFrame(df.select(cols));
    }

    /*
    public ZFrame<Dataset<Row>, Row, Column> select(String... cols){
        return new SparkFrame(df.select(cols));
    }
    */
    
    public ZFrame<Dataset<Row>, Row, Column> select(String col) {
        return new SparkFrame(df.select(col));
    }

    public ZFrame<Dataset<Row>, Row, Column> distinct() {
        return new SparkFrame(df.distinct());
    }
    public List<Row> collectAsList() {
        return df.collectAsList();
    }
}