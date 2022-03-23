package zingg.client;

import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import scala.collection.JavaConverters;
import zingg.client.util.ColName;

import org.apache.spark.sql.Dataset;

//Dataset, Row, column
public class SparkFrame implements ZFrame<Dataset<Row>, Row, Column> {

    public Dataset<Row> df;

    public SparkFrame(Dataset<Row> df) {
        this.df = df;
    }

    public Dataset<Row> df() {
        return df;
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

    
    public ZFrame<Dataset<Row>, Row, Column> select(List<Column> cols){
        return new SparkFrame(df.select(JavaConverters.asScalaIteratorConverter(cols.iterator()).asScala().toSeq()));
    }
    
    
    public ZFrame<Dataset<Row>, Row, Column> select(String col) {
        return new SparkFrame(df.select(col));
    }

    public ZFrame<Dataset<Row>, Row, Column> distinct() {
        return new SparkFrame(df.distinct());
    }
    public List<Row> collectAsList() {
        return df.collectAsList();
    }

    public ZFrame<Dataset<Row>, Row, Column> toDF(String[] cols) {
        return new SparkFrame(df.toDF(cols));
    }
    
    public ZFrame<Dataset<Row>, Row, Column> join(ZFrame<Dataset<Row>, Row, Column> lines1, String joinColumn) {
        return new SparkFrame(df.join(lines1.df(), df.col(joinColumn).equalTo(lines1.df().col(ColName.COL_PREFIX + joinColumn))));
    }

    public ZFrame<Dataset<Row>, Row, Column> joinRight(ZFrame<Dataset<Row>, Row, Column> lines1, String joinColumn) {
        return new SparkFrame(df.join(lines1.df(), df.col(joinColumn).equalTo(lines1.df().col(ColName.COL_PREFIX + joinColumn)), "right"));
    }

    public Column col(String colName) {
        return df.col(colName);
    }

    public long count() {
        return df.count();
    }

    public ZFrame<Dataset<Row>, Row, Column> filter(Column col) {
        return new SparkFrame(df.filter(col));
    }

    public ZFrame<Dataset<Row>, Row, Column> withColumnRenamed(String s, String t) {
        return new SparkFrame(df.withColumnRenamed(s, t));

    }

    public ZFrame<Dataset<Row>, Row, Column> dropDuplicates(String c, String d) {
        return new SparkFrame(df.dropDuplicates(c, d));
    }

    public ZFrame<Dataset<Row>, Row, Column> drop(String c) {
        return new SparkFrame(df.drop(c));
    }


    public ZFrame<Dataset<Row>, Row, Column> dropDuplicates(String[] c) {
        return new SparkFrame(df.dropDuplicates(c));
    }


    public ZFrame<Dataset<Row>, Row, Column> union(ZFrame<Dataset<Row>, Row, Column> other) {
        return new SparkFrame(df.union(other.df()));
    }

    public ZFrame<Dataset<Row>, Row, Column> unionByName(ZFrame<Dataset<Row>, Row, Column> other, boolean flag) {
        return new SparkFrame(df.unionByName(other.df(), flag));
    }

    public ZFrame<Dataset<Row>, Row, Column> withColumn(String s, int c){
        return new SparkFrame(df.withColumn(s, functions.lit(c)));
    }

    public ZFrame<Dataset<Row>, Row, Column> repartition(int nul){
        return new SparkFrame(df.repartition(nul));
    }

    public Column gt(String c) {
		return df.col(c).gt(df.col(ColName.COL_PREFIX + c));
	}

	public Column equalTo(String c, String e){
		return df.col(c).equalTo(e);
	}

	public Column notEqual(String c, String e) {
		return df.col(c).notEqual(e);
	}

}