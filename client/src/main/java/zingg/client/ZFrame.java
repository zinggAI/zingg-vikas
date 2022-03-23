package zingg.client;

import java.util.List;
//Dataset, Row, column
public interface ZFrame<T, R, C> {
    
    public ZFrame<T, R, C> cache();
    public String[] columns();
    public ZFrame<T, R, C> select(C... cols);
    public ZFrame<T, R, C> select(List<C> cols);
    //public ZFrame<T, R, C> select(String... cols);
    public ZFrame<T, R, C> select(String col);
    public ZFrame <T, R, C> distinct();
    public List<R> collectAsList();

    public ZFrame<T,R,C> toDF(String[] cols);

    public ZFrame<T,R,C> join(ZFrame<T,R,C> lines1, String joinColumn);

    public ZFrame<T,R,C> joinRight(ZFrame<T,R,C> lines1, String joinColumn);
    

    public C col(String colname);
    
    public long count();

    public ZFrame<T,R,C> filter(C col);

    public T df();

    public ZFrame<T,R,C> withColumnRenamed(String s, String t);

    public ZFrame<T,R,C> dropDuplicates(String c, String d);

    public ZFrame<T,R,C> dropDuplicates(String[] c);

    public ZFrame<T,R,C> drop(String c);

    public ZFrame<T,R,C> union(ZFrame<T,R,C> other);

    public ZFrame<T,R,C> unionByName(ZFrame<T,R,C> other, boolean flag);

    public ZFrame<T,R,C> withColumn(String s, int c);

    public ZFrame<T,R,C> repartition(int num);

    public C gt(String c);

	public C equalTo(String c, String e);

	public C notEqual(String c, String e);
    
}