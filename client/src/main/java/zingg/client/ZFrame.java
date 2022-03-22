package zingg.client;

import java.util.List;
//Dataset, Row, column
public interface ZFrame<T, R, C> {
    
    public ZFrame<T, R, C> cache();
    public String[] columns();
    public ZFrame<T, R, C> select(C... cols);
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
    
}