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
}