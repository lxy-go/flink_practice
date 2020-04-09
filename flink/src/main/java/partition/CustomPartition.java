package partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区规则
 *
 * @author lixiyan
 * @date 2020/4/7 3:06 PM
 */
public class CustomPartition implements Partitioner<Long>{
    @Override
    public int partition(Long key, int i) {
        System.out.println("总分区数："+i);
        return Integer.parseInt(String.valueOf(key%2));
    }
}
