package partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区测试
 *
 * @author lixiyan
 * @date 2020/4/7 3:09 PM
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple2<Long, String>> text = env.fromElements(Tuple2.of(1L, "a"), Tuple2.of(2L, "b"), Tuple2.of(3L, "c"), Tuple2.of(4L, "d"));

//        DataStream<Tuple2<Long, String>> partitionCustom = text.partitionCustom(new CustomPartition(), new KeySelector<Tuple2<Long, String>, Long>() {
//            @Override
//            public Long getKey(Tuple2<Long, String> in) throws Exception {
//                return in.f0;
//            }
//        });
        //将记录输出到下游Operator的第一个实例。
//        DataStream<Tuple2<Long, String>> partitionCustom = text.global();
        //将记录随机输出到下游Operator的每个实例。
//        DataStream<Tuple2<Long, String>> partitionCustom = text.shuffle();
        //将记录以循环的方式输出到下游Operator的每个实例。
//        DataStream<Tuple2<Long, String>> partitionCustom = text.rebalance();
        //基于上下游Operator的并行度，将记录以循环的方式输出到下游Operator的每个实例。举例: 上游并行度是2，下游是4，则上游一个并行度以循环的方式将记录输出到下游的两个并行度上;上游另一个并行度以循环的方式将记录输出到下游另两个并行度上。若上游并行度是4，下游并行度是2，则上游两个并行度将记录输出到下游一个并行度上；上游另两个并行度将记录输出到下游另一个并行度上。
//        DataStream<Tuple2<Long, String>> partitionCustom = text.rescale();
        // 广播分区将上游数据集输出到下游Operator的每个实例中。适合于大数据集Join小数据集的场景。
//        DataStream<Tuple2<Long, String>> partitionCustom = text.broadcast();
//        将记录输出到下游本地的operator实例。ForwardPartitioner分区器要求上下游算子并行度一样。上下游Operator同属一个SubTasks。
//        DataStream<Tuple2<Long, String>> partitionCustom = text.forward();
//        将记录按Key的Hash值输出到下游Operator实例。
        KeyedStream<Tuple2<Long, String>, Long> partitionCustom = text.keyBy((KeySelector<Tuple2<Long, String>, Long>) value -> value.f0);
        partitionCustom.map(new MapFunction<Tuple2<Long,String>, String>() {
            @Override
            public String map(Tuple2<Long, String> in) throws Exception {
                System.out.println("当前线程id：" + Thread.currentThread().getId() + ",value: " + in);
                return in.f1;
            }
        }).printToErr();

        env.execute(Main.class.getName());
    }
}
