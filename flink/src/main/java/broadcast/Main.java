package broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * broadcast测试
 *
 * @author lixiyan
 * @date 2020/4/6 9:11 AM
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource text = env.addSource(new MyNoParalleSource()).setParallelism(1);
        SingleOutputStreamOperator num = text.broadcast().map(new RichMapFunction<Long, Long>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                System.out.println("map -- open--");
            }

            @Override
            public Long map(Long input) throws Exception {
                String name = Thread.currentThread().getName();
                System.out.println("线程：" + name + "接收数据: " + input);
                return input;
            }
        });
        num.print();
//        发现整个Map元素别处理了4次：
        String name = Main.class.getName();
        env.execute(name);
    }
}
