package broadcast.broadcastState;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.HeapBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * broadcast state
 *
 * @author lixiyan
 * @date 2020/4/6 9:39 PM
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MapStateDescriptor<String, String> dynamicConfig = new MapStateDescriptor<>("dynamicConfig", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        BroadcastStream<String> broadcastStream = env.fromElements("a", "b").setParallelism(1).broadcast(dynamicConfig);

        KeyedStream<Tuple2<String, String>, String> orderStreamByKey = env.addSource(new MyOrderParalleSource()).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });


        SingleOutputStreamOperator<String> process = orderStreamByKey.connect(broadcastStream).process(new KeyedBroadcastProcessFunction<Object, Tuple2<String,String>, String, String>() {
            @Override
            public void processElement(Tuple2<String, String> input, ReadOnlyContext ctx, Collector<String> collector) throws Exception {
                System.out.println("----processElement start------");
                System.out.println(input);
                HeapBroadcastState<String, String> config = (HeapBroadcastState) ctx.getBroadcastState(dynamicConfig);
                String in = input.f0;
                String res = "";
                Iterator<Map.Entry<String, String>> iterator = config.entries().iterator();
                while (iterator.hasNext()){
                    Map.Entry<String, String> next = iterator.next();
                    System.out.println("---广播数据---key: "+next.getKey()+"--value:"+next.getValue());
                }
                if (config.contains(in)) {
                    res = "匹配成功 输入：" + in + " 输出： " + "（" + in + "," + config.get(in) + "）";
                    System.out.println("开始注册定时器：");
                    ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+2000);
                    System.err.println(res);
                }
                System.out.println("----processElement end------");
            }

            @Override
            public void processBroadcastElement(String s, Context ctx, Collector<String> collector) throws Exception {
                System.out.println("----processBroadcastElement start------");
                ctx.getBroadcastState(dynamicConfig).put(s, "broadcast的值 " + s);
                System.out.println(s);
                System.out.println("----processBroadcastElement end------");
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println("---定时器触发 start-----");
                HeapBroadcastState<String, String> config = (HeapBroadcastState) ctx.getBroadcastState(dynamicConfig);
                Iterator<Map.Entry<String, String>> iterator = config.entries().iterator();
                while (iterator.hasNext()){
                    Map.Entry<String, String> next = iterator.next();
                    System.out.println("---广播数据---key: "+next.getKey()+"--value:"+next.getValue());
                }

                System.out.println("---定时器触发 end----");
            }});
        String name = BroadCastDemo.class.getName();
        env.execute(name);
    }
}
