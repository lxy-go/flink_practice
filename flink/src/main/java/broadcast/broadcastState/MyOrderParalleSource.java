package broadcast.broadcastState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义非并行的source
 *
 * @author lixiyan
 * @date 2020/4/6 11:36 AM
 */
public class MyOrderParalleSource implements SourceFunction<Tuple2<String,String>> {

    private long count = 1L;

    private boolean running = true;

    private String[] dict= {"a","b","c","d","e","f","g","h","i"};

    /***
     * 持续产生数据
     *
     * @param ctx source的context
     * @author lixiyan
     * @date 2020/4/6
     */
    @Override
    public void run(SourceContext<Tuple2<String,String>> ctx) throws Exception {
        Random r = new Random(1);
        while (running){
            int rand = r.nextInt(dict.length);
            String s = dict[rand];
            ctx.collect(Tuple2.of(s,s));
            // 延时方便观察
            Thread.sleep(1000);
        }

    }
    /**
     * 取消会调用方法
     *
     * @author lixiyan
     * @date 2020/4/6
     */
    @Override
    public void cancel() {
        running = false;
    }
}
