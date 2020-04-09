package broadcast;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义非并行的source
 *
 * @author lixiyan
 * @date 2020/4/6 11:36 AM
 */
public class MyNoParalleSource implements SourceFunction<Long> {

    private long count = 1L;

    private boolean running = true;

    /***
     * 持续产生数据
     *
     * @param ctx source的context
     * @author lixiyan
     * @date 2020/4/6
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (running){
            ctx.collect(count);
            count++;
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
