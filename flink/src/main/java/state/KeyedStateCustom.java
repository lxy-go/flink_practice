package state;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义KeyedState
 *
 * @author lixiyan
 * @date 2020/4/5 8:34 PM
 */
public class KeyedStateCustom extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Long>>> {

    private ListState<Long> abnormalData;


    // 需要监控的阈值
    private Long threshold;
    // 触发报警的次数
    private Integer numberOfTimes;

    KeyedStateCustom(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig ttl = StateTtlConfig
                // 设置有效期为 1 秒
                .newBuilder(Time.seconds(1))
                // 设置有效期更新规则，这里设置为当创建和写入时，都重置其有效期到规定的10秒
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                /*设置只要值过期就不可见，另外一个可选值是ReturnExpiredIfNotCleanedUp，
                        代表即使值过期了，但如果还没有被物理删除，就是可见的*/
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ListStateDescriptor<Long> abnormalDataDesc = new ListStateDescriptor<>("abnormalData", Long.class);
        abnormalDataDesc.enableTimeToLive(ttl);
        this.abnormalData = getRuntimeContext().getListState(abnormalDataDesc);
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, List<Long>>> out) throws Exception {
        Long inputValue = value.f1;
        // 如果超过阀值，记录不正常信息
        if (inputValue >= threshold) {
            abnormalData.add(inputValue);
        }

        ArrayList<Long> lists = Lists.newArrayList(abnormalData.get().iterator());
        // 如果不正常的数据出现一定次数，则输出报警信息
        if (lists.size() >= numberOfTimes) {
            out.collect(Tuple2.of(value.f0 + "超过指定阈值", lists));
            // 输出后清空状态
            abnormalData.clear();
        }
    }
}
