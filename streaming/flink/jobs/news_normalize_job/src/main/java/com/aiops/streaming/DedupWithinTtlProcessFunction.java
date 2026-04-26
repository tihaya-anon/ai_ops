package com.aiops.streaming;

import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class DedupWithinTtlProcessFunction extends KeyedProcessFunction<String, String, String> {
  private final int ttlMinutes;
  private transient ValueState<Boolean> seen;

  DedupWithinTtlProcessFunction(int ttlMinutes) {
    this.ttlMinutes = ttlMinutes;
  }

  @Override
  public void open(Configuration parameters) {
    StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofMinutes(ttlMinutes))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();
    ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", TypeInformation.of(Boolean.class));
    desc.enableTimeToLive(ttl);
    seen = getRuntimeContext().getState(desc);
  }

  @Override
  public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
    Boolean already = seen.value();
    if (already != null && already) {
      return;
    }
    seen.update(true);
    out.collect(value);
  }
}
