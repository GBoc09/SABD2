package it.uniroma2.sabd.flink.query.common;

import it.uniroma2.sabd.flink.utils.EventTimeUtils;
import it.uniroma2.sabd.model.FlightEvent;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public abstract class GlobalWindowProcessFunction<K, ACC, OUT>
        extends KeyedProcessFunction<K, FlightEvent, OUT> {

    private MapState<Long, ACC> monthlyStatsState;
    private ValueState<Long> nextTimerState;

    @Override
    public void open(Configuration parameters) {
        monthlyStatsState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(stateName() + "-monthly", Long.class, accumulatorClass()));
        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(stateName() + "-timer", Long.class));
    }

    @Override
    public void processElement(FlightEvent event, Context ctx, Collector<OUT> out) throws Exception {
        long eventTimestamp = EventTimeUtils.eventTimestamp(ctx, event);
        long currentWatermark = ctx.timerService().currentWatermark();

        if (isAfterClosedMonthlyBucket(eventTimestamp, currentWatermark)) {
            OutputTag<FlightEvent> tag = discardedEventsTag();
            if (tag != null) {
                ctx.output(tag, event);
            }
            return;
        }

        long monthStart = EventTimeUtils.monthStart(eventTimestamp);
        ACC state = monthlyStatsState.get(monthStart);
        if (state == null) {
            state = createAccumulator(event);
        }

        updateAccumulator(state, event);
        monthlyStatsState.put(monthStart, state);

        if (nextTimerState.value() == null) {
            long next = EventTimeUtils.nextMonthlyTimer(eventTimestamp);
            ctx.timerService().registerEventTimeTimer(next);
            nextTimerState.update(next);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        ACC cumulative = cumulativeStatsBefore(timestamp);
        emitIfValid(cumulative, timestamp, ctx.getCurrentKey(), out);

        long next = EventTimeUtils.nextMonthlyTimer(timestamp);
        ctx.timerService().registerEventTimeTimer(next);
        nextTimerState.update(next);
    }

    private ACC cumulativeStatsBefore(long snapshotTs) throws Exception {
        ACC cumulative = emptyAccumulator();
        for (Map.Entry<Long, ACC> entry : monthlyStatsState.entries()) {
            if (entry.getKey() < snapshotTs) {
                mergeInto(cumulative, entry.getValue());
            }
        }
        return cumulative;
    }
    private boolean isAfterClosedMonthlyBucket(long eventTimestamp, long currentWatermark) {
        return currentWatermark != Long.MIN_VALUE
                && EventTimeUtils.nextMonthlyTimer(eventTimestamp) <= currentWatermark;
    }

    protected abstract String stateName();
    protected abstract Class<ACC> accumulatorClass();
    protected abstract ACC createAccumulator(FlightEvent firstEvent);
    protected abstract ACC emptyAccumulator();
    protected abstract void updateAccumulator(ACC state, FlightEvent event);
    protected abstract void mergeInto(ACC target, ACC source);
    protected abstract void emitIfValid(ACC cumulative, long timestamp, K key, Collector<OUT> out);

    protected OutputTag<FlightEvent> discardedEventsTag() {
        return null;
    }
}
