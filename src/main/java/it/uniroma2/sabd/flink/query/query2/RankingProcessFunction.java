package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.flink.model.Query2Stats;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Secondo stage del pipeline Q2.
 * Riceve Query2Stats per aeroporto (già filtrati per MIN_FLIGHTS),
 * li accumula per finestra (chiave = windowStartEpoch),
 * e al timer emette la classifica top-10 ordinata per severeDelays desc.
 * Stesso pattern di GlobalTDigestProcessFunction:
 * KeyedProcessFunction + ListState + timer.
 */
final class RankingProcessFunction
        extends KeyedProcessFunction<Long, Query2Stats, Query2Stats> {

    private static final int TOP_N = 10;

    private ListState<Query2Stats> airportsState;

    @Override
    public void open(Configuration parameters) {
        airportsState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("q2-airports-per-window", Query2Stats.class));
    }

    @Override
    public void processElement(
            Query2Stats stats,
            Context ctx,
            Collector<Query2Stats> out) throws Exception {

        airportsState.add(stats);

        // Impostiamo il timer al termine esatto della finestra (usando il watermark).
        // Flink garantirà che scatti quando è sicuro che non ci siano altri dati
        // validi in arrivo per questa finestra. Aggiungiamo 1ms per sicurezza.
        ctx.timerService().registerEventTimeTimer(stats.getWindowEndEpoch() + 1);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Query2Stats> out) throws Exception {

        List<Query2Stats> airports = new ArrayList<>();
        airportsState.get().forEach(airports::add);
        airportsState.clear();

        if (airports.isEmpty()) return;

        // Sort descending per severeDelays, e in caso di parità per depDelayMean
        airports.sort((a, b) -> {
            int cmp = Long.compare(b.getSevereDelays(), a.getSevereDelays());
            if (cmp == 0) {
                return Double.compare(b.getDepDelayMean(), a.getDepDelayMean());
            }
            return cmp;
        });

        int rank = 1;
        for (Query2Stats stats : airports.subList(0, Math.min(TOP_N, airports.size()))) {
            stats.setRank(rank++);
            out.collect(stats);
        }
    }
}