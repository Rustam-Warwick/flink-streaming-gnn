package functions.gnn_layers;

import elements.GraphOp;
import iterations.MessageDirection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

public class StreamingGNNLayerFunction extends KeyedProcessFunction<String, GraphOp, GraphOp> implements GNNLayerFunction {
    final OutputTag<GraphOp> forwardOutput = new OutputTag<GraphOp>("forward") {
    };
    final OutputTag<GraphOp> backwardOutput = new OutputTag<GraphOp>("backward") {
    };
    public BaseStorage storage;
    public short position;
    public short numLayers;
    public transient short currentPart;
    public transient Collector<GraphOp> collector;
    public transient KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx;

    public StreamingGNNLayerFunction(BaseStorage storage, short position, short numLayers) {
        this.position = position;
        this.numLayers = numLayers;
        this.storage = storage;
        storage.layerFunction = this;
    }

    @Override
    public short getCurrentPart() {
        return currentPart;
    }

    @Override
    public short getPosition() {
        return position;
    }

    @Override
    public short getNumLayers() {
        return numLayers;
    }

    @Override
    public void message(GraphOp op) {
        try {
            if (op.direction == MessageDirection.BACKWARD) {
                ctx.output(backwardOutput, op);
            } else if (op.direction == MessageDirection.FORWARD) {
                ctx.output(forwardOutput, op);
            } else {
                collector.collect(op);
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sideMessage(GraphOp op, OutputTag<GraphOp> outputTag) {
        ctx.output(outputTag, op);
    }

    @Override
    public BaseStorage getStorage() {
        return storage;
    }

    @Override
    public long currentTimestamp() {
        return ctx.timestamp();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getStorage().open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        getStorage().close();
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        getStorage().onTimer(timestamp);
    }

    @Override
    public TimerService getTimerService() {
        return ctx.timerService();
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.currentPart = Short.parseShort(ctx.getCurrentKey());
        process(value);
    }
}
