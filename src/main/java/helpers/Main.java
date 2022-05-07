package helpers;

import ai.djl.BaseModel;
import ai.djl.Model;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.BlockList;
import ai.djl.nn.SequentialBlock;
import ai.djl.pytorch.engine.PtModel;
import elements.GraphOp;
import functions.nn.SerializableModel;
import functions.nn.gnn.SAGEConv;
import functions.parser.MovieLensStreamParser;
import functions.splitter.EdgeTrainTestSplitter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import partitioner.HDRF;
import plugins.debugging.PrintVertexPlugin;
import plugins.newblock.embedding_layer.GNNStreamingEmbeddingLayer;
import storage.TupleStorage;

import java.time.Duration;
import java.util.List;

public class Main {
    public static BlockList myPartitionedModel(){
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(128));
        sb.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(32));
        return sb.getChildren();
    }
    public static void main(String[] args) throws Exception {
        BlockList bl = myPartitionedModel();
        GraphStream gs = new GraphStream((short) 3);
        DataStream<GraphOp> ratingsEdgeStream = gs.readSocket(new MovieLensStreamParser(), "localhost", 9090)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((event, ts) -> event.getTimestamp()));
        DataStream<GraphOp> partitioned = gs.partition(ratingsEdgeStream, new HDRF());
        DataStream<GraphOp> splittedData = gs.trainTestSplit(partitioned, new EdgeTrainTestSplitter(0.005));

        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(splittedData, List.of(
                new TupleStorage().withPlugin(new PrintVertexPlugin("2262")).withPlugin(new GNNStreamingEmbeddingLayer(new SerializableModel<>("gnn", bl.get(0).getValue()), false))

        ));
        embeddings.print();

////
//        DataStream<GraphOp> trainData = ((SingleOutputStreamOperator<GraphOp>) splittedData).getSideOutput(new OutputTag<>("training", TypeInformation.of(GraphOp.class)));
//        DataStream<GraphOp> outputFunction = gs.gnnLayerNewIteration(embeddings.union(trainData), new TupleStorage().withPlugin(new PrintVertexPlugin("2262")).withPlugin(new EdgeOutputInference() {
//            @Override
//            public Model createOutputModel() {
//                SequentialBlock myBlock = new SequentialBlock();
//                myBlock.add(new LambdaBlock(inputs -> (
//                        new NDList(inputs.get(0).concat(inputs.get(1)))
//                )));
//                myBlock.add(Linear.builder().setUnits(16).build());
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(32).build());
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(1).build());
//                myBlock.add(Activation::sigmoid);
//                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7), new Shape(7));
//                Model model = Model.newInstance("prediction");
//                model.setBlock(myBlock);
//                return model;
//            }
//        }));
//
//        gs.gnnLoss(outputFunction, new SparseCategoricalCrossEntropyLoss(120) {
//            @Override
//            public Loss createLossFunction() {
//                return new BinaryCrossEntropy();
//            }
//        });


        gs.env.execute();
    }
}
