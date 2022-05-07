package functions.nn.gnn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.ParallelBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;

import java.util.List;
import java.util.function.Function;

public class SAGEConv extends GNNBlock{
    int outFeatures;

    public SAGEConv(int outFeatures) {
        this.outFeatures = outFeatures;
        // Update Block
        ParallelBlock updateBLock = new ParallelBlock(new Function<List<NDList>, NDList>() {
            @Override
            public NDList apply(List<NDList> item) {
                return new NDList(item.get(0).get(0).add(item.get(1).get(0)));
            }
        });

        updateBLock.add(
                new SequentialBlock()
                        .add(new LambdaBlock(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return new NDList(ndArrays.get(0));
                            }
                        }))
                        .add(Linear.builder().setUnits(outFeatures).optBias(true).build())
        );
        updateBLock.add(
                new SequentialBlock()
                        .add(new LambdaBlock(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return new NDList(ndArrays.get(1));
                            }
                        }))
                        .add(Linear.builder().setUnits(outFeatures).optBias(true).build())
        );
        // Message block is just a forward
        LambdaBlock messageBlock = new LambdaBlock(new Function<NDList, NDList>() {
            @Override
            public NDList apply(NDList ndArrays) {
                return ndArrays;
            }
        });

        setAgg(AggregatorVariant.MEAN);
        setMessageBlock(messageBlock);
        setUpdateBlock(updateBLock);
    }

}
