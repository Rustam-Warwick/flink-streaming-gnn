package ai.djl.serializers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDHelper;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;

/**
 * Kryo implementation of Tensor Serializer. Works with all NDArrays.
 */
public class NDArrayRawSerializer extends Serializer<NDArray> {
    private static final transient DataType[] dataTypes = DataType.values();

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        output.writeByte(o.getDataType().ordinal()); // Data Types
        output.writeByte(o.getShape().getShape().length); // Shape length
        output.writeLongs(o.getShape().getShape(), true); // Actual Shapes
        ByteBuffer bb = o.toByteBuffer();
        output.writeInt(bb.capacity());
        output.write(bb.array()); // Actual Data
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        DataType dataType = dataTypes[input.readByte()]; // Data Type
        long[] shapes = input.readLongs(input.readByte(), true);
        Shape shape = new Shape(shapes); // Shape
        int bufferSize = input.readInt();
        ByteBuffer data = NDHelper.globalNDManager.allocateDirect(bufferSize);
        data.put(input.readBytes(data.capacity()));
        return NDHelper.globalNDManager.create(data.rewind(), shape, dataType);
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }

}
