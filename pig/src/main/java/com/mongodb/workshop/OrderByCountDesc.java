package com.mongodb.workshop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * MongoDB-Hadoop Workshop
 *
 * Pig UDF that takes a bag of tuples and orders
 * them by frequency/count in descending order.
 *
 */
public class OrderByCountDesc extends EvalFunc<DataBag> {

    private BagFactory factory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        Object value = input.get(0);

        if (value instanceof DataBag) {
            DataBag bag = (DataBag) value;

            final Map<Tuple,Integer> tupleMap = new HashMap<Tuple,Integer>();

            for(Tuple tuple : bag) {
                if (tupleMap.containsKey(tuple)) {
                    int count = tupleMap.get(tuple) + 1;
                    tupleMap.put(tuple, count);
                } else {
                    tupleMap.put(tuple, 1);
                }
            }

            List<Tuple> tuples = new ArrayList<Tuple>();
            tuples.addAll(tupleMap.keySet());

            Collections.sort(tuples, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple t1, Tuple t2) {
                    Integer count1 = tupleMap.get(t1);
                    Integer count2 = tupleMap.get(t2);
                    return count2.compareTo(count1);
                }});

            return factory.newDefaultBag(tuples);
        }
        else {
            throw new ExecException("malformed input");
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema.FieldSchema leftElementFs =
                new Schema.FieldSchema("left", DataType.INTEGER);
            Schema.FieldSchema rightElementFs =
                new Schema.FieldSchema("right", DataType.INTEGER);

            Schema tupleSchema = new Schema(
                Arrays.asList(leftElementFs, rightElementFs));
            Schema.FieldSchema tupleFs =
                new Schema.FieldSchema("tuple", tupleSchema, DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFs);
            Schema.FieldSchema bagFs =
                new Schema.FieldSchema("pairs", bagSchema, DataType.BAG);

            return new Schema(bagFs);
        }
        catch (Exception e) {
            return null;
        }
    }
}
