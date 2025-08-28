package distributed.hadoop.job1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgWpmByUserReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private final DoubleWritable outVal = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context ctx)
            throws IOException, InterruptedException {
        double sum = 0.0;
        long count = 0L;
        for (DoubleWritable v : values) {
            sum += v.get();
            count++;
        }
        if (count > 0) {
            outVal.set(sum / count);
            ctx.write(key, outVal);
        }
    }
}
