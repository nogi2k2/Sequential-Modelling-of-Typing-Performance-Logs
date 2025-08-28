package distributed.hadoop.job2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgWpmByTodMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final Text outKey = new Text();
    private final DoubleWritable outVal = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",", -1);
        if (parts.length < 4 || "user_id".equalsIgnoreCase(parts[0])) return;

        try {
            String tod = parts[2].trim();   
            String wpmStr = parts[3].trim(); 
            if (tod.isEmpty() || wpmStr.isEmpty()) return;

            double wpm = Double.parseDouble(wpmStr);
            outKey.set(tod);
            outVal.set(wpm);
            ctx.write(outKey, outVal);
        } catch (NumberFormatException ignored) { }
    }
}
