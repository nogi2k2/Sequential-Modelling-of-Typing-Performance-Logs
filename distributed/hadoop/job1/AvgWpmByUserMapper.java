package distributed.hadoop.job1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgWpmByUserMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final Text outKey = new Text();
    private final DoubleWritable outVal = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split(",", -1);
        if (parts.length < 4 || "user_id".equalsIgnoreCase(parts[0])) return;

        try {
            String userId = parts[0].trim();     
            String wpmStr = parts[3].trim();     
            if (userId.isEmpty() || wpmStr.isEmpty()) return;

            double wpm = Double.parseDouble(wpmStr);
            outKey.set(userId);
            outVal.set(wpm);
            ctx.write(outKey, outVal);
        } catch (Exception e) {
            System.err.println("Error processing line: " + line + " - " + e.getMessage());
        }
    }
}
