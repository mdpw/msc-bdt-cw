import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributionMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private IntWritable inDegree = new IntWritable();
    private final static IntWritable one = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        
        String[] parts = line.split("\\s+");
        if (parts.length >= 2) {
            try {
                int degree = Integer.parseInt(parts[1]);
                inDegree.set(degree);
                context.write(inDegree, one);
            } catch (NumberFormatException e) {
                // Skip malformed lines
            }
        }
    }
}