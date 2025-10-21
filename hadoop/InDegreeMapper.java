import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InDegreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text targetNode = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#")) return;
        
        String[] nodes = line.split("\\s+");
        if (nodes.length >= 2) {
            try {
                Integer.parseInt(nodes[0]);
                Integer.parseInt(nodes[1]);
                targetNode.set(nodes[1]);
                context.write(targetNode, one);
            } catch (NumberFormatException e) {
                // Skip invalid lines
            }
        }
    }
}