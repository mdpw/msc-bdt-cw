import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InDegreeDistributionDriverOptimized {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: InDegreeDistribution <input> <temp> <o>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        
        // SINGLE OPTIMIZATION: Compression for intermediate data
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        
        System.out.println("=== HADOOP OPTIMIZATION: COMPRESSION ===");
        System.out.println("Map output compression: Snappy codec enabled");
        System.out.println("Benefit: Reduces network I/O during shuffle");
        System.out.println("Target: 193MB shuffle → ~50-70MB compressed");
        System.out.println("========================================");
        
        // Job 1: Calculate in-degrees
        System.out.println("=== STARTING MAPREDUCE JOB 1: Calculate In-Degrees ===");
        long startTime = System.currentTimeMillis();
        
        Job job1 = Job.getInstance(conf, "calculate in-degrees");
        job1.setJarByClass(InDegreeDistributionDriverOptimized.class);
        job1.setMapperClass(InDegreeMapper.class);
        job1.setCombinerClass(InDegreeReducer.class);
        job1.setReducerClass(InDegreeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        boolean job1Success = job1.waitForCompletion(true);
        long job1Time = System.currentTimeMillis() - startTime;
        
        if (!job1Success) {
            System.err.println("Job 1 failed!");
            System.exit(1);
        }
        System.out.println("Job 1 completed in: " + job1Time + " ms");
        
        // Job 2: Calculate distribution
        System.out.println("=== STARTING MAPREDUCE JOB 2: Calculate Distribution ===");
        long startTime2 = System.currentTimeMillis();
        
        Job job2 = Job.getInstance(conf, "calculate distribution");
        job2.setJarByClass(InDegreeDistributionDriverOptimized.class);
        job2.setMapperClass(DistributionMapper.class);
        job2.setCombinerClass(DistributionReducer.class);
        job2.setReducerClass(DistributionReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
        boolean job2Success = job2.waitForCompletion(true);
        long job2Time = System.currentTimeMillis() - startTime2;
        long totalTime = System.currentTimeMillis() - startTime;
        
        if (job2Success) {
            System.out.println("Job 2 completed in: " + job2Time + " ms");
            System.out.println("TOTAL MAPREDUCE TIME: " + totalTime + " ms");
            System.out.println("=== MAPREDUCE ANALYSIS COMPLETE ===");
        }
        System.exit(job2Success ? 0 : 1);
    }
}