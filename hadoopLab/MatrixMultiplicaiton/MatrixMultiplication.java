import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m")); // Rows in A
            int p = Integer.parseInt(conf.get("p")); // Columns in B
            
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
            
            if (indicesAndValue[0].equals("A")) {
                for (int j = 0; j < p; j++) {
                    // Output key: (i, j), Value: (A, k, v)
                    context.write(new Text(indicesAndValue[1] + "," + j), 
                                  new Text("A," + indicesAndValue[2] + "," + indicesAndValue[3]));
                }
            } else {
                for (int i = 0; i < m; i++) {
                    // Output key: (i, j), Value: (B, k, v)
                    context.write(new Text(i + "," + indicesAndValue[2]), 
                                  new Text("B," + indicesAndValue[1] + "," + indicesAndValue[3]));
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<Integer, Double> hashA = new HashMap<>();
            HashMap<Integer, Double> hashB = new HashMap<>();
            
            for (Text val : values) {
                String[] data = val.toString().split(",");
                if (data[0].equals("A")) {
                    hashA.put(Integer.parseInt(data[1]), Double.parseDouble(data[2]));
                } else {
                    hashB.put(Integer.parseInt(data[1]), Double.parseDouble(data[2]));
                }
            }
            
            int n = Integer.parseInt(context.getConfiguration().get("n")); // Columns in A / Rows in B
            double result = 0.0;
            for (int k = 0; k < n; k++) {
                double a_ik = hashA.getOrDefault(k, 0.0);
                double b_kj = hashB.getOrDefault(k, 0.0);
                result += a_ik * b_kj;
            }
            
            if (result != 0) {
                context.write(key, new DoubleWritable(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: MatrixMultiplication <input> <output> <m> <n> <p>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("m", args[2]); // Rows of A
        conf.set("n", args[3]); // Cols of A / Rows of B
        conf.set("p", args[4]); // Cols of B

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}