import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1_PartialMultiply {

    /* ================= MAPPER ================= */
    public static class MMMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Ignore empty lines and Hadoop metadata
            if (line.length() == 0 || line.startsWith("_")) return;

            String[] p = line.split(",");

            // Accept ONLY valid matrix records
            if (p.length != 4) return;

            // A,i,k,val OR B,k,j,val
            if ("A".equals(p[0])) {
                // key = k
                context.write(
                        new Text(p[2]),
                        new Text("A," + p[1] + "," + p[3])
                );
            } else if ("B".equals(p[0])) {
                // key = k
                context.write(
                        new Text(p[1]),
                        new Text("B," + p[2] + "," + p[3])
                );
            }
        }
    }

    /* ================= REDUCER ================= */
    public static class MMReducer
            extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<int[]> A = new ArrayList<>();
            List<int[]> B = new ArrayList<>();

            for (Text v : values) {
                String[] p = v.toString().split(",");
                if (p[0].equals("A")) {
                    A.add(new int[]{
                            Integer.parseInt(p[1]),
                            Integer.parseInt(p[2])
                    });
                } else if (p[0].equals("B")) {
                    B.add(new int[]{
                            Integer.parseInt(p[1]),
                            Integer.parseInt(p[2])
                    });
                }
            }

            // Emit partial products
            for (int[] a : A) {
                for (int[] b : B) {
                    context.write(
                            new Text(a[0] + "," + b[0]),
                            new IntWritable(a[1] * b[1])
                    );
                }
            }
        }
    }

    /* ================= DRIVER ================= */
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Matrix Partial Multiply");

        job.setJarByClass(Job1_PartialMultiply.class);
        job.setMapperClass(MMMapper.class);
        job.setReducerClass(MMReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
