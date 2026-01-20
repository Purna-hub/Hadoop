import java.io.IOException;

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

public class Job2_SumMatrix {

    /* ================= MAPPER ================= */
    public static class SumMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.length() == 0 || line.startsWith("_")) return;

            String[] p = line.split("\\s+");

            if (p.length != 2) return;

            context.write(
                    new Text(p[0]),
                    new IntWritable(Integer.parseInt(p[1]))
            );
        }
    }

    /* ================= REDUCER ================= */
    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /* ================= DRIVER ================= */
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Matrix Final Sum");

        job.setJarByClass(Job2_SumMatrix.class);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
