import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {

    /* ================= MAPPER ================= */
    public static class WeatherMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.length() == 0) return;

            String[] parts = line.split(",");

            // Skip header or invalid rows
            if (parts.length != 3 || parts[2].equalsIgnoreCase("Temperature"))
                return;

            try {
                String location = parts[0];
                int temperature = Integer.parseInt(parts[2]);
                context.write(new Text(location), new IntWritable(temperature));
            } catch (NumberFormatException e) {
                // Ignore bad data
            }
        }
    }

    /* ================= REDUCER ================= */
    public static class WeatherReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int count = 0;

            for (IntWritable v : values) {
                sum += v.get();
                count++;
            }

            if (count > 0) {
                double avg = (double) sum / count;
                context.write(key, new DoubleWritable(avg));
            }
        }
    }

    /* ================= DRIVER ================= */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Data Mining");

        job.setJarByClass(WeatherAnalysis.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        // 🔑 CRITICAL FIX (Map Output Types)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final Output Types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
