import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
        	// Normalization is done on row.
            // input: movie_row:movie_col \t count
        	// outputKey = movie_row
        	// outputValue = movie_col=count
            String[] movies_count = value.toString().trim().split("\t");
            String[] movies = movies_count[0].split(":");
            String count = movies_count[1];
            context.write(new Text(movies[0]), new Text(movies[1] + ":" + count));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input: key = movie_row, value = <movie_col1:count, movie_col2:count...>
            // output: key = movie_col, value = movie_row=count
            // Use a map to store iterable values.
            Map<String, Integer> m = new HashMap<String, Integer>(); // (movie_col, count)
            int movieRowSum = 0;
            while (values.iterator().hasNext()) {
                String[] movieCol_count = values.iterator().next().toString().split(":");
                String movieCol = movieCol_count[0]
                int count = Integer.parseInt(movieCol_count[1]);
                movieRowSum += count;
                m.put(movieCol, count);
            }

            for (Map.Entry<String, Integer> entry: m.entrySet()) {
                String movieCol = entry.getKey();
                Double count = entry.getValue();
                String proba = movieRow.toString() + "=" + count / movieRowSum;
                context.write(new Text(movieCol), new Text(proba));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);
        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
