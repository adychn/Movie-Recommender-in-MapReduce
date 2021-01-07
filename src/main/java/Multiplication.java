import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input: movieCol /t movieRow=proba
			// outputKey: movieCol
			// outputValue: movieRow=proba
			String[] line = value.toString().trim().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));
		}
	}
	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input: original raw file. user, movie, rating
			// outputKey: movieCol
			// outputValue: users:rating
			String[] line = value.toString().trim().split(",");
			context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
		}
	}
	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Users' ratings for movieCol
			// inputKey: movieCol
			// intpuValue = <movieRow1=proba, movieRow2=proba, ... , userA:rating, userB:rating...>
 			// outputKey = user:movieRow
			// outputValue = proba * rating
			Map<String, Double> probaMap = new HashMap<String, Double>(); // movieRow->proba
			Map<String, Double> ratingMap = new HashMap<String, Double>(); // user->rating
			for (Text value : values) {
				if (value.toString().contains("=")) {
					String[] movieRow_proba = value.toString().split("=");
					probaMap.put(movieRow_proba[0], Double.parseDouble(movieRow_proba[1]));
				} else {
					String[] user_rating = value.toString().split(":");
					ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}

			for (Map.Entry<String, Double> entry : probaMap.entrySet()) {
				String movieRow = entry.getKey();
				double proba = entry.getValue();
				for (Map.Entry<String, Double> element : ratingMap.entrySet()) {
					String user = element.getKey();
					double rating = element.getValue();
					context.write(new Text(user + ":" + movieRow), new DoubleWritable(proba*rating));
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);
		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
