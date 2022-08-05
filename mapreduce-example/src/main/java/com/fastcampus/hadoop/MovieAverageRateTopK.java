package com.fastcampus.hadoop;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MovieAverageRateTopK extends Configured implements Tool {
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieId = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns[0].equals("movieId")) {
                return;
            }
            movieId.set(columns[0]);
            outValue.set("M" + columns[1]);
            context.write(movieId, outValue);
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieId = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] colums = value.toString().split(",");
            if (colums[0].equals("userId")) {
                return;
            }
            movieId.set(colums[1]);
            outValue.set("R" + colums[2]);
            context.write(movieId, outValue);
        }
    }

    public static class MovieRatingJoinReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> ratingList = new ArrayList<>();
        private Text movieName = new Text();
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ratingList.clear();

            for (Text value : values) {
                if (value.charAt(0) == 'M') {
                    movieName.set(value.toString().substring(1));
                } else if (value.charAt(0) == 'R') {
                    ratingList.add(value.toString().substring(1));
                }
            }

            double average = ratingList.stream().mapToDouble(Double::parseDouble).average().orElse(0.0);
            outValue.set(String.valueOf(average));
            context.write(movieName, outValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "MovieAverageRateTopK First");
        job.setJarByClass(MovieAverageRateTopK.class);
        job.setReducerClass(MovieRatingJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MovieAverageRateTopK(), args);
        System.exit(exitCode);
    }
}
