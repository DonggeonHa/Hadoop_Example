package com.fastcampus.hadoop;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class MovieAverageRateTopK extends Configured implements Tool {
    private final static int K = 30;

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

    public static class TopKMapper extends Mapper<Object, Text, Text, Text> {
        // Key값을 기준으로 정렬이 되어있는 맵
        private TreeMap<Double, Text> topKMap = new TreeMap<>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            topKMap.put(Double.parseDouble(columns[1]), new Text(columns[0]));

            if (topKMap.size() > K) {
                topKMap.remove(topKMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Double k : topKMap.keySet()) {
                // 맵의 아웃풋으로 평점이 키로 출력이 되고, 두번째로 영화제목이 출력이 된다
                context.write(new Text(k.toString()), topKMap.get(k));
            }
        }
    }

    public static class TopKReducer extends Reducer<Text, Text, Text, Text> {
        private TreeMap<Double, Text> topKMap = new TreeMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                topKMap.put(Double.parseDouble(key.toString()), new Text(value));
                if (topKMap.size() > K) {
                    topKMap.remove(topKMap.firstKey());
                }
            }
        }

        @Override
        protected  void cleanup(Context context) throws IOException, InterruptedException {
            // 내림차순 키값으로 가져와서 처리
            for (Double k : topKMap.descendingKeySet()) {
                context.write(topKMap.get(k), new Text(k.toString()));
            }
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

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        int returnCode = job.waitForCompletion(true) ? 0 : 1;

        // 정상적으로 완료가 됐을 시
        if (returnCode == 0) {
            Job job2 = Job.getInstance(getConf(), "MovieAverageRateTopK Second" );
            job2.setJarByClass(MovieAverageRateTopK.class);
            job2.setMapperClass(TopKMapper.class);
            job2.setReducerClass(TopKReducer.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            return job2.waitForCompletion(true) ? 0 : 1;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MovieAverageRateTopK(), args);
        System.exit(exitCode);
    }
}
