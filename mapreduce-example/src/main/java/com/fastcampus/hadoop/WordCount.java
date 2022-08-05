package com.fastcampus.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.awt.*;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount extends Configured implements Tool {
    // Mapper 클래스 <입력 키, 입력 벨류, 출력 키, 출력 벨류 클래스들>
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // private Text word = new Text();
        protected Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // 라인 데이터를 토큰 단위로 나눈 후에 while문을 통해서 반복해가지고 나와 있는 단어와 숫자1을 map의 아웃풋으로 출력
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase());
                context.write(word, one);
                // (hadoop, 1)
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Reducer 클래스 <입력 키, 입력 벨류, 출력 키, 출력 벨류 클래스들>
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            // 리듀서는 키별로 전달된 등장횟수를 모두 더하여 전체 등장회수를 생성합니다.
            // 문자가 key로 들어오고, 등장 횟수가 Iterable 형태로 전달됩니다. 이 값을 모두 더하여 결과를 출력합니다.
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            // (hadoop, 3)
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 드라이버 구현
        Job job = Job.getInstance(getConf(), "wordcount");

        // 주어진 클래스를 포함하는 jar파일을 찾아서 셋팅해줌.
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // 출력할 데이터 타입
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // 입력 파일 위치 /// 지정한 위치의 파일을 라인단위로 읽어서 맵에 전달.
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 출력 파일 위치 /// 지정한 위치에 파일로 출력

        // 정상적으로 끝나면 0을 리턴
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }
}
