package com.fastcampus.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/** input Format을 기존에 사용했던 텍스트인풋포맷이 아닌 키밸류텍스트인풋포맷을 사용할 예정
 *  키밸류텍스트인풋포맷같은 경우 라인별로 데이터를 읽어서 키와 밸류로 파싱을 할 수 있게 인풋포맷이다.
 *  키와 밸류를 구분하기 위한 새퍼레이터 값을 지정해주어야 한다
 * */
public class SortWordCount extends Configured implements Tool {
    public static class SortMapper extends Mapper<Text, Text, LongWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // (hadoop 3)
            context.write(new LongWritable(Long.parseLong(value.toString())), key);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job job = Job.getInstance(conf, "SortWordCount");

        job.setJarByClass(SortWordCount.class);
        job.setMapperClass(SortMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortWordCount(), args);
        System.exit(exitCode);
    }
}
