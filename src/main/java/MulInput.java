import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class MulInput {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        long startTime = System.currentTimeMillis();
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        if (params.get("input")!=null){

        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
//        int Parallelismnum = 16;
//        env.setParallelism(Parallelismnum);
        env.getParallelism();
        // get input data
        ArrayList<DataStream<String>> streams = new ArrayList<DataStream<String>>();
        DataStream<String> stream=env.readTextFile("G:\\streams\\0.txt");
        for(int i=1;i<10;i++){
            streams.add(env.readTextFile("G:\\streams\\"+i+".txt"));
            stream.join(streams.get(i-1));
        }

        DataStream<Tuple2<Integer, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                stream.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);

        // emit result
//        System.out.println("开始sink至文件");
        counts.writeAsText("E:\\Desktop\\result1", FileSystem.WriteMode.OVERWRITE);

//        counts.print();
        // execute program
        env.execute("Streaming WordCount");

        long endTime = System.currentTimeMillis();
        System.out.println("十份输入"+env.getParallelism() + "线程无sink总耗时" + (endTime - startTime));
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<Integer, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
            // normalize and split the line
            String[] line = value.toLowerCase().split(" ");
            if (line.length > 1) {
                Integer id = Integer.parseInt(line[1]);
//                out.collect(new Tuple2<>(id, 1));
            }

            // emit the pairs
//            for (String word : line) {
//                System.out.print(word+" ");
//                if (word.length() > 0) {
////                    out.collect(new Tuple2<>(token, 1));
//                }
//            }
        }
    }

}