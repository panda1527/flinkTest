import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

//import dataSource.Write;
public class SinleInput {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        System.out.println("-input <path>设置数据源文件路径（必需）；初次运行添加参数-init <num>生成num条流数据，每条数据大概40B；-output <path>设置sink文件夹(可选)；-para <num>设置并行数（可选)");
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data
        DataStreamSource<String> Source;
        String input = params.get("input");
        String init = params.get("init");
//        int num=Integer.parseInt(params.get("num"));
        if (input != null) {
            if (init != null) {
                System.out.println("初始化生成数据源");
                dataSource.Write.createStream(input,Integer.parseInt(init));
            }
            Source = env.readTextFile(input);
        } else {
            System.out.println("请使用-input path设置输入参数");
            return;
        }
        if (params.get("para") != null) {
            int parallelism = Integer.parseInt(params.get("para"));
            env.setParallelism(parallelism);   //设置并行数
        }

//        SingleOutputStreamOperator<Tuple4<Long, Integer, Double, Double>> reducedStream = env.readTextFile("D:\\stream2.txt")
        SingleOutputStreamOperator<Tuple4<Long, Integer, Double, Double>> reducedStream = Source.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(0)) {
                    @Override
                    public long extractTimestamp(String s) {
                        String[] tokens = s.split(" ");
                        long time = Long.parseLong(tokens[0]) * 1000;
                        return time;
                    }
                })
                .flatMap(new Tokenizer())
                .keyBy(1)
//                .window(TumblingEventTimeWindows.of(Time.seconds(1000)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(100)))
                .window(SlidingEventTimeWindows.of(Time.seconds(1000), Time.seconds(100)))
                .reduce(new ReduceFunction<Tuple4<Long, Integer, Double, Double>>() {
                    @Override
                    public Tuple4<Long, Integer, Double, Double> reduce(Tuple4<Long, Integer, Double, Double> t1, Tuple4<Long, Integer, Double, Double> t2) throws Exception {

                        if (t1.f0 > t2.f0) {
//                            System.out.println(t1);
                            return t1;
                        } else {
//                            System.out.println(t2);
                            return t2;
                        }
                    }
                });

//        reducedStream.print();
        // emit result
//        System.out.println("开始sink至文件");
//        counts.writeAsText("E:\\Desktop\\result1", FileSystem.WriteMode.OVERWRITE);
        String output = params.get("output");
        String sink;
        if (output != null) {
            sink="sink至文件";
            reducedStream.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        } else {
            System.out.println("未设置output路径，无sink");
            sink="无sink";
        }
        long startTime = System.currentTimeMillis();
        env.execute("Streaming WordCount");
        long endTime = System.currentTimeMillis();
        System.out.println(Integer.parseInt(init)+"条记录文件1000s长度滑动窗口，100s滑动一次，" + env.getParallelism() + "线程"+sink+"总耗时" + (endTime - startTime));
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple4<Long, Integer, Double, Double>> {

        @Override
        public void flatMap(String value, Collector<Tuple4<Long, Integer, Double, Double>> out) {
            // normalize and split the line
//            System.out.println(value);
            String[] line = value.toLowerCase().split(" ");
            if (line.length > 3) {
                Long time = Long.parseLong(line[0].trim()) * 1000;
                Integer id = Integer.parseInt(line[1].trim());
                Double lon;
                try {
                    lon = Double.parseDouble(line[2].trim());
                } catch (NumberFormatException e) {
                    //错误处理
                    return;
                }
                Double lat = Double.parseDouble(line[3].trim());
                out.collect(new Tuple4<>(time, id, lon, lat));
            }
        }
    }
}