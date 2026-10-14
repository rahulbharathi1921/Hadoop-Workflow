import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Double> mapA = new HashMap<>();
        Map<Integer, Double> mapB = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts[0].equals("A")) {
                mapA.put(Integer.parseInt(parts[1]), Double.parseDouble(parts[2]));
            } else {
                mapB.put(Integer.parseInt(parts[1]), Double.parseDouble(parts[2]));
            }
        }

        double result = 0;
        for (Integer k : mapA.keySet()) {
            if (mapB.containsKey(k)) {
                result += mapA.get(k) * mapB.get(k);
            }
        }
        context.write(key, new Text(String.valueOf(result)));
    }
}