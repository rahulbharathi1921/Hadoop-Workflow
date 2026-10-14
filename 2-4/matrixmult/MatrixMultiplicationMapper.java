import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMapper extends Mapper<Object, Text, Text, Text> {
    int m = 2; // rows in A
    int n = 2; // cols in A / rows in B
    int p = 3; // cols in B

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split(",");
        if (parts.length < 4) {
            return;
        }

        String matrixName = parts[0].trim().toUpperCase();
        int row = Integer.parseInt(parts[1].trim());
        int col = Integer.parseInt(parts[2].trim());
        double val = Double.parseDouble(parts[3].trim());

        if (matrixName.startsWith("A")) {
            for (int k = 0; k < p; k++) {
                context.write(new Text(row + "," + k), new Text("A," + col + "," + val));
            }
        } else if (matrixName.startsWith("B")) {
            for (int i = 0; i < m; i++) {
                context.write(new Text(i + "," + col), new Text("B," + row + "," + val));
            }
        }
    }
}