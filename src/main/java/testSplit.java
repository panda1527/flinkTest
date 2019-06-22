import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class testSplit {

    public static void main(String[] args) throws IOException {
        String path = args[0];
        BufferedReader input = new BufferedReader(new FileReader(path));
        String line;
        long lc = 0;
        Set<Integer> distinctIDs = new HashSet<>();
        long begin = System.currentTimeMillis();
        while ((line = input.readLine()) != null) {
            String[] fields = line.split(" ");
            lc++;
            distinctIDs.add(Integer.parseInt(fields[1]));
        }
        System.out.println("Records: " + lc + " distinct: " + distinctIDs.size() + " time: " + (System.currentTimeMillis() - begin));
    }
}
