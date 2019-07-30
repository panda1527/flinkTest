package dataSource;

import java.io.File;
import java.io.IOException;
import java.util.Random;


public class selectArea {
    public static int last_time = 0;
    public static double speed = 5.0 / 60 / 6;  //速度为2dis/1h，即（5/60/6）dis/10s，dis为lat方加lon方之和开方

    public static class City {
        //        String name;
        int id;
        boolean isCenter;
        double lon;
        double lat;
        int nearestCenter;

//        public void setName(String name) {
//            this.name = name;
//        }

        public void setId(int id) {
            this.id = id;
        }

        public void setCenter(boolean center) {
            isCenter = center;
        }

        public void setLon(double lon) {
            this.lon = lon;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public void setNearestCenter(int nearestCenter) {
            this.nearestCenter = nearestCenter;
        }

//        public String getName() {
//            return name;
//        }

        public int getId() {
            return id;
        }

        public boolean isCenter() {
            return isCenter;
        }

        public double getLon() {
            return lon;
        }

        public double getLat() {
            return lat;
        }

        public int getNearestCenter() {
            return nearestCenter;
        }

        public City(int id, boolean isCenter, double lon, double lat) {
//            this.name = name;
            this.id = id;
            this.isCenter = isCenter;
            this.lon = lon;
            this.lat = lat;
        }

        public City(int id, boolean isCenter, double lon, double lat, int nearestCenter) {
            this.id = id;
            this.isCenter = isCenter;
            this.lon = lon;
            this.lat = lat;
            this.nearestCenter = nearestCenter;
        }
        @Override
        public String toString() {
            return
                    "id=" + id +
                            ", isCenter=" + isCenter +
                            ", lon=" + lon +
                            ", lat=" + lat +
                            ", nearestCenter=" + nearestCenter;
        }
    }

    private static String filenameTemp;

    public static double dis(double lon1, double lat1, double lon2, double lat2) {
        return Math.sqrt(Math.pow((lon1 - lon2), 2) + Math.pow((lat1 - lat2), 2));
    }

    //    public static City[] createCities(int num, String outputFile) throws IOException {
    public static City[] createCities(int num) throws IOException {
        City[] cities = new City[num];
        boolean bool = false;
        Random r1 = new Random(1);
        Random r2 = new Random(2);
        double lon, lat;
//        try {
//            // lon范围100-120；lat范围23-40
//            createFile(outputFile);
//            File file = new File(outputFile);
//            FileOutputStream out = new FileOutputStream(file);
//            BufferedOutputStream Buff = new BufferedOutputStream(out);
        for (int i = 0; i < num / 10; i++) {
            lon = r1.nextDouble() * 20 + 100;
            lat = r2.nextDouble() * 17 + 23;
            cities[i] = new City(i, true, lon, lat, i);
//                Buff.write((cities[i].toString() + "\r\n").getBytes());
        }
        for (int i = num / 10; i < num; i++) {
            lon = r1.nextDouble() * 20 + 100;
            lat = r2.nextDouble() * 17 + 23;
            cities[i] = new City(i, false, lon, lat);
            double dis = dis(lon, lat, cities[0].lon, cities[0].lat);
            int nearest = 0;
            for (int j = 1; j < num / 10; j++) {
                if (dis(lon, lat, cities[j].lon, cities[j].lat) < dis) {
                    dis = dis(lon, lat, cities[j].lon, cities[j].lat);
                    nearest = j;
                }
            }
            cities[i].nearestCenter = nearest;
//                Buff.write((cities[i].toString() + "\r\n").getBytes());
        }
//            Buff.flush();
//            Buff.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return cities;
    }

    public static boolean createFile(String fileName) {
        Boolean bool = false;
        File file = new File(fileName);
        try {
            //如果文件不存在，则创建新的文件
            if (!file.exists()) {
                file.createNewFile();
                bool = true;
                System.out.println("success create file,the file is " + filenameTemp);
                //创建文件成功后，写入内容到文件里
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bool;
    }

    //    public static Order[] createOrders(City[] cities, int num, String outputFile) throws IOException {



    public static void main(String[] args) throws IOException {
    }
}
