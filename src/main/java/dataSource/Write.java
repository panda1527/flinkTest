package dataSource;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;


public class Write {
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

    public static class Order {
        int id;
        int type;
        int start_time1;
        int start_city;
        int first_transfer;
        int arrive_time1;
        int start_time2;
        int last_transfer;
        int arrive_time2;
        int start_time3;
        int end_city;
        int arrive_time3;
        double lon;
        double lat;
        int buyer;
        int seller;
        boolean done = false;

        public Order(int id, int type, int start_time1, int start_city, int end_city, int buyer, int seller, boolean done) {
            this.id = id;
            this.type = type;
            this.start_time1 = start_time1;
            this.start_city = start_city;
            this.end_city = end_city;
            this.buyer = buyer;
            this.seller = seller;
            this.done = done;
        }

        @Override
        public String toString() {
            return "id=" + id +
                    ", type=" + type +
                    ", start_time1=" + start_time1 +
                    "path=" + start_city + "-" + first_transfer + "-" + last_transfer + "-" + end_city +
                    ", buyer=" + buyer +
                    ", seller=" + seller;
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
    public static Order[] createOrders(City[] cities, int num) throws IOException {
        Order[] orders = new Order[num];
        Random rType = new Random(1);
        Random rTime = new Random(2);
        Random rStart = new Random(3);
        Random rEnd = new Random(4);
        Random rBuyer = new Random(5);
        Random rSeller = new Random(6);
        int type;
        int start_time1;
        int start_city;
        int end_city;
        int buyer;
        int seller;
//        try {
//            // lon范围100-120；lat范围23-40
//            createFile(outputFile);
//            File file = new File(outputFile);
//            FileOutputStream out = new FileOutputStream(file);
//            BufferedOutputStream Buff = new BufferedOutputStream(out);
        for (int i = 0; i < num; i++) {
            type = rType.nextInt(10);
            start_time1 = rTime.nextInt(24 * 60 * 6);     //一天内随机下单，时间单位为10s
            start_city = rStart.nextInt(1000);
            end_city = rEnd.nextInt(1000);
            buyer = rBuyer.nextInt(100000);   //十万个买家
            seller = rSeller.nextInt(1000);   //一千个卖家
            orders[i] = new Order(i, type, start_time1, start_city, end_city, buyer, seller, false);
            orders[i].first_transfer = cities[orders[i].start_city].nearestCenter;
            orders[i].last_transfer = cities[orders[i].end_city].nearestCenter;
            orders[i].arrive_time1 = orders[i].start_time1 + (int) (dis(cities[orders[i].start_city].lon, cities[orders[i].start_city].lat, cities[orders[i].first_transfer].lon, cities[orders[i].first_transfer].lat) / speed);
            orders[i].start_time2 = 2 * orders[i].arrive_time1 - orders[i].start_time1;
            orders[i].arrive_time2 = orders[i].start_time2 + (int) (dis(cities[orders[i].first_transfer].lon, cities[orders[i].first_transfer].lat, cities[orders[i].last_transfer].lon, cities[orders[i].last_transfer].lat) / speed);
            orders[i].start_time3 = 2 * orders[i].arrive_time2 - orders[i].start_time2;
            orders[i].arrive_time3 = orders[i].start_time3 + (int) (dis(cities[orders[i].last_transfer].lon, cities[orders[i].last_transfer].lat, cities[orders[i].end_city].lon, cities[orders[i].end_city].lat) / speed);
            if (last_time < orders[i].arrive_time3) {
                last_time = orders[i].arrive_time3;
            }
//                Buff.write((orders[i].toString() + "\r\n").getBytes());
        }
//            Buff.flush();
//            Buff.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return orders;
    }

    public static int createStream(String outputFile,int ordersNum) throws IOException {
        System.out.println("开始生产数据流");
        City[] cities = createCities(1000);    //生成城市信息保存文件

        Order[] orders = createOrders(cities, ordersNum/12000); //生成订单信息保存文件
        int len = orders.length;
        int order_id;
        double lon;
        double lat;
        createFile(outputFile);
        File file = new File(outputFile);
        FileOutputStream out = new FileOutputStream(file);
        BufferedOutputStream Buff = new BufferedOutputStream(out);
        int count = 0;
        try {
            for (int time = 0; time <= last_time; time++) {
                for (int i = 0; i < len; i++) {
                    if (!orders[i].done) {
                        if (time < orders[i].start_time1) {
                            lon = cities[orders[i].start_city].lon;
                            lat = cities[orders[i].start_city].lat;
                            Buff.write((time + " " + i + " " + lon + " " + lat + "\r\n").getBytes());
                        } else if (time >= orders[i].start_time1 && time <= orders[i].arrive_time1) {
                            lon = cities[orders[i].start_city].lon + (cities[orders[i].first_transfer].lon - cities[orders[i].start_city].lon) * (time - orders[i].start_time1) / (orders[i].arrive_time1 - orders[i].start_time1);
                            lat = cities[orders[i].start_city].lat + (cities[orders[i].first_transfer].lat - cities[orders[i].start_city].lat) * (time - orders[i].start_time1) / (orders[i].arrive_time1 - orders[i].start_time1);
                            Buff.write((time + " " + i + " " + lon + " " + lat + "\r\n").getBytes());
                        } else if (time >= orders[i].arrive_time1 && time <= orders[i].start_time2) {
                            lon = cities[orders[i].first_transfer].lon;
                            lat = cities[orders[i].first_transfer].lat;
                            Buff.write((time + " " + i + " " + lon + " " + lat + "\r\n").getBytes());
                        } else if (time >= orders[i].start_time2 && time <= orders[i].arrive_time2) {
                            lon = cities[orders[i].first_transfer].lon + (cities[orders[i].last_transfer].lon - cities[orders[i].first_transfer].lon) * (time - orders[i].start_time2) / (orders[i].arrive_time2 - orders[i].start_time2);
                            lat = cities[orders[i].first_transfer].lat + (cities[orders[i].last_transfer].lat - cities[orders[i].first_transfer].lat) * (time - orders[i].start_time2) / (orders[i].arrive_time2 - orders[i].start_time2);
                            Buff.write((time + " " + i + " " + lon + " " + lat + "\r\n").getBytes());
                        } else if (time >= orders[i].arrive_time2 && time <= orders[i].start_time3) {
                            lon = cities[orders[i].last_transfer].lon;
                            lat = cities[orders[i].last_transfer].lat;
                            Buff.write((time + " " + i + " " + lon + " " + lat + "\r\n").getBytes());
                        } else if (time >= orders[i].start_time3 && time <= orders[i].arrive_time3) {
                            lon = cities[orders[i].last_transfer].lon + (cities[orders[i].end_city].lon - cities[orders[i].last_transfer].lon) * (time - orders[i].start_time3) / (orders[i].arrive_time3 - orders[i].start_time3);
                            lat = cities[orders[i].last_transfer].lat + (cities[orders[i].end_city].lat - cities[orders[i].last_transfer].lat) * (time - orders[i].start_time3) / (orders[i].arrive_time3 - orders[i].start_time3);
                            Buff.write((time + " " + i + " " + lon + " " + lat + "\r\n").getBytes());
                        } else {
                            orders[i].done = true;
                        }
                    } else {
                        lon = cities[orders[i].end_city].lon;
                        lat = cities[orders[i].end_city].lat;
                        Buff.write((time + " " + i + " " + lon + " " + lat + "\r\n").getBytes());
                    }
                    count++;

                }
//                System.out.println(time*10+"秒各订单定位输出完成");
            }
            System.out.println("生成流数量" + count);
            Buff.flush();
            Buff.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(last_time);
        createStream("D:\\stream1.txt",3000);   //生成数据流信息保存文件
        System.out.println("程序结束");
    }
}
