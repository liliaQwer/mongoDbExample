package com.itechart;

import com.itechart.customer.Customer;
import com.itechart.customer.CustomerRepository;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.MapReduceAction;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class ApplicationStarter implements CommandLineRunner {

    @Autowired
    MongoClient mongoClient;

    @Autowired
    private CustomerRepository repository;

    public static void main(String[] args) {
        SpringApplication.run(ApplicationStarter.class, args);
    }

    @Override
    public void run(String... args) {
        System.out.println("Traffic Task");
        trafficTask();
        System.out.println("*************************************");
        System.out.println("Friends Task");
        friendsTask();
    }

    public void trafficTask() {
        MongoDatabase db = mongoClient.getDatabase("website_traffic");

        MongoCollection traffics = db.getCollection("traffic");
        fillTrafficCollection(traffics);

        System.out.println("Results of aggregate function:");
        printAggregateTraffic(traffics);

        System.out.println("Results of mapReduce:");
        printMapReduceTraffic(traffics);
    }

    private void printMapReduceFriends(MongoDatabase db, MongoCollection friends) {
        String map = String.join("", "function(){",
                " for (var i = 0; i < this.friends.length; i++){ ",
                " var key = this.friends[i].friend_id;",
                " emit(key,1); ",
                " }",
                " };");

        String reduce = String.join("","function(key, values){ ",
                 " var sum= 0;",
                 " values.forEach(function (value){ ",
                 " sum +=1; ",
                 " });",
                 " return (key, sum); ",
                 " };");

        friends.mapReduce(map, reduce).action(MapReduceAction.REPLACE).collectionName("friends_stat");
        db.getCollection("friends_stat").find().sort(new Document("value", -1)).limit(5).
                forEach((Block<Document>) document -> System.out.println(document.toJson()));
    }

    private void printMapReduceTraffic(MongoCollection traffics) {
        String map = String.join("","function(){",
                "emit({resource: this. url}, {total:1});",
                "};");

        String reduce = String.join("","function(key, values){",
                "var sum = 0;",
                "values.forEach(function (value){",
                "sum += 1;",
                "});",
                "return ({resource: key}, {total: sum});",
                "};");

        traffics.mapReduce(map, reduce).forEach((Block<Document>) document -> System.out.println(document.toJson()));
    }

    private void printAggregateFriends(MongoCollection friends) {
        friends.aggregate(
                Arrays.asList(
                        Aggregates.unwind("$friends"),
                        Aggregates.group("$friends.friend_id", Accumulators.sum("count", 1)),
                        Aggregates.sort(new Document("count", -1)),
                        Aggregates.limit(5)
                )
        ).forEach((Block<Document>) document -> System.out.println(document.toJson()));
    }

    private void printAggregateTraffic(MongoCollection traffics) {
        traffics.aggregate(
                Arrays.asList(
                        //  Aggregates.group("$url", Accumulators.sum("count", 1)),
                        Aggregates.group(new Document().append("resource", "$url"), Accumulators.sum("total", 1)),
                        Aggregates.sort(new Document("count", -1))
                )
        ).forEach((Block<Document>) document -> System.out.println(document.toJson()));
    }

    public void friendsTask() {
        MongoDatabase db = mongoClient.getDatabase("friendship");

        MongoCollection friends = db.getCollection("friends");
        fillFriendsCollection(friends);

        System.out.println("Results of aggregate:");
        printAggregateFriends(friends);

        System.out.println("Results of mapReduce:");
        printMapReduceFriends(db, friends);
    }

    private void fillTrafficCollection(MongoCollection traffics) {
        traffics.deleteMany(new Document());

        Document traffic = new Document();
        traffic.append("url", "Url1");
        traffic.append("date", "01.01.2010");
        traffic.append("ip", "1.1.1.1");
        traffics.insertOne(traffic);

        traffic = new Document();
        traffic.append("url", "Url1");
        traffic.append("date", "01.01.2010");
        traffic.append("ip", "1.1.1.1");
        traffics.insertOne(traffic);

        traffic = new Document();
        traffic.append("url", "Url2");
        traffic.append("date", "01.01.2010");
        traffic.append("ip", "1.1.1.2");
        traffics.insertOne(traffic);

        traffic = new Document();
        traffic.append("url", "Url2");
        traffic.append("date", "01.01.2010");
        traffic.append("ip", "1.1.1.3");
        traffics.insertOne(traffic);

        traffic = new Document();
        traffic.append("url", "Url3");
        traffic.append("date", "01.01.2010");
        traffic.append("ip", "1.1.1.1");
        traffics.insertOne(traffic);

        traffics.find().forEach((Block<Document>) document -> System.out.println(document.toJson()));
    }

    private void fillFriendsCollection(MongoCollection friends) {
        friends.deleteMany(new Document());

        Document human = new Document();
        human.append("_id", 1);
        human.append("name", "Pupkin");
        List<BasicDBObject> humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "2"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 2);
        human.append("name", "Holms");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "1"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 3);
        human.append("name", "Watson");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "1"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 4);
        human.append("name", "Green");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "2"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 5);
        human.append("name", "Adams");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "6"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 6);
        human.append("name", "Tomson");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "7"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 7);
        human.append("name", "Conor");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "1"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 8);
        human.append("name", "Queen");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "1"));
        humanFriends.add(new BasicDBObject("friend_id", "9"));
        humanFriends.add(new BasicDBObject("friend_id", "2"));
        humanFriends.add(new BasicDBObject("friend_id", "4"));
        humanFriends.add(new BasicDBObject("friend_id", "5"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 9);
        human.append("name", "Adison");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "2"));
        humanFriends.add(new BasicDBObject("friend_id", "10"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        human.append("_id", 10);
        human.append("name", "Krutoy");
        humanFriends = new ArrayList<>();
        humanFriends.add(new BasicDBObject("friend_id", "2"));
        humanFriends.add(new BasicDBObject("friend_id", "7"));
        humanFriends.add(new BasicDBObject("friend_id", "6"));
        human.append("friends", humanFriends);
        friends.insertOne(human);

        friends.find().forEach((Block<Document>) document -> System.out.println(document.toJson()));
    }

    private void testCustomerRepository() {
        repository.deleteAll();

        // save a couple of customers
        repository.save(new Customer("Alice", "Smith"));
        repository.save(new Customer("Bob", "Smith"));

        // fetch all customers
        System.out.println("Customers found with findAll():");
        System.out.println("-------------------------------");
        for (Customer customer : repository.findAll()) {
            System.out.println(customer);
        }
        System.out.println();

        // fetch an individual customer
        System.out.println("Customer found with findByFirstName('Alice'):");
        System.out.println("--------------------------------");
        System.out.println(repository.findByFirstName("Alice"));

        System.out.println("Customers found with findByLastName('Smith'):");
        System.out.println("--------------------------------");
        for (Customer customer : repository.findByLastName("Smith")) {
            System.out.println(customer);
        }
    }
}
