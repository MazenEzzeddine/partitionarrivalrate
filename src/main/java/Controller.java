import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;





public class Controller implements Runnable{


    private static final Logger log = LogManager.getLogger(Controller.class);

    public static String CONSUMER_GROUP;
    public static int numberOfPartitions;
    public static AdminClient admin = null;




    static Long sleep;
    static double doublesleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;


    ////WIP TODO
    public static Map<MemberDescription, Float> maxConsumptionRatePerConsumer = new HashMap<>();
    public static Map<MemberDescription, Long> consumerToLag = new HashMap<>();
    public static Instant lastDecision;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    ///////////////////////////////////////////////////////////////////////////


    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;
    static boolean firstIteration= true;

    static  TopicDescription td;

    static DescribeTopicsResult tdr;

    static ArrayList<Partition> partitions= new ArrayList<>();




    private static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        tdr =  admin.describeTopics(Collections.singletonList(topic));
        td =tdr.values().get(topic).get();

        for(TopicPartitionInfo p: td.partitions()){
            partitions.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic has the following partitions {}", td.partitions().size() );
    }





    private static void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(Controller.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();


        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
        log.info("The consumer group {} is in state {}", Controller.CONSUMER_GROUP,
                consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).state().toString());


        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members()) {


                log.info("Calling the consumer {} for its consumption rate ", memberDescription.host());

            MemberAssignment memberAssignment = memberDescription.assignment();
            for (TopicPartition tp : memberAssignment.topicPartitions()) {

                log.info("member consumerId {} clientId {} is assigned partition {}", memberDescription.consumerId(),
                        memberDescription.clientId(), tp.partition());
            }

        }



    }




    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();

        for(TopicPartitionInfo p :  td.partitions()){
            requestLatestOffsets.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();

        for(TopicPartitionInfo p :  td.partitions()) {
            TopicPartition t = new  TopicPartition(topic, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();

            partitions.get(p.partition()).setPreviousLastOffset(partitions.get(p.partition()).getCurrentLastOffset());
            partitions.get(p.partition()).setCurrentLastOffset(latestOffset);
            partitions.get(p.partition()).setLag(latestOffset-committedoffset);
        }
        if(!firstIteration){
            computeTotalArrivalRate();
            queryConsumerGroup();
        }else{
            firstIteration = false;
        }
    }






    private static void computeTotalArrivalRate() throws ExecutionException, InterruptedException {

        double totalArrivalRate =0;
        long totallag = 0;

        for(Partition p: partitions) {
            p.setArrivalRate((double)(p.getCurrentLastOffset()-p.getPreviousLastOffset())/doublesleep);
            log.info(p.toString());

           totalArrivalRate +=  p.getArrivalRate();
            totallag +=  p.getLag();
        }

        log.info("totalArrivalRate {}", totalArrivalRate);
        log.info("totallag {}", totallag);

    }

    @Override
    public void run() {

        try {
            readEnvAndCrateAdminClient();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();

        doublesleep = (double) sleep/1000.0;


        while (true) {
            log.info("New Iteration:");
            try {
                getCommittedLatestOffsetsAndLag();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //computeTotalArrivalRate();

            log.info("Sleeping for {} seconds", sleep / 1000.0);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            log.info("End Iteration;");
            log.info("=============================================");
        }

    }
}




