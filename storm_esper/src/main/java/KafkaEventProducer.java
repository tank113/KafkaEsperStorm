
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class KafkaEventProducer {

    private String fileName;
    private String colHeader;
    private org.apache.kafka.clients.producer.Producer producer;

    /**
     * Initialize Kafka configuration
     */
    public void initKafkaConfig() {

        // Build the configuration required for connecting to Kafka
        Properties props = new Properties();
        // List of Kafka brokers. If there're multiple brokers, they're
        props.put("bootstrap.servers", "localhost:9092");
        // Serializer used for sending data to kafka. Since we are sending
        // string, we are using StringEncoder.
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        // We want acks from Kafka that messages are properly received.
        props.put("request.required.acks", "1");

        // Create the producer instance
        producer = new KafkaProducer(props);
    }

    /**
     * Initialize configuration for file to be read (csv)
     *
     * @param fileName
     * @throws IOException
     */
    public void initFileConfig(String fileName) throws IOException, Exception {
        this.fileName = fileName;
        System.out.println(this.fileName);
        try {
            InputStream inStream = this.getClass().getClassLoader()
                    .getResourceAsStream(this.fileName);
            Reader reader = new InputStreamReader(inStream);
            BufferedReader buffReader = IOUtils.toBufferedReader(reader);

            // Get the header line to initialize CSV parser
            colHeader = buffReader.readLine();
            System.out.println("File header :: " + colHeader);

            if (StringUtils.isEmpty(colHeader)) {
                throw new Exception("Column header is null, something is wrong");
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    /**
     * Send csv file data to the named topic on Kafka broker
     *
     * @param topic
     * @throws IOException
     */
    public void sendFileDataToKafka(String topic) throws IOException {

        Iterable<CSVRecord> csvRecords = null;

        // Parse the CSV file, using the column header
        try {
            InputStream inStream = this.getClass().getClassLoader()
                    .getResourceAsStream(fileName);
            Reader reader = new InputStreamReader(inStream);

            String[] colNames = StringUtils.split(colHeader, ',');
            csvRecords = CSVFormat.DEFAULT.withHeader(colNames).parse(reader);
        } catch (IOException e) {
            System.out.println(e);
            throw e;
        }

        // We iterate over the records and send each over to Kafka broker
        // Get the next record from input file
        CSVRecord csvRecord = null;
        Iterator<CSVRecord> csvRecordItr = csvRecords.iterator();
        boolean firstRecDone = false;
        while (csvRecordItr.hasNext()) {
            try {
                csvRecord = csvRecordItr.next();
                if (!firstRecDone) {
                    firstRecDone = true;
                    continue;
                }
                // Get a map of column name and value for a record
                Map<String, String> keyValueRecord = csvRecord.toMap();

                // Create the message to be sent
                ObjectMapper mapper = new ObjectMapper();

                JsonNode jsonNode = mapper.valueToTree(keyValueRecord);
                // Send the message
                //System.out.println(jsonNode);
                ProducerRecord<String, JsonNode> data = new ProducerRecord<String, JsonNode>(topic,jsonNode);
                producer.send(data);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


            }

    /**
     * Send csv file data to the named topic on Kafka broker
     *
     * @param topic
     * @throws IOException
     */
    public void sendOutputToKafka(Object outputCEP, String topic) throws IOException {
        try{

            System.out.println("output \t" +  outputCEP + "\n");
            // Create the message to be sent
            ObjectMapper mapper = new ObjectMapper();

            JsonNode jsonNode = mapper.valueToTree(outputCEP);
            // Send the message
            //System.out.println(jsonNode);
            ProducerRecord<String, JsonNode> data = new ProducerRecord<String, JsonNode>(topic,jsonNode);
            producer.send(data);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * Cleanup stuff
     */
    public void cleanup() {
        producer.close();
    }
}
