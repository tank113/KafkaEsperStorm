import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PushToInfluxDbBolt{

    private static final Logger LOG = LoggerFactory.getLogger(PushToInfluxDbBolt.class);
    private static final long serialVersionUID = 2L;
    // Configurations keys
    public static final String KEY_INFLUXDB_URL = "metrics.influxdb.url";
    public static final String KEY_INFLUXDB_USERNAME = "metrics.influxdb.username";
    public static final String KEY_INFLUXDB_PASSWORD = "metrics.influxdb.password";
    public static final String KEY_INFLUXDB_DATABASE = "metrics.influxdb.database";
    public static final String KEY_INFLUXDB_MEASUREMENT_PREFIX = "metrics.influxdb.measurement.prefix";
    public static final String KEY_INFLUXDB_ENABLE_GZIP = "metrics.influxdb.enable.gzip";

    // Default config values for non requires
    public static final String DEFAULT_INFLUXDB_URL = "http://192.168.99.100:8083";
    public static final String DEFAULT_INFLUXDB_USERNAME = ""; //empty
    public static final String DEFAULT_INFLUXDB_PASSWORD = ""; //empty
    public static final String DEFAULT_INFLUXDB_DATABASE = "stormCEPAnalytics";
    public static final String DEFAULT_INFLUXDB_MEASUREMENT_PREFIX = "storm-";
    public static final Boolean DEFAULT_INFLUXDB_ENABLE_GZIP = true;

    private InfluxDB influxDB;
    private BatchPoints batchPoints;
    private String influxdbUrl;
    private String influxdbUsername;
    private String influxdbPassword;
    private String influxdbDatabase;
    private String influxdbMeasurementPrefix;
    private Boolean influxdbEnableGzip;
    private static PushToInfluxDbBolt pushToInfluxDbBolt;

    public PushToInfluxDbBolt() {
        this.influxdbUrl = "http://127.0.0.1:8086";
        this.influxdbUsername = "";
        this.influxdbPassword = "";
        this.influxdbDatabase = "stormCEPAnalyticsUp";
        this.influxdbMeasurementPrefix = "storm-";
        this.influxdbEnableGzip = true;

        this.prepareConnection();
    }

    private boolean databaseWasCreated = false;
    private Map<String, Object> fields;
    private Map<String, String> tags;
    private Config config;

   /* public PushToInfluxDbBolt(Map<Object, Object> config) {


        this.influxdbUrl = (String) getKeyValueOrDefaultValue(config, KEY_INFLUXDB_URL, DEFAULT_INFLUXDB_URL);
        this.influxdbUsername = (String) getKeyValueOrDefaultValue(config, KEY_INFLUXDB_USERNAME, DEFAULT_INFLUXDB_USERNAME);
        this.influxdbPassword = (String) getKeyValueOrDefaultValue(config, KEY_INFLUXDB_PASSWORD, DEFAULT_INFLUXDB_PASSWORD);
        this.influxdbDatabase = (String) getKeyValueOrDefaultValue(config, KEY_INFLUXDB_DATABASE, DEFAULT_INFLUXDB_DATABASE);
        this.influxdbMeasurementPrefix = (String) getKeyValueOrDefaultValue(config, KEY_INFLUXDB_MEASUREMENT_PREFIX, DEFAULT_INFLUXDB_MEASUREMENT_PREFIX);
        this.influxdbEnableGzip = (Boolean) getKeyValueOrDefaultValue(config, KEY_INFLUXDB_ENABLE_GZIP, DEFAULT_INFLUXDB_ENABLE_GZIP);


    }*/

    /**
     * Look at the object collection if key exist, if not, it return defaultValue
     *
     * @param objects      Collection of Object
     * @param key          Key to lookup at objects collections
     * @param defaultValue default value to be returned
     * @return Object
     */
    /*private Object getKeyValueOrDefaultValue(Map<Object, Object> objects, String key, Object defaultValue) {
        if (objects.containsKey(key)) {
            return objects.get(key);
        } else {
            LOG.warn("{}: Using default parameter for {}", this.getClass().getSimpleName(), key);
            return defaultValue;
        }
    }*/

    /**
     * Prepare connection pool to InfluxDB server
     */
    private void prepareConnection() {
        if (influxDB == null) {
            LOG.debug("{}: Preparing connection to InfluxDB: [ url='{}', username='{}', password='{}' ]",
                    this.getClass().getSimpleName(),
                    this.influxdbUrl,
                    this.influxdbUsername,
                    this.influxdbPassword
            );

            if (this.influxdbUsername.isEmpty() && this.influxdbPassword.isEmpty()) {
                this.influxDB = InfluxDBFactory.connect(this.influxdbUrl);
            } else {
                this.influxDB = InfluxDBFactory.connect(this.influxdbUrl, this.influxdbUsername, this.influxdbPassword);
            }

            // additional connections options
            if (this.influxdbEnableGzip) {
                this.influxDB.enableGzip();
            }
        } else {
            LOG.debug("{}: InfluxDB connection was available: [ url='{}', username='{}', password='{}' ]",
                    this.getClass().getSimpleName(),
                    this.influxdbUrl,
                    this.influxdbUsername,
                    this.influxdbPassword
            );
        }
    }

    /**
     * Create the database if not exist
     */
    private void createDatabaseIfNotExists() {
        if (!this.databaseWasCreated) {

            LOG.debug("{}: Creating database with name = {}", this.getClass().getSimpleName(), this.influxdbDatabase);

            this.influxDB.createDatabase(this.influxdbDatabase);
            this.databaseWasCreated = true;
        }
    }

    /**
     * Create a BatchPoints
     */
    private void prepareBatchPoints() {

        if(!databaseWasCreated){
            this.createDatabaseIfNotExists();
        }

        this.batchPoints = BatchPoints
                .database(this.influxdbDatabase)
                .retentionPolicy("autogen")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();



    }




    private void prepareDataPoint(Map <String, Object> data) {

        if (this.batchPoints == null) {
            this.prepareBatchPoints();
        }

        final String measurement = this.influxdbMeasurementPrefix;

        //Map.Entry <String, Object> entry = data.entrySet().iterator().next();

        Point.Builder builder = Point.measurement(measurement)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag(this.tags)
                .fields(this.fields);

        for(Map.Entry<String, Object> entry: data.entrySet()) {
            builder.addField(entry.getKey(), new Double(entry.getValue().toString()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: DataPoint name={} has value type={}", this.getClass().getSimpleName(), entry.getKey(), entry.getValue().getClass().getName());

        }
        }

        builder.build();
        Point point = (Point) builder.build();
        this.batchPoints.point(point);

    }

    /**
     * Send Points to InfluxDB server
     */

    private void sendPoints() {

        //this.createDatabaseIfNotExists();

        if (this.batchPoints != null) {

            LOG.debug("{}: Sending points to database = {}", this.getClass().getSimpleName(), this.influxdbDatabase);

            this.influxDB.write(this.batchPoints);
            this.batchPoints = null;
        } else {
            LOG.warn("No points values to send");
        }
    }

    /**
     * Close connection to InfluxDB server
     */
    private void closeConnection() {

        LOG.debug("{}: Closing connection to database = {}", this.getClass().getSimpleName(), this.influxdbDatabase);

        this.influxDB.close();
    }

    /**
     * Assign the field for every dataPoint.
     *
     * @param fields
     */
    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    /**
     * Assign the tags for every dataPoint.
     *
     * @param tags
     */
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }


    /*public void execute(Tuple input, BasicOutputCollector collector) {

        ObjectMapper mapper = new ObjectMapper();

        // Take input from both Feature selection and Trends detection Bolt
        if("fsBolt".equals(input.getSourceStreamId())){

            Fields fsEventKeys = input.getFields();
            List<Object> fsEventVal = input.getValues();
            Map<String, Object> result_fsBolt = new HashMap<String, Object>();
            for(int i=0; i<fsEventKeys.size();i++){
                result_fsBolt.put(fsEventKeys.get(i), fsEventVal.get(i));
            }

            sendFSDataToDB(result_fsBolt);
        }

        if("tdBolt".equals(input.getSourceStreamId())){
            Fields tdEventKeys = input.getFields();
            List<Object> tdEventVal = input.getValues();
            Map<String, Object> result_tdBolt = new HashMap<String, Object>();
            for(int i=0; i<tdEventKeys.size();i++){
                result_tdBolt.put(tdEventKeys.get(i), tdEventVal.get(i));

            }

            sendTDDataToDB(result_tdBolt);
        }


    }*/

    public void sendFSDataToDB(Map<String, Object> fsData){

        PushToInfluxDbBolt caller = new PushToInfluxDbBolt();
        caller.prepareDataPoint(fsData);
        caller.sendPoints();
    }

    public void sendTDDataToDB(Map<String, Object> tdData){
        PushToInfluxDbBolt caller = new PushToInfluxDbBolt();
        caller.prepareDataPoint(tdData);
        caller.sendPoints();
    }


}