
# Click-stream Event Application

## Architecture Design Schema

![DesignSchema](/images/kafka-schema.png)
+ Click-stream producer sends records to event-topic
+ Click-stream consumer read records from event-topic
+ Number of unique events calculated
+ Event-listener producer sends results to unique-event-topic

## Installations
### Confluent Kafka
Download **Confluent Platform** from [here](https://www.confluent.io/download/) and unzip files into somewhere in local.

```console
$ vim ~/.bashrc
export CONFLUENT_HOME=/folder/path/of/confluent
export PATH=$PATH:$CONFLUENT_HOME/bin
```
Install  Confluent CLI 

```console
$ curl -L --http1.1 https://cnfl.io/cli | sh -s -- -b $CONFLUENT_HOME/bin

```

Start Confluent platform and components
```console
$ confluent local start

Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
Starting kafka-rest
kafka-rest is [UP]
Starting connect
connect is [UP]
Starting ksql-server
ksql-server is [UP]
Starting control-center
control-center is [UP]
```

## Application

### Create Python environment to run code
```console
$ cd your-project
$ python3.6 -m virtualenv venv
$ source venv/bin/activate
(env)$ pip install confluent-kafka
```
### Record types
Click-stream event

```json
{
"date":"2020-03-25T19:05:49UTC",  
"productId":"4pqurkxx3oku8yj",  
"eventName":"productview",  
"userId":"ur0kzmbj"  
}
```
Unique event name
```json
{
"productview":9278,  
"purchase":9301,  
"addtobasket":9317,  
"removefrombasket":9293,  
"receiveAt":"2020-02-27T00:06:38UTC"
}
```




### Topic creation
```python
# Admin client configuration
admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'}) 

# Example configuration of topics
topic_obj_list = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topic_name_list ]

# Call create_topics to asynchronously create topics. Returns dict of <topic,future>
topic_dict = admin_client.create_topics(topic_obj_list)

# Wait for each topic create process.
for topic, f in topic_dict.items():
    try:
        f.result() 
        print("Topic {} created".format(topic))
        
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
```
Producer and Consumer configurations
```python
producer_conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()
                }

consumer_conf = {'bootstrap.servers': "localhost:9092",
                'group.id': "group1",
                'default.topic.config': {'auto.offset.reset': 'smallest'},
                'on_commit': commit_completed
                }
                
clickstream_producer = Producer(producer_conf)
clickstream_consumer = Consumer(consumer_conf)
event_listener_producer = Producer(producer_conf)

```

### Running click-stream producer


```python
def run_clickstream_producer(producer, event_names):    
    try:
        while True:
            data = {
                "date": get_random_date(),
                "productId": get_random_string(stringLength=15),
                "eventName": get_random_event_name(event_names),
                "userId": get_random_string(stringLength=8)  
            }
            producer.produce(topic= 'topic1', 
                             key=get_random_string(), 
                             value=json.dumps(data).encode('utf-8'), 
                             callback=acked)

            # Wait up to 0.5 second for events. Callbacks will be invoked if the message is acknowledged.
            producer.poll(0.5)
    finally:                
        # Close down producer.
        producer.flush()
```



### Running click-stream consumer and event producer


```python
def run_event_consumer(consumer, producer, consumer_topic, producer_topic, event_dict):
    try:
        consumer.subscribe([consumer_topic])

        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(str(msg.error().code()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                
                # Consumed record
                data = json.loads(msg.value().decode('utf-8'))
                
                # Increment unique event count by one
                event_dict[data['eventName']] += 1
                
                # Tag with date
                event_dict['receiveAt'] = get_date()
                
                # Send unique event records to topic
                producer.produce(
                    topic= producer_topic,
                    key=get_random_string(), 
                    value=json.dumps(event_dict).encode('utf-8'), 
                    callback=acked
                )
                
                consumer.commit(async=True)
    finally:
        # Close down consumer to commit final offsets.
        producer.flush()
        consumer.close()
```

Running these producers and consumers as threads
```python
random.seed(datetime.now())

# There are two topics for the case
topic_name_list = ["event-topic","unique-event-topic"]

event_names = ['productview', 'purchase', 'addtobasket', 'removefrombasket']

event_dict = {
    'productview' : 0,
    'purchase' : 0,
    'addtobasket' : 0,
    'removefrombasket' : 0
    }

#create_topics(topic_name_list)

# Create producers and consumer for the case
clickstream_producer, clickstream_consumer, event_listener_producer = create_consumer_producer()


# Pushs clickstream data to topic 
t = threading.Thread(target=run_clickstream_producer, 
                 args=(clickstream_producer, event_names,)
                )

# Runs event listener, calculates and produce the calculated data to another topic
w = threading.Thread(target=run_event_consumer, 
                     args=(clickstream_consumer,
                           event_listener_producer, 
                           topic_name_list[0], 
                           topic_name_list[1], 
                           event_dict,
                          )
                    )
# Run threads
t.start()
w.start()

```
### Real-time Dashboard

http://localhost:9021/ on browser

Click-stream records

![Event-Topic](/images/event-topic.png)

İnput - Ouput data rate of click-stream records

![record-load](/images/event-topic2.png)

Calculated event records

![Unique-Event-Topic](/images/unique-event-topic.png)
İnput - Ouput data rate of calculated records

![record-load](/images/unique-event-topic2.png)

## Sources

+ [Confluent Platform Quick Start (Local)](https://docs.confluent.io/current/quickstart/ce-quickstart.html#ce-quickstart](https://docs.confluent.io/current/quickstart/ce-quickstart.html#ce-quickstart))

+ [Kafka Python Client](https://docs.confluent.io/current/clients/python.html)
