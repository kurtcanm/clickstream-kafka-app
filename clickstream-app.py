import time, random
import json, string
import socket, threading
from dateutil import tz
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

def create_topics(topic_name_list):
    '''
        Creates topics for producer and consumer processes 
    '''

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
            

def get_date():
    '''
        Returns now as a date string
    '''
    
    return  datetime.now(tz=tz.UTC).strftime("%Y-%m-%dT%H:%M:%S%Z")


def get_random_date():
    '''
        Returns random date as a string
    '''
    
    random_date = datetime.now(tz=tz.UTC)- timedelta(seconds=random.randint(1000, 10000000)) 
    
    return random_date.strftime("%Y-%m-%dT%H:%M:%S%Z")

def get_random_string(stringLength=12):
    '''
        Returns random word as a string
    '''
    
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(stringLength))


def get_random_event_name(event_names):
    '''
        Returns random event name as a string
    '''
    
    return random.choice(event_names)


def acked(err, msg):
    '''
        Logs an entry about delivery process of producer
    '''
    
    if err is not None:

        print("Failed to deliver message: %s: %s" % (str(msg), str(err)) )
    else:
        print("Message produced: %s" % (str(msg)) ) 

def commit_completed(err, partitions):
    '''
        Logs an entry about sent request 
    '''
    
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))

          
def create_consumer_producer():
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
    
    return clickstream_producer, clickstream_consumer, event_listener_producer
    
    
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

def main():
    
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
    
if __name__ == '__main__':
    main()