import random
import time
import cc_config

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class Reading(object):
    """
    Sensor Reading Record

    Args:
        device_id (str): Sensor unique device_id

        co (long): Carbon Monoxide volume

        humidity (long): Humidity percentage

        motion(boolean): Whether the device has moved from its original position

        temp(long): Temperature of the device

        ampere_hour(long): charge consumed by the device

        device_ts(long): event timestamp of the device when an event is emitted
    """

    def __init__(self, device_id, co, humidity, motion, temp, ampere_hour, device_ts):
        self.device_id = device_id
        self.co = co
        self.humidity = humidity
        self.motion = motion
        self.temp = temp
        self.ampere_hour = ampere_hour
        self.device_ts = device_ts


def reading_to_dict(reading, ctx):
    """
    Returns a dict representation of a Reading  instance for serialization.

    Args:
        reading (Reading): Reading instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with sensor device reading attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return dict(device_id=reading.device_id,
                co=reading.co,
                humidity=reading.humidity,
                motion=reading.motion,
                temp=reading.temp,
                ampere_hour=reading.ampere_hour,
                device_ts=reading.device_ts)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
        in this case, msg.key() will return the sensor device id, since, that is set
        as the key in the message.
    """

    if err is not None:
        print("Delivery failed for Device event {}: {}".format(msg.key(), err))
        return
    print('Device event {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    topic = 'sensor_readings'
    schema = 'reading.avsc'

    with open(f"avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = cc_config.sr_config
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     reading_to_dict)

    producer_conf = cc_config.cc_config
    producer = Producer(producer_conf)

    print("Producing device events to topic {}. ^C to exit.".format(topic))
    while True:

        devices = ['b8:27:eb:bf:9d:51', '00:0f:00:70:91:0a', '1c:bf:ce:15:ec:4d']
        device_id = random.choice(devices)
        co = round(random.uniform(0.0011, 0.0072), 4)
        humidity = round(random.uniform(45.00, 78.00), 2)
        motion = random.choice([True, False])
        temp = round(random.uniform(17.00, 36.00), 2)
        ampere_hour = round(random.uniform(0.10, 1.80), 2)
        device_ts = int(time.time() * 1000)

        try:
            reading = Reading(device_id=device_id,
                        co=co,
                        humidity=humidity,
                        motion=motion,
                        temp=temp,
                        ampere_hour=ampere_hour,
                        device_ts=device_ts,)
            producer.produce(topic=topic,
                             key=device_id,
                             value=avro_serializer(reading, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            time.sleep(5)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue
        finally:
            producer.flush()


if __name__ == '__main__':
    main()