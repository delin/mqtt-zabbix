#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Kyle Gordon"
__copyright__ = "Copyright (C) Kyle Gordon"

import csv
import logging
import os
import signal
import socket
import sys
import time

import paho.mqtt.client as mqtt
import protobix

if sys.version_info >= (3, 0):
    import configparser
else:
    import ConfigParser as configparser


class ZabbixMQTT:
    def __init__(self, config_path="/etc/mqtt-zabbix/mqtt-zabbix.cfg"):
        # Read the config file
        config = configparser.RawConfigParser()
        config.read(config_path)

        # Use ConfigParser to pick out the settings
        self.DEBUG = config.getboolean("global", "DEBUG")
        self.LOGFILE = config.get("global", "LOGFILE")
        self.MQTT_HOST = config.get("global", "MQTT_HOST")
        self.MQTT_PORT = config.getint("global", "MQTT_PORT")
        self.MQTT_TOPIC = config.get("global", "MQTT_TOPIC")
        self.KEYHOST = config.get("global", "KEYHOST")
        self.KEYFILE = config.get("global", "KEYFILE")

        self.APPNAME = "mqtt-zabbix"
        self.PRESENCETOPIC = "clients/" + socket.getfqdn() + "/" + self.APPNAME + "/state"
        client_id = self.APPNAME + "_%d" % os.getpid()
        self.mqttc = mqtt.Client(client_id=client_id)

        LOGFORMAT = "%(asctime)-15s %(message)s"

        if self.DEBUG:
            logging.basicConfig(filename=self.LOGFILE,
                                level=logging.DEBUG,
                                format=LOGFORMAT)
        else:
            logging.basicConfig(filename=self.LOGFILE,
                                level=logging.INFO,
                                format=LOGFORMAT)

        self.zbx_sender = protobix.DataContainer()

    def start(self):
        # Use the signal module to handle signals
        signal.signal(signal.SIGTERM, self.cleanup)
        signal.signal(signal.SIGINT, self.cleanup)

        # Connect to the broker
        self.connect()

        # Try to loop_forever until interrupted
        try:
            self.mqttc.loop_forever()
        except KeyboardInterrupt:
            logging.info("Interrupted by keypress")
            sys.exit(0)

    def on_publish(self, mosq, obj, mid):
        """
        What to do when a message is published
        """
        logging.debug("MID " + str(mid) + " published.")

    def on_subscribe(self, mosq, obj, mid, qos_list):
        """
        What to do in the event of subscribing to a topic"
        """
        logging.debug("Subscribe with mid " + str(mid) + " received.")

    def on_unsubscribe(self, mosq, obj, mid):
        """
        What to do in the event of unsubscribing from a topic
        """
        logging.debug("Unsubscribe with mid " + str(mid) + " received.")

    def on_connect(self, self2, mosq, obj, result_code):
        """
        Handle connections (or failures) to the broker.
        This is called after the client has received a CONNACK message
        from the broker in response to calling connect().
        The parameter rc is an integer giving the return code:

        0: Success
        1: Refused â unacceptable protocol version
        2: Refused â identifier rejected
        3: Refused â server unavailable
        4: Refused â bad user name or password (MQTT v3.1 broker only)
        5: Refused â not authorised (MQTT v3.1 broker only)
        """
        logging.debug("on_connect RC: " + str(result_code))
        if result_code == 0:
            logging.info("Connected to %s:%s", self.MQTT_HOST, self.MQTT_PORT)
            # Publish retained LWT as per
            # http://stackoverflow.com/q/97694
            # See also the will_set function in connect() below
            self.mqttc.publish(self.PRESENCETOPIC, "1", retain=True)
            self.process_connection()
        elif result_code == 1:
            logging.info("Connection refused - unacceptable protocol version")
            self.cleanup()
        elif result_code == 2:
            logging.info("Connection refused - identifier rejected")
            self.cleanup()
        elif result_code == 3:
            logging.info("Connection refused - server unavailable")
            logging.info("Retrying in 30 seconds")
            time.sleep(30)
        elif result_code == 4:
            logging.info("Connection refused - bad user name or password")
            self.cleanup()
        elif result_code == 5:
            logging.info("Connection refused - not authorised")
            self.cleanup()
        else:
            logging.warning("Something went wrong. RC:" + str(result_code))
            self.cleanup()

    def on_disconnect(self, mosq, obj, result_code):
        """
        Handle disconnections from the broker
        """
        if result_code == 0:
            logging.info("Clean disconnection")
        else:
            logging.info("Unexpected disconnection! Reconnecting in 5 seconds")
            logging.debug("Result code: %s", result_code)
            time.sleep(5)

    def on_message(self, mosq, obj, msg):
        """
        What to do when the client recieves a message from the broker
        """
        logging.debug("Received: " + str(msg.payload) +
                      " received on topic " + str(msg.topic) +
                      " with QoS " + str(msg.qos))
        self.process_message(msg)

    def on_log(self, mosq, obj, level, string):
        """
        What to do with debug log output from the MQTT library
        """
        logging.debug(string)

    def process_connection(self):
        """
        What to do when a new connection is established
        """
        logging.debug("Processing connection")
        self.mqttc.subscribe(self.MQTT_TOPIC, 2)

    def process_message(self, msg):
        """
        What to do with the message that's arrived.
        Looks up the topic in the KeyMap dictionary, and forwards
        the message onto Zabbix using the associated Zabbix key
        """
        logging.debug("Processing : " + msg.topic)
        topics = self.get_topics()
        if msg.topic in topics:
            if msg.payload == "ON":
                msg.payload = 1
            if msg.payload == "OFF":
                msg.payload = 0

            zbx_key = topics[msg.topic]

            logging.info("Sending %s %s to Zabbix for key %s", msg.topic, msg.payload, zbx_key)

            self.zbx_sender.data_type = 'items'
            self.zbx_sender.add_item(self.KEYHOST, zbx_key, str(msg.payload))
            self.zbx_sender.send()
        else:
            # Received something with a /raw/ topic,
            # but it didn't match anything. Log it, and discard it
            logging.debug("Unknown: %s", msg.topic)

    def cleanup(self, signum=15, frame=None):
        """
        Signal handler to ensure we disconnect cleanly
        in the event of a SIGTERM or SIGINT.
        """
        logging.info("Disconnecting from broker")
        # Publish a retained message to state that this client is offline
        self.mqttc.publish(self.PRESENCETOPIC, "0", retain=True)
        self.mqttc.disconnect()
        logging.info("Exiting on signal %d", signum)
        sys.exit(signum)

    def connect(self):
        """
        Connect to the broker, define the callbacks, and subscribe
        This will also set the Last Will and Testament (LWT)
        The LWT will be published in the event of an unclean or
        unexpected disconnection.
        """
        logging.debug("Connecting to %s:%s", self.MQTT_HOST, self.MQTT_PORT)
        # Set the Last Will and Testament (LWT) *before* connecting
        self.mqttc.will_set(self.PRESENCETOPIC, "0", qos=0, retain=True)
        result = self.mqttc.connect(self.MQTT_HOST, self.MQTT_PORT, 60)
        if result != 0:
            logging.info("Connection failed with error code %s. Retrying", result)
            time.sleep(10)
            self.connect()

        # Define the callbacks
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_disconnect = self.on_disconnect
        self.mqttc.on_publish = self.on_publish
        self.mqttc.on_subscribe = self.on_subscribe
        self.mqttc.on_unsubscribe = self.on_unsubscribe
        self.mqttc.on_message = self.on_message

        if self.DEBUG:
            self.mqttc.on_log = self.on_log

    def get_topics(self):
        """
        Read the topics and keys into a dictionary for internal lookups
        """
        logging.debug("Loading map")
        with open(self.KEYFILE, mode="r") as inputfile:
            reader = csv.reader(inputfile)
            return dict((rows[0], rows[1]) for rows in reader)


if __name__ == "__main__":
    zbx_mqtt = ZabbixMQTT()
    zbx_mqtt.start()
