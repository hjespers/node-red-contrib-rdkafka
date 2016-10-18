"use strict";
module.exports = function(RED) {
    var Kafka = require('node-rdkafka');
    var util = require("util");

    function KafkaBrokerNode(n) {
        RED.nodes.createNode(this, n);
        this.broker = n.broker;
        this.clientid = n.clientid;
    }
    RED.nodes.registerType("kafka-broker", KafkaBrokerNode, {});

    function RdKafkaInNode(n) {
        RED.nodes.createNode(this, n);
        this.topic = n.topic;
        this.broker = n.broker;
        this.cgroup = n.cgroup;
        this.autocommit = n.autocommit;
        this.brokerConfig = RED.nodes.getNode(this.broker);
        var node = this;
        var instream;
        //var topic = this.topic;
        if (this.brokerConfig) {
            node.status({
                fill: "red",
                shape: "ring",
                text: "disconnected"
            });
            try {
                var consumer = new Kafka.KafkaConsumer({
                    'group.id': this.cgroup,
                    'client.id': this.brokerConfig.clientid,
                    'metadata.broker.list': this.brokerConfig.broker,
                    'socket.keepalive.enable': true,
                    'enable.auto.commit': this.autocommit,
                    'queue.buffering.max.ms': 1,
                    'fetch.min.bytes': 1,
                    'fetch.wait.max.ms': 1,         //librkafka recommendation for low latency 
                    'fetch.error.backoff.ms': 100   //librkafka recommendation for low latency
                }, {});
            } catch(e) {
                console.log(e);
            }

            // subscribe to kafka topic (if provided), otherwise print error message
            if (this.topic) {
                try {
                    //stream api
                    instream = consumer.getReadStream(this.topic, {
                        waitInterval: 0
                    });
                } catch(e) {
                    //util.log('[rdkafka] Error creating consumer read stream:' +e);
                    util.log('[rdkafka] Error creating consumer connection:' +e);
                }
                
                util.log('[rdkafka] Created input stream on topic = ' + this.topic);

                node.status({
                    fill: "green",
                    shape: "dot",
                    text: "connected"
                });                   

                instream.on('data', function(data) {
                    //console.log('Got message');
                    //console.log(data.message.toString());
                    var msg = {
                        topic: data.topic,
                        offset: data.offset,
                        partition: data.partition,
                        size: data.size
                    };
                    if (data.message) {
                        msg.payload = data.message.toString();
                    } else {
                        msg.payload = ""; //in case of msg with null value
                    }
                    if (data.key) {
                        msg.key = data.key.toString();
                    }
                    try {
                        node.send(msg);
                    } catch(e) {
                        // statements
                        util.log('[rdkafka] error sending node message: ' +e);
                    }
                    
                });

                instream.on('error', function(err) {
                    // Here's where we'll know if something went wrong sending to Kafka
                    console.error('[rdkafka] Error in our kafka input stream');
                    console.error(err);
                });

            } else {
                this.error('missing input topic');
            }
        } else {
            this.error("missing broker configuration");
        }
        this.on('close', function() {
            //consumer.disconnect();
        });
    }
    RED.nodes.registerType("rdkafka in", RdKafkaInNode);

    function RdKafkaOutNode(n) {
        RED.nodes.createNode(this, n);
        this.topic = n.topic;
        this.broker = n.broker;
        this.brokerConfig = RED.nodes.getNode(this.broker);
        var node = this;
        var producer, stream;

        if (this.brokerConfig) {
            this.status({
                fill: "red",
                shape: "ring",
                text: "disconnected"
            });
            try {
                producer = new Kafka.Producer({
                    'client.id': this.brokerConfig.clientid,
                    'metadata.broker.list': this.brokerConfig.broker,
                    //'compression.codec': 'gzip',
                    'retry.backoff.ms': 200,
                    'message.send.max.retries': 15,
                    'socket.keepalive.enable': true,
                    'queue.buffering.max.messages': 100000,
                    'queue.buffering.max.ms': 10,
                    'batch.num.messages': 1000000
                });    
            } catch(e) {
                console.log(e);
            }

            // if kafka topic is specified it takes precidence over msg.topic so just create a output stream once and reuse it
            if (this.topic != "") {
                try {
                    stream = producer.getWriteStream(this.topic);
                    util.log('[rdkafka] Created output stream with topic = ' + this.topic);

                    stream.on('error', function(err) {
                        // Here's where we'll know if something went wrong sending to Kafka
                        util.error('[rdkafka] Error in our kafka stream');
                        console.error(err);
                    }); 
                } catch (e) {
                    util.log('[rdkafka] error creating producer writestream: ' + e);
                }
            }
            // This call returns a new writable stream to our topic 'topic-name'
            this.status({
                fill: "green",
                shape: "dot",
                text: "connected"
            });

            this.on("input", function(msg) {
                //handle different payload types including JSON object
                var message;
                if( typeof msg.payload === 'object') {
                    message = JSON.stringify(msg.payload);
                } else {
                    message = msg.payload.toString();
                }

                if (msg == null || (msg.topic == "" && this.topic == "")) {
                    util.log("[rdkafka] request to send a NULL message or NULL topic on session: " + this.client.ref + " object instance: " + this.client[("_instances")]);
                } else if (msg != null && msg.topic != "" && this.topic == "") {
                    // use the topic specified on the message since one is not configured 
                    try {
                        var newstream = producer.getWriteStream(msg.topic);
                    } catch (e) {
                        // statements
                        util.log('[rdkafka] error creating producer write stream : ' + e);
                    }

                    // Writes a message to the stream
                    try {
                        var queuedSuccess = newstream.write(message);
                    } catch(e) {
                        util.log('[rdkafka] error writing to producer writestream: ' + e);
                    }

                    if (queuedSuccess) {
                        //console.log('[rdkafka] We queued our message using topic from msg.topic!');
                    } else {
                        // Note that this only tells us if the stream's queue is full,
                        // it does NOT tell us if the message got to Kafka!  See below...
                        util.log('[rdkafka] Too many messages in our queue already');
                    }

                    newstream.on('error', function(err) {
                        // Here's where we'll know if something went wrong sending to Kafka
                        console.error('[rdkafka] Error in our kafka stream');
                        console.error(err);
                    });
                } else if (msg != null && this.topic != "") {
                    // Writes a message to the cached stream
                    var queuedSuccess = stream.write(message);
                    if (queuedSuccess) {
                        //console.log('[rdkafka] We queued our message!');
                    } else {
                        // Note that this only tells us if the stream's queue is full,
                        // it does NOT tell us if the message got to Kafka!  See below...
                        util.log('[rdkafka]Too many messages in our queue already');
                    }
                }
            });
        } else {
            this.error("[rdkafka] missing broker configuration");
        }
        this.on('close', function() {
            //producer.disconnect();
        });
    }
    RED.nodes.registerType("rdkafka out", RdKafkaOutNode);

};