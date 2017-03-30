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
        var consumer;
        //var topic = this.topic;
        if (node.brokerConfig !== undefined) {
            node.status({
                fill: "red",
                shape: "ring",
                text: "disconnected"
            });
            if (node.topic !== undefined) {  
                // subscribe to kafka topic (if provided), otherwise print error message
                try {   
                    // create node-rdkafka consumer      
                    consumer = new Kafka.KafkaConsumer({
                        'group.id': node.cgroup,
                        'client.id': node.brokerConfig.clientid,
                        'metadata.broker.list': node.brokerConfig.broker,
                        'socket.keepalive.enable': true,
                        'enable.auto.commit': node.autocommit,
                        'queue.buffering.max.ms': 1,
                        'fetch.min.bytes': 1,
                        'fetch.wait.max.ms': 1,         //librkafka recommendation for low latency 
                        'fetch.error.backoff.ms': 100   //librkafka recommendation for low latency
                    }, {});

                    // Setup Flowing mode
                    consumer.connect();
                
                    // consumer event handlers
                    consumer
                        .on('ready', function() {
                            node.status({
                                fill: "green",
                                shape: "dot",
                                text: "connected"
                            });  
                            consumer.subscribe([node.topic]);
                            consumer.consume();
                            util.log('[rdkafka] Created consumer subscription on topic = ' + node.topic);
                        })
                        .on('data', function(data) {
                            // Output the actual message contents
                            var msg = {
                                topic: data.topic,
                                offset: data.offset,
                                partition: data.partition,
                                size: data.size
                            };
                            if (data.value) {
                                msg.payload = data.value.toString();
                            } else {
                                msg.payload = ""; //in case of msg with null value
                                //console.log('data.value was null');
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
                        })
                        .on('error', function(err) {
                            // Here's where we'll know if something went wrong consuming from Kafka
                            console.error('[rdkafka] Error in our kafka consumer');
                            console.error(err);
                        });
                } catch(e) {
                    //util.log('[rdkafka] Error creating consumer read stream:' +e);
                    util.log('[rdkafka] Error creating consumer connection:' +e);
                }
            } else {
                node.error('missing input topic');
            }
        } else {
            node.error("missing broker configuration");
        }
        node.on('close', function() {
            consumer.unsubscribe([node.topic]);
            consumer.disconnect();
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

        if (node.brokerConfig !== undefined) {
            this.status({
                fill: "red",
                shape: "ring",
                text: "disconnected"
            });
            try {
                producer = new Kafka.Producer({
                    'client.id': node.brokerConfig.clientid,
                    'metadata.broker.list': node.brokerConfig.broker,
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
            if (this.topic !== "") {
                try {
                    stream = producer.getWriteStream(node.topic);
                    util.log('[rdkafka] Created output stream with topic = ' + node.topic);

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
                var message, newstream, queuedSuccess;
                if( typeof msg.payload === 'object') {
                    message = JSON.stringify(msg.payload);
                } else {
                    message = msg.payload.toString();
                }

                if (msg === null || (msg.topic === "" && node.topic === "")) {
                    util.log("[rdkafka] request to send a NULL message or NULL topic");
                } else if (msg !== null && msg.topic !== "" && node.topic === "") {
                    // use the topic specified on the message since one is not configured 
                    try {
                        newstream = producer.getWriteStream(msg.topic);
                    } catch (e) {
                        // statements
                        util.log('[rdkafka] error creating producer write stream : ' + e);
                    }

                    // Writes a message to the stream
                    try {
                        queuedSuccess = newstream.write(message);
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
                } else if (msg !== null && node.topic !== "") {
                    // Writes a message to the cached stream
                    queuedSuccess = stream.write(message);
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
            producer.disconnect();
        });
    }
    RED.nodes.registerType("rdkafka out", RdKafkaOutNode);

};
