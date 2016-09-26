

module.exports = function(RED) {
    "use strict";
    var Kafka = require('node-rdkafka'); 
    var util = require("util");

    function KafkaBrokerNode(n) {
        RED.nodes.createNode(this,n);
        this.broker = n.broker;
        this.clientid = n.clientid;
    }
    RED.nodes.registerType("kafka-broker",KafkaBrokerNode,{
    });

    function RdKafkaInNode(n) {
        RED.nodes.createNode(this,n);
        this.topic = n.topic;
        this.broker = n.broker;
        this.cgroup = n.cgroup;
        this.brokerConfig = RED.nodes.getNode(this.broker);
        var node = this;
        var instream;
        if (this.brokerConfig) {
            node.status({fill:"red",shape:"ring",text:"disconnected"});
            var consumer = new Kafka.KafkaConsumer({
                'group.id': this.cgroup,
                'client.id': this.brokerConfig.clientid,
                'metadata.broker.list': this.brokerConfig.broker   
            }, {});
            if (this.topic) {
                instream = consumer.getReadStream(this.topic);
                util.log('[rdkafka] Created input stream on topic = ' + this.topic);
                //console.log('[rdkafka]  group.id = ' + this.cgroup);
                //console.log('[rdkafka]  metadata.broker.list = ' + this.brokerConfig.broker);


                instream.on('data', function(data) {
                  //console.log('Got message');
                  //console.log(data.message.toString());
                  var msg = {
                    payload : data.message.toString(),
                    topic : data.topic,
                    offset : data.offset,
                    partition : data.partition,
                    size : data.size
                  }
                  node.send(msg);
                });

                instream.on('error', function (err) {
                      // Here's where we'll know if something went wrong sending to Kafka
                      console.error('[rdkafka] Error in our kafka input stream');
                      console.error(err);
                });  

                node.status({fill:"green",shape:"dot",text:"connected"});
            } else {
                this.error('missing input topic');
            }
        } else {
            this.error("missing broker configuration");
        }   
        this.on('close', function() {
            consumer.disconnect(); 
        });
    }
    RED.nodes.registerType("rdkafka in",RdKafkaInNode);

    function RdKafkaOutNode(n) {
        RED.nodes.createNode(this,n);
        this.topic = n.topic;
        this.broker = n.broker;
        this.brokerConfig = RED.nodes.getNode(this.broker);
        var node = this;
        var producer, stream;

        if (this.brokerConfig ) {
            this.status({fill:"red",shape:"ring",text:"disconnected"});
            producer = new Kafka.Producer({
               'client.id': this.brokerConfig.clientid,
               'metadata.broker.list': this.brokerConfig.broker,
               'compression.codec': 'gzip',
               'retry.backoff.ms': 200,
               'message.send.max.retries': 10,
               'socket.keepalive.enable': true,
               'queue.buffering.max.messages': 100000,
               'queue.buffering.max.ms': 100,
               'batch.num.messages': 1000000,
               'session.timeout.ms': 5000    
            })
            if (this.topic != "" ) {
                stream = producer.getWriteStream(this.topic);
                util.log('[rdkafka] Created output stream with topic = ' + this.topic );
            }
            // This call returns a new writable stream to our topic 'topic-name'
            this.status({fill:"green",shape:"dot",text:"connected"});

            this.on("input",function(msg) {
                if (msg == null || (msg.topic == "" && this.topic == "")) {
                    util.log("[rdkafka] request to send a NULL message or NULL topic on session: " + this.client.ref + " object instance: " + this.client[("_instances")]);
                } else if (msg != null && msg.topic != "" && this.topic == "" ) {
                    // use the topic on the message
                    var newstream = producer.getWriteStream(msg.topic);
                    // Writes a message to the stream
                    var queuedSuccess = newstream.write(msg.payload.toString());
                    if (queuedSuccess) {
                      //console.log('[rdkafka] We queued our message using topic from msg.topic!');
                    } else {
                      // Note that this only tells us if the stream's queue is full,
                      // it does NOT tell us if the message got to Kafka!  See below...
                      util.log('[rdkafka] Too many messages in our queue already');
                    }

                    stream.on('error', function (err) {
                      // Here's where we'll know if something went wrong sending to Kafka
                      console.error('[rdkafka] Error in our kafka stream');
                      console.error(err);
                    })   
                } else if (msg != null && this.topic != "" ) {
                    //console.log('***got here***');
                    //console.log('topic = ' + this.topic);
                    //console.log('msg.topic = ' + msg.topic);
                    //console.log('msg.payload = ' + msg.payload);
                    // Writes a message to the cached stream
                    var queuedSuccess = stream.write(msg.payload.toString());
                    if (queuedSuccess) {
                      //console.log('[rdkafka] We queued our message!');
                    } else {
                      // Note that this only tells us if the stream's queue is full,
                      // it does NOT tell us if the message got to Kafka!  See below...
                      console.log('[rdkafka]Too many messages in our queue already');
                    }

                    stream.on('error', function (err) {
                      // Here's where we'll know if something went wrong sending to Kafka
                      console.error('[rdkafka] Error in our kafka stream');
                      console.error(err);
                    })   
                }
            });
        } else {
            this.error("[rdkafka] missing broker configuration");
        }
        this.on('close', function() {
            producer.disconnect();
        });
    }
    RED.nodes.registerType("rdkafka out",RdKafkaOutNode);

};
