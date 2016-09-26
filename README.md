node-red-contrib-rdkafka
########################


Node-RED (http://nodered.org) nodes for publish/subscribe messaging using the new Apache Kafka Consumer/Producer API and without requiring direct communications with Zookeeper. Based on the the high performance librdkafka Apache Kafka C/C++ library (https://github.com/edenhill/librdkafka).

Works with Apache Kafka 0.9 and 0.10 including Confluent Open Source and Enterprise distributions 2.0 and 3.0.


#Install

Run the following command in the root directory of your Node-RED install

    npm install node-red-contrib-rdkafka

You may see a lot of warnings as librdkafka compiles and installs, particularily on MacOS, but it does work.

Start node-red as normal

	node-red -v

Point your browser to http://localhost:1880

You should see white rdkafka input and output nodes in the pallet on the left side of the screen.

[input png]
[output png]

Drag either rdkafka node to the canvas and double click to configure the topic, brokers, clientID and groupID.

[rdkafka config dialog]

Click on the pencil icon to the right of the broker selection box to configure a kafka broker connection if one does not already exist.

[broker config dialog]

Publish and subscribe just as you would with the mqtt node with some small differences namely:

topics should not contain "/" or "." characters
kafka wildcard/regex subscriptions are not yet fully tested
ensure you have unique Group IDs configured unless you want multiple consumers to be in a Kafka consumer group

#Author

Hans Jespersen, https://github.com/hjespers

