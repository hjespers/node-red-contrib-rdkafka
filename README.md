#node-red-contrib-rdkafka

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
<ul>
    <li>input <img src="https://github.com/hjespers/node-red-contrib-rdkafka/blob/master/images/rdkafka-in.png"></li>
    <li>output <img src="https://github.com/hjespers/node-red-contrib-rdkafka/blob/master/images/rdkafka-out.png"></li>
</ul>

Drag either rdkafka node to the canvas and double click to configure the topic, brokers, clientID and groupID.

<img src="https://github.com/hjespers/node-red-contrib-rdkafka/blob/master/images/rdkafka-in-config.png">

Click on the pencil icon to the right of the broker selection box to configure a kafka broker connection if one does not already exist.

<img src="https://github.com/hjespers/node-red-contrib-rdkafka/blob/master/images/rdkafka-broker-config.png">

Publish and subscribe just as you would with the mqtt node with some small differences namely:
<ul>
	<li>topics should not contain "/" or "." characters
	<li>kafka wildcard/regex subscriptions are not yet fully tested
	<li>ensure you have unique Group IDs configured unless you want multiple consumers to be in a Kafka consumer group
</ul>

#Author

Hans Jespersen, https://github.com/hjespers

#Feedback and Support

For more information, feedback, or support see https://github.com/hjespers/node-red-contrib-rdkafka/issues