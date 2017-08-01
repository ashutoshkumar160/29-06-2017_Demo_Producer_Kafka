package org.stackroute;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerExampleApplication {

	private static KafkaProducer<String, String> kp;
	private static final String topic = "test4";

	public void initialize() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("request.required.acks", "1");
		kp = new KafkaProducer<String, String>(props);

	}

	public void publishMesssage() throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			System.out.print("Enter message to send to kafka broker(Press 'Y' to close producer): ");
			String msg = null;
			msg = reader.readLine();
			kp.send(new ProducerRecord<String, String>(topic, msg));
			if ("Y".equals(msg)) {
				break;
			}
			System.out.println("--> Message [" + msg + "] sent.Check message on Consumer's program console");
		}
		return;
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(ProducerExampleApplication.class, args);

		ProducerExampleApplication mp = new ProducerExampleApplication();
		// Initialize producer
		mp.initialize();
		// Publish message
		mp.publishMesssage();
		// Close the producer
		kp.close();
	}
}
