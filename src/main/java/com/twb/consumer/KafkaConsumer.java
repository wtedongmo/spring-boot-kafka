package com.twb.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twb.constant.ApplicationConstant;
import com.twb.dto.Student;

import java.util.Map;

@Component
public class KafkaConsumer {

	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

	@KafkaListener(groupId = ApplicationConstant.GROUP_ID_JSON, topics = ApplicationConstant.TOPIC_NAME,
			containerFactory = ApplicationConstant.KAFKA_LISTENER_CONTAINER_FACTORY)
	public void receivedMessage(Object message) throws JsonProcessingException {
		ConsumerRecord<String, Object> consumerRecord = (ConsumerRecord<String, Object>) message;
		Map map = (Map) consumerRecord.value();
		ObjectMapper mapper = new ObjectMapper();
		String jsonString = mapper.writeValueAsString(map);
		Student student = mapper.readValue(jsonString, Student.class);

//		GsonBuilder gsonBuilder = new GsonBuilder();
//		Gson gson = gsonBuilder.create();
////		String jsonString = gson.toJson(map);
//		Student stud = gson.fromJson(jsonString, Student.class);
		logger.info("\nJson message received using Kafka listener: " + student);
		logger.info("\nString message received using Kafka listener: " + jsonString);
	}
}
