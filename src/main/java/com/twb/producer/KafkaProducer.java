package com.twb.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import com.twb.constant.ApplicationConstant;
import com.twb.dto.Student;

@RestController
@RequestMapping("/api/kafka/produce")
public class KafkaProducer {

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	@PostMapping("/student")
	public String sendMessage(@RequestBody Object student) {

		try {
			ListenableFuture<SendResult<String, Object>> future =kafkaTemplate.send(ApplicationConstant.TOPIC_NAME, student);
			future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
				final ObjectMapper mapper = new ObjectMapper();
				@Override
				public void onSuccess(SendResult<String, Object> result) {
					try {
						System.out.println("Sent message=[" + mapper.writeValueAsString(student) +
							"] with offset=[" + result.getRecordMetadata().offset() + "]");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				@Override
				public void onFailure(Throwable ex) {
					try {
						System.out.println("Unable to send message=["
							+ mapper.writeValueAsString(student) + "] due to : " + ex.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
				}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "json student sent successfully";
	}

	@PostMapping
	public String sendSimpleMessage(@RequestParam String message) {

		try {
			kafkaTemplate.send(ApplicationConstant.TOPIC_NAME, message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "String message sent successfully";
	}

}
