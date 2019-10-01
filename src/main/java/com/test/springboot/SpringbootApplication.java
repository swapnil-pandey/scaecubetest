package com.test.springboot;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.net.Address;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

@SpringBootApplication
public class SpringbootApplication {

	public void init() {

	}

	private static Cluster client = null;

	public static void main(String[] args) {
		SpringApplication.run(SpringbootApplication.class, args);

		String clusterSeed = "192.168.1.177:3000";
		String ip = "192.168.1.177";

		ClusterConfig configWithFixedPort = new ClusterConfig()
				.membership(opts -> opts.seedMembers(Address.from(clusterSeed))).memberHost(ip)
				.metadataDecoder(SpringbootApplication::decode).metadataEncoder(SpringbootApplication::encode)
				.transport(t -> t.messageCodec(new MessageCodecImpl())).transport(opts -> opts.port(3000));

		client = new ClusterImpl(configWithFixedPort).handler(cluster -> {

			return new ClusterMessageHandler() {
				@Override
				public void onGossip(Message gossip) {
					handleMessages(0, client, gossip);
				}

				@Override
				public void onMessage(Message message) {
					handleMessages(1, client, message);
				}
			};
		}).startAwait();
		client.spreadGossip(Message.fromData("hello")).subscribe(null, Throwable::printStackTrace);
	}

	private static void handleMessages(int type, Cluster client, Message message) {
		if (type == 0) {
			System.out.println("heard gossip: " + message.data());
			client.spreadGossip(Message.fromData("hello")).subscribe(null, Throwable::printStackTrace);

		} else if (type == 1) {
			System.out.println("heard message: " + message.data());

		}
	}

	static void handleGossip(Cluster cient) {

	}

	static Object decode(ByteBuffer buffer) {
		return buffer;
	}

	static ByteBuffer encode(Object metadata) {
		return ByteBuffer.allocate(0);
	}

	private static class MessageCodecImpl implements MessageCodec {

		static final ObjectMapper OBJECT_MAPPER = initMapper();

		@Override
		public Message deserialize(InputStream stream) throws Exception {
			return OBJECT_MAPPER.readValue(stream, Message.class);
		}

		@Override
		public void serialize(Message message, OutputStream stream) throws Exception {
			OBJECT_MAPPER.writeValue(stream, message);
		}

		static ObjectMapper initMapper() {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
			mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
			mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
			mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT, JsonTypeInfo.As.PROPERTY);
			return mapper;
		}
	}
}