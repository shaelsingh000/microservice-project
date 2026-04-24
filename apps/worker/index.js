const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "worker",
  brokers: [process.env.KAFKA_BROKER || "kafka:9092"]
});

// ✅ DEFINE CONSUMER HERE (GLOBAL SCOPE)
const consumer = kafka.consumer({ groupId: "order-group" });

async function run() {
  let retries = 5;

  while (retries) {
    try {
      console.log("Connecting to Kafka...");
      await consumer.connect();
      console.log("Connected to Kafka");
      break;
    } catch (err) {
      console.log("Retrying Kafka connection...");
      retries--;
      await new Promise(res => setTimeout(res, 5000));
    }
  }

  // ❗ If still not connected → crash
  if (retries === 0) {
    console.error("Failed to connect to Kafka");
    process.exit(1);
  }

  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("Received order:", message.value.toString());
    }
  });
}

run();
