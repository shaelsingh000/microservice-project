const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "backend",
  brokers: [process.env.KAFKA_BROKER || "kafka:9092"]
});

const producer = kafka.producer();

// ✅ FIX: wrap in async function
async function initKafka() {
  const admin = kafka.admin();

  try {
    await admin.connect();

    await admin.createTopics({
      topics: [{ topic: "orders", numPartitions: 1 }]
    });

    console.log("Kafka topic ensured");

    await admin.disconnect();

    await producer.connect();
    console.log("Producer connected");

  } catch (err) {
    console.error("Kafka init failed:", err);
    process.exit(1); // crash → restart by Docker
  }
}

initKafka();

app.post("/order", async (req, res) => {
  const order = req.body;

  await producer.send({
    topic: "orders",
    messages: [{ value: JSON.stringify(order) }]
  });

  res.send({ status: "Order sent to Kafka" });
});

app.get("/health", (req, res) => {
  res.send("OK");
});

app.listen(3000, () => {
  console.log("Backend running on port 3000");
});
