/**
 * MG6 MQTT → MongoDB Backend
 * Runs as a Background Worker on Render
 */

require("dotenv").config();
const mqtt = require("mqtt");
const { MongoClient } = require("mongodb");

// ================== ENV ==================
const {
  MQTT_URL,
  MQTT_USERNAME,
  MQTT_PASSWORD,
  MQTT_TOPIC,
  MONGODB_URI,
  DB_NAME,
  COLLECTION_NAME,
  MIN_SAVE_INTERVAL_SEC,
} = process.env;

// ================== VALIDATION ==================
const requiredVars = [
  "MQTT_URL",
  "MQTT_USERNAME",
  "MQTT_PASSWORD",
  "MQTT_TOPIC",
  "MONGODB_URI",
  "DB_NAME",
  "COLLECTION_NAME",
  "MIN_SAVE_INTERVAL_SEC",
];

for (const v of requiredVars) {
  if (!process.env[v]) {
    console.error(`❌ Missing env variable: ${v}`);
    process.exit(1);
  }
}

const minIntervalSec = Number(MIN_SAVE_INTERVAL_SEC);
if (!Number.isFinite(minIntervalSec) || minIntervalSec < 0) {
  console.error(
    `❌ MIN_SAVE_INTERVAL_SEC must be a valid number. Got: ${MIN_SAVE_INTERVAL_SEC}`
  );
  process.exit(1);
}
const minIntervalMs = minIntervalSec * 1000;

// ================== STATE ==================
// Mejor: throttle por topic (o cámbialo por deviceId si lo tienes dentro del payload)
const lastSavedByKey = new Map(); // key -> timestamp

// ================== MONGODB ==================
const mongoClient = new MongoClient(MONGODB_URI, {
  // Opcional: mejora estabilidad en algunos entornos
  // maxPoolSize: 10,
  // serverSelectionTimeoutMS: 10000,
});

let collection;

async function connectMongo() {
  console.log("🔌 Connecting to MongoDB...");
  await mongoClient.connect();
  const db = mongoClient.db(DB_NAME);
  collection = db.collection(COLLECTION_NAME);
  console.log(
    `✅ MongoDB connected → DB: ${DB_NAME}, Collection: ${COLLECTION_NAME}`
  );
}

// ================== MQTT ==================
function connectMQTT() {
  console.log("🔌 Connecting to MQTT...");
  console.log(`➡️  MQTT_URL: ${MQTT_URL}`);
  console.log(`➡️  MQTT_TOPIC: ${MQTT_TOPIC}`);

  // Sanitiza credenciales para evitar espacios/saltos de línea de Render
  const username = (MQTT_USERNAME || "").trim();
  const password = (MQTT_PASSWORD || "").trim();

  // Logs seguros (no imprimen la password)
  console.log(`➡️  MQTT_USERNAME: ${JSON.stringify(username)} len=${username.length}`);
  console.log(`➡️  MQTT_PASSWORD len=${password.length}`);

  const isMqtts = MQTT_URL.startsWith("mqtts://");

  const client = mqtt.connect(MQTT_URL, {
    username,
    password,
    keepalive: 60,
    reconnectPeriod: 5000,
    connectTimeout: 30_000,

    // TLS options (para mqtts)
    ...(isMqtts
      ? {
          // Déjalo en true. Tu error actual NO es TLS, es auth.
          rejectUnauthorized: true,
        }
      : {}),
  });

  client.on("connect", () => {
    console.log("✅ MQTT connected");

    client.subscribe(MQTT_TOPIC, { qos: 0 }, (err, granted) => {
      if (err) {
        console.error("❌ Subscribe error:", err.message);
        return;
      }
      console.log(
        "📡 Subscribed:",
        granted?.map((g) => `${g.topic}(qos=${g.qos})`).join(", ") || MQTT_TOPIC
      );
    });
  });

  client.on("reconnect", () => console.log("♻️ MQTT reconnecting..."));
  client.on("offline", () => console.log("📴 MQTT offline"));
  client.on("close", () => console.log("🔌 MQTT connection closed"));

  client.on("message", async (topic, message) => {
    // Key para throttle: por topic.
    // Si tu payload trae algo como deviceId/imei/sn, mejor usa eso para no bloquear otros sensores.
    const throttleKey = topic;

    try {
      const now = Date.now();
      const last = lastSavedByKey.get(throttleKey) || 0;
      if (now - last < minIntervalMs) return; // throttle

      // JSON parse seguro
      const raw = message.toString();
      let payload;
      try {
        payload = JSON.parse(raw);
      } catch (e) {
        console.error(
          "❌ Invalid JSON payload. Topic:",
          topic,
          "Raw:",
          raw.slice(0, 300)
        );
        return;
      }

      // marca throttle solo cuando ya parseó bien (y antes de guardar)
      lastSavedByKey.set(throttleKey, now);

      const doc = {
        topic,
        payload,
        receivedAt: new Date(),
      };

      await collection.insertOne(doc);
      console.log("📥 Saved reading:", doc.receivedAt.toISOString(), "Topic:", topic);
    } catch (err) {
      console.error("❌ Error processing message:", err?.message || err);
    }
  });

  client.on("error", (err) => {
    console.error("❌ MQTT error:", err.message);
  });

  return client;
}

// ================== GRACEFUL SHUTDOWN ==================
async function shutdown(signal) {
  console.log(`🛑 Received ${signal}. Closing connections...`);
  try {
    await mongoClient.close();
  } catch (e) {
    console.error("Mongo close error:", e?.message || e);
  }
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ================== START ==================
(async () => {
  try {
    console.log("🚀 Starting MG6 MQTT Backend...");
    await connectMongo();
    connectMQTT();
  } catch (err) {
    console.error("❌ Fatal error:", err?.message || err);
    process.exit(1);
  }
})();
