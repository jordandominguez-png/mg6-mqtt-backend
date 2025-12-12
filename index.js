/**
 * MG6 MQTT → MongoDB Backend
 * Runs as a Background Worker on Render
 */

require('dotenv').config();
const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');

// ================== ENV ==================
const {
  MQTT_URL,
  MQTT_USERNAME,
  MQTT_PASSWORD,
  MQTT_TOPIC,
  MONGODB_URI,
  DB_NAME,
  COLLECTION_NAME,
  MIN_SAVE_INTERVAL_SEC
} = process.env;

// ================== VALIDATION ==================
const requiredVars = [
  'MQTT_URL',
  'MQTT_USERNAME',
  'MQTT_PASSWORD',
  'MQTT_TOPIC',
  'MONGODB_URI',
  'DB_NAME',
  'COLLECTION_NAME',
  'MIN_SAVE_INTERVAL_SEC'
];

for (const v of requiredVars) {
  if (!process.env[v]) {
    console.error(`❌ Missing env variable: ${v}`);
    process.exit(1);
  }
}

// ================== STATE ==================
let lastSavedAt = 0;
const minIntervalMs = Number(MIN_SAVE_INTERVAL_SEC) * 1000;

// ================== MONGODB ==================
const mongoClient = new MongoClient(MONGODB_URI);
let collection;

async function connectMongo() {
  console.log('🔌 Connecting to MongoDB...');
  await mongoClient.connect();
  const db = mongoClient.db(DB_NAME);
  collection = db.collection(COLLECTION_NAME);
  console.log(`✅ MongoDB connected → DB: ${DB_NAME}, Collection: ${COLLECTION_NAME}`);
}

// ================== MQTT ==================
function connectMQTT() {
  console.log('🔌 Connecting to MQTT...');

  const client = mqtt.connect(MQTT_URL, {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    keepalive: 60,
    reconnectPeriod: 5000
  });

  client.on('connect', () => {
    console.log('✅ MQTT connected');
    client.subscribe(MQTT_TOPIC, () => {
      console.log(`📡 Subscribed to topic: ${MQTT_TOPIC}`);
    });
  });

  client.on('message', async (topic, message) => {
    try {
      const now = Date.now();

      if (now - lastSavedAt < minIntervalMs) {
        return; // throttle save
      }

      lastSavedAt = now;

      const payload = JSON.parse(message.toString());

      const doc = {
        topic,
        payload,
        receivedAt: new Date()
      };

      await collection.insertOne(doc);

      console.log('📥 Saved reading:', doc.receivedAt.toISOString());
    } catch (err) {
      console.error('❌ Error processing message:', err.message);
    }
  });

  client.on('error', (err) => {
    console.error('❌ MQTT error:', err.message);
  });
}

// ================== START ==================
(async () => {
  try {
    console.log('🚀 Starting MG6 MQTT Backend...');
    await connectMongo();
    connectMQTT();
  } catch (err) {
    console.error('❌ Fatal error:', err);
    process.exit(1);
  }
})();
