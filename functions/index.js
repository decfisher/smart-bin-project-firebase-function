require("dotenv").config();

const functions = require("firebase-functions");
const mongoose = require("mongoose");

const BinDataEntry = require("./models/BinDataObj");

// Listen to Google Pub/Sub message publish
exports.pubSubToMongoDB = functions.pubsub.topic("smart-bin-data").onPublish(async (message) => {
    mongoose.connect(
        process.env.MONGO_DB_CONNECTION_URL, 
        { useNewUrlParser: true, useUnifiedTopology: true, dbName: process.env.DB_NAME }
    );

    const binDataEntry = new BinDataEntry({
        deviceId: message.attributes.device_id,
        publishedAt: message.attributes.published_at,
        percentageFull: parseInt(message.json.percentageFull),
        rubbishVolume: parseFloat(message.json.rubbishVolume),
        latitude: parseFloat(message.json.latitude),
        longitude: parseFloat(message.json.longitude)
    });

    try {
        const saved = await binDataEntry.save();
        functions.logger.log(JSON.stringify(saved));
    } catch (err) {
        functions.logger.log(JSON.stringify({ message: err }));
    }
})
