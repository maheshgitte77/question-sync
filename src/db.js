const { MongoClient } = require("mongodb");
const config = require("./config");

let client;

const connect = async () => {
    if (client) return client;
    client = new MongoClient(config.mongoUri, {
        maxPoolSize: 10,
        serverSelectionTimeoutMS: 20000
    });
    await client.connect();
    return client;
};

const getCollections = async () => {
    const mongoClient = await connect();
    const db = mongoClient.db(config.dbName);
    return {
        db,
        listCollection: db.collection("questions_list"),
        detailCollection: db.collection("questions_detail"),
        stateCollection: db.collection("sync_state"),
        errorCollection: db.collection("sync_errors")
    };
};

const ensureIndexes = async () => {
    const { listCollection, detailCollection, stateCollection, errorCollection } =
        await getCollections();
    await Promise.all([
        listCollection.createIndex({ slug: 1 }, { unique: true }),
        listCollection.createIndex({ problem_id: 1 }),
        listCollection.createIndex({ modified: -1 }),
        detailCollection.createIndex({ slug: 1 }, { unique: true }),
        detailCollection.createIndex({ id: 1 }),
        detailCollection.createIndex({ modified: -1 }),
        stateCollection.createIndex({ _id: 1 }),
        errorCollection.createIndex({ createdAt: -1 })
    ]);
};

module.exports = { connect, getCollections, ensureIndexes };

