import 'dotenv/config';
import { MongoClient } from 'mongodb';
import axios from 'axios';

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DATABASE = process.env.MONGODB_DATABASE;
const LANGGRAPH_API_URL = process.env.LANGGRAPH_INSIGHTS_API_URL;
const LANGSMITH_API_KEY = process.env.LANGSMITH_API_KEY;
const ASSISTANT_ID = process.env.LANGGRAPH_INSIGHTS_AGENT_ASSISTANT_ID || 'insights-agent';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '5', 10);
const DELAY_MS = parseInt(process.env.DELAY_BETWEEN_REQUESTS_MS || '1000', 10);
const BATCH_DELAY_MS = parseInt(process.env.DELAY_BETWEEN_BATCHES_MS || '120000', 10);

const SOURCE_COLLECTION = 'meetingdata';
const TRACKING_COLLECTION = 'processed_meetingdata';

if (LANGSMITH_API_KEY) {
  // Set x-api-key header
  axios.defaults.headers.common['x-api-key'] = LANGSMITH_API_KEY;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function callInsightsAgent(fullResponse) {
  return await axios.post(`${LANGGRAPH_API_URL}/runs`, {
    assistant_id: ASSISTANT_ID,
    input: {
      messages: [
        {
          role: 'user',
          content: `{"content": "Meeting Insights: ${fullResponse}"}`,
        },
      ],
    },
  });
}

async function markAsProcessed(trackingCollection, documentId) {
  await trackingCollection.updateOne(
    { _id: documentId },
    { $set: { processedAt: new Date() } },
    { upsert: true }
  );
}

async function getUnprocessedDocuments(db) {
  const sourceCollection = db.collection(SOURCE_COLLECTION);
  const trackingCollection = db.collection(TRACKING_COLLECTION);

  // Get all processed IDs
  const processedDocs = await trackingCollection.find({}, { projection: { _id: 1 } }).toArray();
  const processedIds = new Set(processedDocs.map((doc) => doc._id.toString()));

  // Get all source documents
  const allDocuments = await sourceCollection
    .find({ createdAt: { $gt: new Date('2025-11-19') } })
    .toArray();

  // Filter out already processed
  const unprocessed = allDocuments.filter((doc) => !processedIds.has(doc._id.toString()));

  return unprocessed;
}

async function processDocument(doc, trackingCollection) {
  const docId = doc._id;

  // Convert document to string for the API call
  // Adjust this based on your document structure
  const fullResponse =
    typeof doc.llm_response?.full_response === 'string'
      ? doc.llm_response.full_response
      : JSON.stringify(doc);

  try {
    await callInsightsAgent(fullResponse);
    await markAsProcessed(trackingCollection, docId);
    return { success: true, id: docId };
  } catch (error) {
    console.error(`Failed to process document ${docId}:`, error.message);
    return { success: false, id: docId, error: error.message };
  }
}

async function main() {
  console.log('🚀 Starting Insight Agent Processor\n');

  if (!MONGODB_URI || !MONGODB_DATABASE || !LANGGRAPH_API_URL) {
    console.error('❌ Missing required environment variables. Check your .env file.');
    process.exit(1);
  }

  const client = new MongoClient(MONGODB_URI);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB\n');

    const db = client.db(MONGODB_DATABASE);
    const trackingCollection = db.collection(TRACKING_COLLECTION);

    // Create index on tracking collection for faster lookups
    await trackingCollection.createIndex({ _id: 1 });

    const unprocessedDocs = await getUnprocessedDocuments(db);
    const totalUnprocessed = unprocessedDocs.length;

    console.log(`📊 Found ${totalUnprocessed} unprocessed documents\n`);

    if (totalUnprocessed === 0) {
      console.log('✨ All documents have already been processed!');
      return;
    }

    let successCount = 0;
    let failCount = 0;

    // Process in batches
    for (let i = 0; i < unprocessedDocs.length; i += BATCH_SIZE) {
      const batch = unprocessedDocs.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(unprocessedDocs.length / BATCH_SIZE);

      console.log(`📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} documents)`);

      for (const doc of batch) {
        const result = await processDocument(doc, trackingCollection);

        if (result.success) {
          successCount++;
          console.log(`  ✅ Processed: ${result.id}`);
        } else {
          failCount++;
          console.log(`  ❌ Failed: ${result.id} - ${result.error}`);
        }

        // Delay between requests to avoid rate limiting
        await sleep(DELAY_MS);
      }

      const progress = Math.round(((i + batch.length) / totalUnprocessed) * 100);
      console.log(`📈 Progress: ${progress}% (${i + batch.length}/${totalUnprocessed})\n`);
      await sleep(BATCH_DELAY_MS);
    }

    console.log('━'.repeat(50));
    console.log(`\n🎉 Processing Complete!`);
    console.log(`   ✅ Successful: ${successCount}`);
    console.log(`   ❌ Failed: ${failCount}`);
    console.log(`   📊 Total: ${successCount + failCount}`);
  } catch (error) {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await client.close();
    console.log('\n👋 Disconnected from MongoDB');
  }
}

main();
