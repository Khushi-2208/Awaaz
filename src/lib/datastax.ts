import 'dotenv/config'
import { DataAPIClient } from '@datastax/astra-db-ts';

const client = new DataAPIClient(process.env.ASTRA_DB_APPLICATION_TOKEN);
const db = client.db(process.env.ASTRA_DB_API_ENDPOINT as string);

export const schemesCollection = db.collection(process.env.ASTRA_DB_COLLECTION as string, {keyspace: process.env.ASTRA_DB_KEYSPACE});

export async function initializeCollection() {
  try {
    await db.createCollection(process.env.ASTRA_DB_COLLECTION as string, {
      vector: {
        dimension: 768, // Gemini text-embedding-004 dimension
        metric: 'cosine'
      }
    });
    console.log('Collection created successfully');
  } catch (error: any) {
    if (error.message.includes('already exists')) {
      console.log('Collection already exists');
    } else {
      throw error;
    }
  }
}