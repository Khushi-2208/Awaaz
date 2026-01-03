// import 'dotenv/config';
// import { DataAPIClient } from "@datastax/astra-db-ts";
// import { GoogleGenerativeAI } from "@google/generative-ai";
// import { PuppeteerWebBaseLoader } from "@langchain/community/document_loaders/web/puppeteer";
// import { promises as fs } from "fs";

// const {
//   ASTRA_DB_KEYSPACE, 
//   ASTRA_DB_COLLECTION, 
//   ASTRA_DB_API_ENDPOINT, 
//   ASTRA_DB_APPLICATION_TOKEN, 
//   GOOGLE_GEMINI_API_KEY
// } = process.env;

// const genAI = new GoogleGenerativeAI(GOOGLE_GEMINI_API_KEY!);
// const client = new DataAPIClient(ASTRA_DB_APPLICATION_TOKEN);
// const db = client.db(ASTRA_DB_API_ENDPOINT as string, {keyspace: ASTRA_DB_KEYSPACE});

// const PROGRESS_FILE = "./src/scripts/processed_schemes_structured.json";

// const SchemeURLs = [
//     'https://pmaymis.gov.in/',
//     'https://pmjay.gov.in/',
//     'https://pmkisan.gov.in/',
//     'https://www.mudra.org.in/',
//     'https://www.india.gov.in/sukanya-samriddhi-yojana',
//     'https://www.npscra.nsdl.co.in/',
//     'https://pmjdy.gov.in/',
//     'https://www.india.gov.in/spotlight/pradhan-mantri-jan-dhan-yojana',
//     'https://wcd.nic.in/bbbp-schemes',
//     'https://www.india.gov.in/my-government/schemes',
//     'https://www.india.gov.in/spotlight/scholarships-students',
//     'https://labour.gov.in/schemes',
//     'https://mmry.brlps.in/',
//     'https://pmujjwalayojana.in/janani-bal-suraksha-yojana/',
//     'https://missionshakti.wcd.gov.in/',
//     'https://pmmvy.wcd.gov.in/',
//     'https://pmdaksh.dosje.gov.in/',
//     'https://pmajay.dosje.gov.in/',
//     'https://eshram.gov.in/social-security-welfare-schemes',
//     'https://www.pmuy.gov.in/',
//     'https://lakhpatididi.gov.in/'
// ];

// interface StructuredScheme {
//   name: string;
//   nameHindi: string;
//   eligibility: string;
//   eligibilityHindi: string;
//   benefits: string;
//   benefitsHindi: string;
//   category: string;
//   targetAge?: string;
//   targetGender?: string;
//   targetIncome?: string;
//   applyLink: string;
//   rawContent: string;
// }

// const loadProcessedUrls = async (): Promise<Set<string>> => {
//   try {
//     const data = await fs.readFile(PROGRESS_FILE, "utf-8");
//     return new Set(JSON.parse(data));
//   } catch (error) {
//     return new Set();
//   }
// };

// const saveProcessedUrl = async (url: string, processedUrls: Set<string>) => {
//   processedUrls.add(url);
//   await fs.writeFile(PROGRESS_FILE, JSON.stringify(Array.from(processedUrls), null, 2), "utf-8");
// };

// const scrapePage = async (url: string): Promise<string> => {
//   const loader = new PuppeteerWebBaseLoader(url, {
//     launchOptions: { headless: true },
//     gotoOptions: { waitUntil: "domcontentloaded" },
//     evaluate: async (page, browser) => {
//       const result = await page.evaluate(() => document.body.innerHTML);
//       await browser.close();
//       return result;
//     }
//   });
//   return (await loader.scrape())?.replace(/<[^>]*>?/gm, ' ').replace(/\s+/g, ' ') || '';
// };

// const extractSchemeWithGemini = async (content: string, url: string): Promise<StructuredScheme | null> => {
//   try {
//     // Use gemini-1.5-pro-latest which should be available
//     const model = genAI.getGenerativeModel({ 
//       model: "gemini-2.5-flash"
//     });

//     const prompt = `Analyze this Indian government scheme website and extract information.

// Content: ${content.substring(0, 6000)}
// URL: ${url}

// Extract and return ONLY a JSON object with this exact structure:
// {
//   "name": "Scheme name in English",
//   "nameHindi": "योजना का नाम हिंदी में",
//   "eligibility": "Who can apply (age, gender, income, occupation)",
//   "eligibilityHindi": "पात्रता हिंदी में",
//   "benefits": "What beneficiaries get",
//   "benefitsHindi": "लाभ हिंदी में",
//   "category": "housing/healthcare/agriculture/entrepreneurship/education/pension/banking/women/employment",
//   "targetAge": "18-60 or all ages",
//   "targetGender": "male/female/all",
//   "targetIncome": "income criteria or all",
//   "applyLink": "${url}"
// }

// Return ONLY the JSON object, no markdown formatting, no explanation.`;

//     const result = await model.generateContent(prompt);
//     const response = await result.response;
//     let text = response.text();
    
//     // Clean up markdown formatting
//     text = text.replace(/```json\n?|\n?```/g, '').trim();
    
//     const parsed = JSON.parse(text);
//     return { ...parsed, rawContent: content.substring(0, 2000) };
    
//   } catch (error: any) {
//     console.error('Gemini extraction error:', error.message);
//     return null;
//   }
// };

// const createEmbedding = async (text: string): Promise<number[]> => {
//   try {
//     // Use models.embedContent for embeddings with text-embedding-004
//     const model = genAI.getGenerativeModel({ 
//       model: "text-embedding-004"
//     });
    
//     const result = await model.embedContent(text);
//     return result.embedding.values;
    
//   } catch (error: any) {
//     console.error('Embedding error:', error.message);
//     throw error;
//   }
// };

// const createCollection = async () => {
//   try {
//     await db.createCollection(ASTRA_DB_COLLECTION as string, {
//       vector: { dimension: 768, metric: "dot_product" }
//     });
//     console.log('Collection created');
//   } catch (error: any) {
//     if (error.message?.includes('already exists')) {
//       console.log('Collection exists');
//     } else {
//       throw error;
//     }
//   }
// };

// const ingestStructuredSchemes = async () => {
//   await createCollection();
//   const collection = db.collection(ASTRA_DB_COLLECTION as string);
//   const processedUrls = await loadProcessedUrls();

//   console.log('Starting structured ingestion...\n');

//   for (const url of SchemeURLs) {
//     if (processedUrls.has(url)) {
//       console.log(`Skipping: ${url}`);
//       continue;
//     }

//     console.log(`\nProcessing: ${url}`);

//     try {
//       // Scrape
//       console.log('Scraping...');
//       const content = await scrapePage(url);
      
//       if (content.length < 500) {
//         console.log('Insufficient content');
//         await saveProcessedUrl(url, processedUrls);
//         continue;
//       }

//       // Extract with Gemini
//       console.log('Extracting scheme details...');
//       const scheme = await extractSchemeWithGemini(content, url);
      
//       if (!scheme) {
//         console.log('Failed to extract');
//         await saveProcessedUrl(url, processedUrls);
//         continue;
//       }

//       console.log(`Extracted: ${scheme.name}`);

//       // Create embedding
//       console.log('Creating embedding...');
//       const embeddingText = `${scheme.name} ${scheme.nameHindi} ${scheme.eligibility} ${scheme.eligibilityHindi} ${scheme.benefits} ${scheme.benefitsHindi} Category: ${scheme.category} Age: ${scheme.targetAge} Gender: ${scheme.targetGender} Income: ${scheme.targetIncome}`.trim();
      
//       const vector = await createEmbedding(embeddingText);

//       // Store
//       console.log('Storing in database...');
//       await collection.insertOne({
//         ...scheme,
//         $vector: vector,
//         createdAt: new Date().toISOString()
//       });

//       console.log('SUCCESS');

//       await saveProcessedUrl(url, processedUrls);
      
//       // Rate limiting - 3 seconds between requests
//       await new Promise(resolve => setTimeout(resolve, 3000));

//     } catch (error: any) {
//       console.error(`Error: ${error.message}`);
      
//       if (error.message?.includes('429') || error.message?.includes('quota')) {
//         console.log('Rate limit - waiting 60 seconds');
//         await new Promise(resolve => setTimeout(resolve, 60000));
//       }
//     }
//   }

//   console.log('\nIngestion complete!');
// };

// ingestStructuredSchemes();

import 'dotenv/config';
import { DataAPIClient } from "@datastax/astra-db-ts";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { PuppeteerWebBaseLoader } from "@langchain/community/document_loaders/web/puppeteer";
import { promises as fs } from "fs";

const {
  ASTRA_DB_KEYSPACE, 
  ASTRA_DB_COLLECTION, 
  ASTRA_DB_API_ENDPOINT, 
  ASTRA_DB_APPLICATION_TOKEN, 
  GOOGLE_GEMINI_API_KEY
} = process.env;

const genAI = new GoogleGenerativeAI(GOOGLE_GEMINI_API_KEY!);
const client = new DataAPIClient(ASTRA_DB_APPLICATION_TOKEN);
const db = client.db(ASTRA_DB_API_ENDPOINT as string, {keyspace: ASTRA_DB_KEYSPACE});

const PROGRESS_FILE = "./src/scripts/processed_schemes_multilingual.json";

// Configuration for rate limiting
const DELAY_BETWEEN_REQUESTS = 20000; // 20 seconds between each URL
const DELAY_ON_RATE_LIMIT = 180000; // 3 minutes if rate limited
const MAX_RETRIES = 3;

// Add state-specific scheme URLs
const SchemeURLs = [
    // Central Government Schemes
    'https://pmaymis.gov.in/',
    'https://pmjay.gov.in/',
    'https://pmkisan.gov.in/',
    'https://www.mudra.org.in/',
    'https://www.india.gov.in/sukanya-samriddhi-yojana',
    'https://www.npscra.nsdl.co.in/',
    'https://pmjdy.gov.in/',
    'https://www.india.gov.in/spotlight/pradhan-mantri-jan-dhan-yojana',
    'https://wcd.nic.in/bbbp-schemes',
    'https://www.india.gov.in/my-government/schemes',
    'https://www.india.gov.in/spotlight/scholarships-students',
    'https://labour.gov.in/schemes',
    'https://mmry.brlps.in/',
    'https://pmujjwalayojana.in/janani-bal-suraksha-yojana/',
    'https://missionshakti.wcd.gov.in/',
    'https://pmmvy.wcd.gov.in/',
    'https://pmdaksh.dosje.gov.in/',
    'https://pmajay.dosje.gov.in/',
    'https://eshram.gov.in/social-security-welfare-schemes',
    'https://www.pmuy.gov.in/',
    'https://lakhpatididi.gov.in/',
    
    // Bihar State Schemes
    'https://state.bihar.gov.in/main/CitizenHome.html',
    'https://www.bihar.gov.in/Profile/Schemes.html',
];

interface StructuredScheme {
  name: string;
  nameHindi: string;
  nameBhojpuri: string;
  eligibility: string;
  eligibilityHindi: string;
  eligibilityBhojpuri: string;
  benefits: string;
  benefitsHindi: string;
  benefitsBhojpuri: string;
  category: string;
  targetAge?: string;
  targetGender?: string;
  targetIncome?: string;
  targetState?: string;
  schemeLevel: string;
  applyLink: string;
  rawContent: string;
}

const loadProcessedUrls = async (): Promise<Set<string>> => {
  try {
    const data = await fs.readFile(PROGRESS_FILE, "utf-8");
    return new Set(JSON.parse(data));
  } catch (error) {
    return new Set();
  }
};

const saveProcessedUrl = async (url: string, processedUrls: Set<string>) => {
  processedUrls.add(url);
  await fs.writeFile(PROGRESS_FILE, JSON.stringify(Array.from(processedUrls), null, 2), "utf-8");
};

const scrapePage = async (url: string): Promise<string> => {
  try {
    console.log('  Loading page...');
    const loader = new PuppeteerWebBaseLoader(url, {
      launchOptions: { headless: true },
      gotoOptions: { waitUntil: "domcontentloaded", timeout: 30000 },
      evaluate: async (page, browser) => {
        const result = await page.evaluate(() => document.body.innerHTML);
        await browser.close();
        return result;
      }
    });
    const content = (await loader.scrape())?.replace(/<[^>]*>?/gm, ' ').replace(/\s+/g, ' ') || '';
    console.log(`  Scraped ${content.length} characters`);
    return content;
  } catch (error: any) {
    console.error(`  Scraping error: ${error.message}`);
    throw error;
  }
};

const extractSchemeWithGemini = async (content: string, url: string, retryCount = 0): Promise<StructuredScheme | null> => {
  try {
    console.log(`  Extracting with Gemini (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
    
    const model = genAI.getGenerativeModel({ 
      model: "gemini-2.0-flash-exp"
    });

    const prompt = `Analyze this Indian government scheme website and extract information in THREE languages: English, Hindi, and Bhojpuri.

Content: ${content.substring(0, 8000)}
URL: ${url}

IMPORTANT INSTRUCTIONS:
1. Extract scheme details in English, Hindi, and Bhojpuri
2. Bhojpuri should be authentic Bhojpuri language (भोजपुरी), not just Hindi
3. Detect if this is a Central Government scheme or State-specific scheme
4. If state-specific, identify which state (bihar, uttar pradesh, jharkhand, etc.)
5. If central scheme, set targetState as "all"

Return ONLY a JSON object with this exact structure:
{
  "name": "Scheme name in English",
  "nameHindi": "योजना का नाम हिंदी में",
  "nameBhojpuri": "योजना के नाम भोजपुरी में",
  "eligibility": "Who can apply (age, gender, income, occupation, state)",
  "eligibilityHindi": "पात्रता हिंदी में",
  "eligibilityBhojpuri": "पात्रता भोजपुरी में",
  "benefits": "What beneficiaries get",
  "benefitsHindi": "लाभ हिंदी में",
  "benefitsBhojpuri": "लाभ भोजपुरी में",
  "category": "housing/healthcare/agriculture/entrepreneurship/education/pension/banking/women/employment",
  "targetAge": "18-60 or all ages",
  "targetGender": "male/female/all",
  "targetIncome": "income criteria or all",
  "targetState": "all for central schemes, or bihar/uttar pradesh/jharkhand/etc for state schemes",
  "schemeLevel": "central or state",
  "applyLink": "${url}"
}

BHOJPURI LANGUAGE GUIDELINES:
- Use authentic Bhojpuri vocabulary and grammar
- Example: "ई योजना के फायदा" (This scheme's benefits)
- Example: "जवन लोग आवेदन कर सकेला" (Who can apply)
- Use Devanagari script for Bhojpuri
- Common Bhojpuri words: के, सकेला, बा, बानी, रहल, जाला, आवेला, करेला, खातिर

Return ONLY the JSON object, no markdown formatting, no explanation.`;

    const result = await model.generateContent(prompt);
    const response = await result.response;
    let text = response.text();
    
    // Clean up markdown formatting
    text = text.replace(/json\n?|\n?/g, '').trim();
    
    const parsed = JSON.parse(text);
    console.log(`  ✓ Successfully extracted: ${parsed.name}`);
    return { ...parsed, rawContent: content.substring(0, 2000) };
    
  } catch (error: any) {
    if (error.message?.includes('429') || error.message?.includes('quota') || error.message?.includes('Too Many Requests')) {
      if (retryCount < MAX_RETRIES - 1) {
        console.log(`  ⚠ Rate limit hit, waiting ${DELAY_ON_RATE_LIMIT/1000} seconds before retry...`);
        await new Promise(resolve => setTimeout(resolve, DELAY_ON_RATE_LIMIT));
        return extractSchemeWithGemini(content, url, retryCount + 1);
      } else {
        console.error('  ✗ Max retries reached for rate limit');
        throw new Error('Rate limit exceeded after retries');
      }
    }
    
    console.error(`  ✗ Gemini extraction error: ${error.message}`);
    return null;
  }
};

const createEmbedding = async (text: string, retryCount = 0): Promise<number[]> => {
  try {
    console.log('  Creating embedding...');
    
    const model = genAI.getGenerativeModel({ 
      model: "text-embedding-004"
    });
    
    const result = await model.embedContent(text);
    console.log('  ✓ Embedding created');
    return result.embedding.values;
    
  } catch (error: any) {
    if (error.message?.includes('429') || error.message?.includes('quota')) {
      if (retryCount < MAX_RETRIES - 1) {
        console.log(`  ⚠ Rate limit hit, waiting ${DELAY_ON_RATE_LIMIT/1000} seconds before retry...`);
        await new Promise(resolve => setTimeout(resolve, DELAY_ON_RATE_LIMIT));
        return createEmbedding(text, retryCount + 1);
      } else {
        throw new Error('Rate limit exceeded for embedding after retries');
      }
    }
    
    console.error('  ✗ Embedding error:', error.message);
    throw error;
  }
};

const createCollection = async () => {
  try {
    await db.createCollection(ASTRA_DB_COLLECTION as string, {
      vector: { dimension: 768, metric: "dot_product" }
    });
    console.log('✓ Collection created');
  } catch (error: any) {
    if (error.message?.includes('already exists')) {
      console.log('✓ Collection already exists');
    } else {
      throw error;
    }
  }
};

const ingestMultilingualSchemes = async () => {
  console.log('Starting multilingual ingestion with state support...\n');
  console.log(`Rate limiting: ${DELAY_BETWEEN_REQUESTS/1000}s between requests, ${DELAY_ON_RATE_LIMIT/1000}s on rate limit\n`);
  
  await createCollection();
  const collection = db.collection(ASTRA_DB_COLLECTION as string);
  const processedUrls = await loadProcessedUrls();

  console.log(`Total URLs: ${SchemeURLs.length}`);
  console.log(`Already processed: ${processedUrls.size}`);
  console.log(`Remaining: ${SchemeURLs.length - processedUrls.size}\n`);

  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < SchemeURLs.length; i++) {
    const url = SchemeURLs[i];
    
    if (processedUrls.has(url)) {
      console.log(`[${i + 1}/${SchemeURLs.length}] ⊘ Skipping (already processed): ${url}`);
      continue;
    }

    console.log(`\n[${i + 1}/${SchemeURLs.length}] Processing: ${url}`);
    console.log('─'.repeat(80));

    try {
      // Step 1: Scrape
      const content = await scrapePage(url);
      
      if (content.length < 500) {
        console.log('  ⚠ Insufficient content, skipping');
        await saveProcessedUrl(url, processedUrls);
        failCount++;
        continue;
      }

      // Step 2: Extract with Gemini
      const scheme = await extractSchemeWithGemini(content, url);
      
      if (!scheme) {
        console.log('  ✗ Failed to extract, skipping');
        await saveProcessedUrl(url, processedUrls);
        failCount++;
        continue;
      }

      console.log(`  → State: ${scheme.targetState} | Level: ${scheme.schemeLevel}`);

      // Step 3: Create embedding
      const embeddingText = `${scheme.name} ${scheme.nameHindi} ${scheme.nameBhojpuri} ${scheme.eligibility} ${scheme.eligibilityHindi} ${scheme.eligibilityBhojpuri} ${scheme.benefits} ${scheme.benefitsHindi} ${scheme.benefitsBhojpuri} Category: ${scheme.category} Age: ${scheme.targetAge} Gender: ${scheme.targetGender} Income: ${scheme.targetIncome} State: ${scheme.targetState} Level: ${scheme.schemeLevel}`.trim();
      
      const vector = await createEmbedding(embeddingText);

      // Step 4: Store in database
      console.log('  Storing in database...');
      await collection.insertOne({
        ...scheme,
        $vector: vector,
        createdAt: new Date().toISOString()
      });

      console.log('  ✓ SUCCESS - Scheme ingested');
      successCount++;

      await saveProcessedUrl(url, processedUrls);
      
      // Rate limiting - wait between requests
      if (i < SchemeURLs.length - 1) {
        console.log(`  ⏱ Waiting ${DELAY_BETWEEN_REQUESTS/1000} seconds before next request...`);
        await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_REQUESTS));
      }

    } catch (error: any) {
      console.error(`  ✗ ERROR: ${error.message}`);
      failCount++;
      
      if (error.message?.includes('429') || error.message?.includes('quota') || error.message?.includes('Too Many Requests')) {
        console.log(`  ⚠ Rate limit detected - waiting ${DELAY_ON_RATE_LIMIT/1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, DELAY_ON_RATE_LIMIT));
      } else {
        // For other errors, still save as processed to avoid getting stuck
        await saveProcessedUrl(url, processedUrls);
      }
    }
  }

  console.log('\n' + '═'.repeat(80));
  console.log('INGESTION COMPLETE');
  console.log('═'.repeat(80));
  console.log(`✓ Success: ${successCount}`);
  console.log(`✗ Failed: ${failCount}`);
  console.log(`Total processed: ${processedUrls.size}/${SchemeURLs.length}`);
  console.log('═'.repeat(80));
};

// Run the ingestion
ingestMultilingualSchemes().catch(error => {
  console.error('\n❌ Fatal error:', error);
  process.exit(1);
});