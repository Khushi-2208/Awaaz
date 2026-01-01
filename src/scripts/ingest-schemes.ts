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

// Initialize with correct SDK
const genAI = new GoogleGenerativeAI(GOOGLE_GEMINI_API_KEY!);
const client = new DataAPIClient(ASTRA_DB_APPLICATION_TOKEN);
const db = client.db(ASTRA_DB_API_ENDPOINT as string, {keyspace: ASTRA_DB_KEYSPACE});

const PROGRESS_FILE = "./src/scripts/processed_schemes_structured.json";

const SchemeURLs = [
    'https://pmaymis.gov.in/',
    'https://pmjay.gov.in/',
    'https://pmkisan.gov.in/',
    'https://www.mudra.org.in/',
    'https://www.india.gov.in/sukanya-samriddhi-yojana',
    'https://www.npscra.nsdl.co.in/',
    'https://pmjdy.gov.in/',
    'https://www.india.gov.in/spotlight/pradhan-mantri-jan-dhan-yojana',
    'https://www.standupmitra.in/',
    'https://wcd.nic.in/bbbp-schemes',
    'https://www.myscheme.gov.in/',
    'https://www.india.gov.in/my-government/schemes',
    'https://www.india.gov.in/spotlight/scholarships-students',
    'https://labour.gov.in/schemes',
    'https://msme.gov.in/schemes-programmes',
    'https://mmry.brlps.in/',
    'https://pmujjwalayojana.in/janani-bal-suraksha-yojana/',
    'https://missionshakti.wcd.gov.in/',
    'https://pmmvy.wcd.gov.in/'

];

interface StructuredScheme {
  name: string;
  nameHindi: string;
  eligibility: string;
  eligibilityHindi: string;
  benefits: string;
  benefitsHindi: string;
  category: string;
  targetAge?: string;
  targetGender?: string;
  targetIncome?: string;
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
  const loader = new PuppeteerWebBaseLoader(url, {
    launchOptions: { headless: true },
    gotoOptions: { waitUntil: "domcontentloaded" },
    evaluate: async (page, browser) => {
      const result = await page.evaluate(() => document.body.innerHTML);
      await browser.close();
      return result;
    }
  });
  return (await loader.scrape())?.replace(/<[^>]*>?/gm, ' ').replace(/\s+/g, ' ') || '';
};

const extractSchemeWithGemini = async (content: string, url: string): Promise<StructuredScheme | null> => {
  try {
    // Use gemini-1.5-pro-latest which should be available
    const model = genAI.getGenerativeModel({ 
      model: "gemini-2.5-flash"
    });

    const prompt = `Analyze this Indian government scheme website and extract information.

Content: ${content.substring(0, 6000)}
URL: ${url}

Extract and return ONLY a JSON object with this exact structure:
{
  "name": "Scheme name in English",
  "nameHindi": "योजना का नाम हिंदी में",
  "eligibility": "Who can apply (age, gender, income, occupation)",
  "eligibilityHindi": "पात्रता हिंदी में",
  "benefits": "What beneficiaries get",
  "benefitsHindi": "लाभ हिंदी में",
  "category": "housing/healthcare/agriculture/entrepreneurship/education/pension/banking/women/employment",
  "targetAge": "18-60 or all ages",
  "targetGender": "male/female/all",
  "targetIncome": "income criteria or all",
  "applyLink": "${url}"
}

Return ONLY the JSON object, no markdown formatting, no explanation.`;

    const result = await model.generateContent(prompt);
    const response = await result.response;
    let text = response.text();
    
    // Clean up markdown formatting
    text = text.replace(/```json\n?|\n?```/g, '').trim();
    
    const parsed = JSON.parse(text);
    return { ...parsed, rawContent: content.substring(0, 2000) };
    
  } catch (error: any) {
    console.error('Gemini extraction error:', error.message);
    return null;
  }
};

const createEmbedding = async (text: string): Promise<number[]> => {
  try {
    // Use models.embedContent for embeddings with text-embedding-004
    const model = genAI.getGenerativeModel({ 
      model: "text-embedding-004"
    });
    
    const result = await model.embedContent(text);
    return result.embedding.values;
    
  } catch (error: any) {
    console.error('Embedding error:', error.message);
    throw error;
  }
};

const createCollection = async () => {
  try {
    await db.createCollection(ASTRA_DB_COLLECTION as string, {
      vector: { dimension: 768, metric: "dot_product" }
    });
    console.log('Collection created');
  } catch (error: any) {
    if (error.message?.includes('already exists')) {
      console.log('Collection exists');
    } else {
      throw error;
    }
  }
};

const ingestStructuredSchemes = async () => {
  await createCollection();
  const collection = db.collection(ASTRA_DB_COLLECTION as string);
  const processedUrls = await loadProcessedUrls();

  console.log('Starting structured ingestion...\n');

  for (const url of SchemeURLs) {
    if (processedUrls.has(url)) {
      console.log(`Skipping: ${url}`);
      continue;
    }

    console.log(`\nProcessing: ${url}`);

    try {
      // Scrape
      console.log('Scraping...');
      const content = await scrapePage(url);
      
      if (content.length < 500) {
        console.log('Insufficient content');
        await saveProcessedUrl(url, processedUrls);
        continue;
      }

      // Extract with Gemini
      console.log('Extracting scheme details...');
      const scheme = await extractSchemeWithGemini(content, url);
      
      if (!scheme) {
        console.log('Failed to extract');
        await saveProcessedUrl(url, processedUrls);
        continue;
      }

      console.log(`Extracted: ${scheme.name}`);

      // Create embedding
      console.log('Creating embedding...');
      const embeddingText = `${scheme.name} ${scheme.nameHindi} ${scheme.eligibility} ${scheme.eligibilityHindi} ${scheme.benefits} ${scheme.benefitsHindi} Category: ${scheme.category} Age: ${scheme.targetAge} Gender: ${scheme.targetGender} Income: ${scheme.targetIncome}`.trim();
      
      const vector = await createEmbedding(embeddingText);

      // Store
      console.log('Storing in database...');
      await collection.insertOne({
        ...scheme,
        $vector: vector,
        createdAt: new Date().toISOString()
      });

      console.log('SUCCESS');

      await saveProcessedUrl(url, processedUrls);
      
      // Rate limiting - 3 seconds between requests
      await new Promise(resolve => setTimeout(resolve, 3000));

    } catch (error: any) {
      console.error(`Error: ${error.message}`);
      
      if (error.message?.includes('429') || error.message?.includes('quota')) {
        console.log('Rate limit - waiting 60 seconds');
        await new Promise(resolve => setTimeout(resolve, 60000));
      }
    }
  }

  console.log('\nIngestion complete!');
};

ingestStructuredSchemes();