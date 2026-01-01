

import { NextRequest, NextResponse } from 'next/server';
import { DataAPIClient } from '@datastax/astra-db-ts';
import { GoogleGenerativeAI } from '@google/generative-ai';

const client = new DataAPIClient(process.env.ASTRA_DB_APPLICATION_TOKEN!);
const db = client.db(process.env.ASTRA_DB_API_ENDPOINT!, {
  keyspace: process.env.ASTRA_DB_KEYSPACE
});
const genAI = new GoogleGenerativeAI(process.env.GOOGLE_GEMINI_API_KEY!);

// Extract user profile from query
async function extractUserProfile(query: string) {
  const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
  
  const prompt = `Extract user profile from this query. Return ONLY a JSON object, no markdown:
  
Query: "${query}"

{
  "age": <number or null>,
  "gender": "male" | "female" | "all",
  "language": "hi" | "en"
}

Rules:
- age: extract exact number, if not mentioned use null
- gender: "male" if query has male/man/boy/पुरुष/लड़का, "female" if female/woman/girl/महिला/लड़की, otherwise "all"
- language: "hi" if query contains Hindi/Devanagari characters, else "en"

Return ONLY JSON, no explanation.`;

  const result = await model.generateContent(prompt);
  const text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
  return JSON.parse(text);
}

export async function POST(req: NextRequest) {
  try {
    const { query, language: userLanguage } = await req.json();

    if (!query) {
      return NextResponse.json({ error: 'Query required' }, { status: 400 });
    }

    // Extract user profile first
    const profile = await extractUserProfile(query);
    const detectedLanguage = profile.language || userLanguage || 'en';

    console.log('User Profile:', profile);

    // Create embedding for semantic search
    const embeddingModel = genAI.getGenerativeModel({ 
      model: "text-embedding-004"
    });
    
    const embeddingResult = await embeddingModel.embedContent(query);
    const queryVector = embeddingResult.embedding.values;

    if (!queryVector) {
      throw new Error('Failed to create embedding');
    }

    // Build metadata filter based on your ingest-scheme.ts structure
    const filter: any = {};
    
    // Gender filter: Match targetGender field from your ingestion
    if (profile.gender && profile.gender !== 'all') {
      filter.$or = [
        { targetGender: profile.gender },
        { targetGender: 'all' },
        { targetGender: { $exists: false } }
      ];
    }

    console.log('Filter:', JSON.stringify(filter));

    // Vector search with metadata filter
    const collection = db.collection(process.env.ASTRA_DB_COLLECTION!);
    const cursor = collection.find(filter, {
      sort: { $vector: queryVector },
      limit: 20,
      includeSimilarity: true
    });

    const results = await cursor.toArray();
    console.log(`Found ${results.length} schemes after vector search`);

    // Additional client-side filtering for age
    let filtered = results.filter((r: any) => {
      // Similarity threshold
      if (r.$similarity <= 0.5) {
        return false;
      }

      // Age filtering based on targetAge field from your ingestion
      if (profile.age && r.targetAge) {
        const targetAge = r.targetAge.toLowerCase();
        
        // Skip if it says "all ages"
        if (targetAge === 'all ages' || targetAge === 'all') {
          return true;
        }

        // Parse age range like "18-60"
        const ageMatch = targetAge.match(/(\d+)-(\d+)/);
        if (ageMatch) {
          const minAge = parseInt(ageMatch[1]);
          const maxAge = parseInt(ageMatch[2]);
          
          if (profile.age < minAge || profile.age > maxAge) {
            console.log(`Filtered out ${r.name}: age ${profile.age} not in ${minAge}-${maxAge}`);
            return false;
          }
        }
      }

      return true;
    });

    console.log(`After age/similarity filter: ${filtered.length} schemes`);

    if (filtered.length === 0) {
      const noResultMsg = detectedLanguage === 'hi' 
        ? 'क्षमा करें, आपकी प्रोफाइल से मेल खाने वाली कोई योजना नहीं मिली।' 
        : 'Sorry, no schemes found matching your profile.';
        
      return NextResponse.json({ 
        schemes: [{
          id: 'no-results',
          name: noResultMsg,
          eligibility: '',
          benefits: '',
          applyLink: ''
        }]
      });
    }

    // Format results in correct language
    const schemes = filtered.slice(0, 6).map((r: any) => ({
      id: r._id || 'scheme-' + Math.random(),
      name: detectedLanguage === 'hi' ? (r.nameHindi || r.name) : r.name,
      eligibility: detectedLanguage === 'hi' ? (r.eligibilityHindi || r.eligibility) : r.eligibility,
      benefits: detectedLanguage === 'hi' ? (r.benefitsHindi || r.benefits) : r.benefits,
      applyLink: r.applyLink,
      category: r.category,
      // Debug info
      targetGender: r.targetGender,
      targetAge: r.targetAge,
      similarity: r.$similarity?.toFixed(3)
    }));

    console.log('Returning schemes:', schemes.map(s => ({
      name: s.name, 
      gender: s.targetGender, 
      age: s.targetAge
    })));

    return NextResponse.json({ 
      schemes,
      language: detectedLanguage
    });

  } catch (error: any) {
    console.error('Query error:', error);
    return NextResponse.json({ 
      error: error.message || 'Internal error' 
    }, { status: 500 });
  }
}