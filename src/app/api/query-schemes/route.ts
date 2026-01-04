import { NextRequest, NextResponse } from 'next/server';
import { DataAPIClient } from '@datastax/astra-db-ts';
import { GoogleGenerativeAI } from '@google/generative-ai';

const client = new DataAPIClient(process.env.ASTRA_DB_APPLICATION_TOKEN!);
const db = client.db(process.env.ASTRA_DB_API_ENDPOINT!, {
  keyspace: process.env.ASTRA_DB_KEYSPACE
});
const genAI = new GoogleGenerativeAI(process.env.GOOGLE_GEMINI_API_KEY!);

// Helper function to check language
function detectLanguageType(text: string): string {
  const hindiPattern = /[\u0900-\u097F]/;
  const bhojpuriKeywords = ['के', 'सकेला', 'बा', 'बानी', 'रहल', 'जाला', 'आवेला', 'करेला', 'खातिर', 'रउआ', 'हमार', 'हमरा'];
  
  if (!hindiPattern.test(text)) {
    return 'en';
  }
  
  const hasBhojpuri = bhojpuriKeywords.some(word => text.includes(word));
  if (hasBhojpuri) {
    return 'bho';
  }
  
  return 'hi';
}

function containsHindi(text: string): boolean {
  if (!text) return false;
  return /[\u0900-\u097F]/.test(text);
}

function isEnglish(text: string): boolean {
  if (!text) return true;
  return !containsHindi(text);
}

// Extract user profile including state
async function extractUserProfile(query: string) {
  const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
  
  const prompt = `Extract user profile from this query. Return ONLY a JSON object, no markdown:
  
Query: "${query}"

{
  "age": <number or null>,
  "gender": "male" | "female" | "all",
  "language": "hi" | "en" | "bho",
  "state": "bihar" | "uttar pradesh" | "jharkhand" | "all" | etc
}

Rules:
- age: extract exact number, if not mentioned use null
- gender: "male" if query has male/man/boy/पुरुष/लड़का/मर्द, "female" if female/woman/girl/महिला/लड़की/औरत, otherwise "all"
- language: 
  * "bho" if query contains Bhojpuri words like: के, सकेला, बा, बानी, रहल, जाला, आवेला, करेला, खातिर, रउआ, हमार, हमरा
  * "hi" if query contains Hindi/Devanagari characters but not Bhojpuri
  * "en" otherwise
- state: extract state name if mentioned (bihar/बिहार, uttar pradesh/उत्तर प्रदेश, jharkhand/झारखंड, etc). If not mentioned, use "all"

Common Bhojpuri phrases:
- "हम बिहार से बानी" = I am from Bihar
- "का योजना बा" = What schemes are there
- "हमके चाही" = I need
- "रउआ बतावा" = Please tell me

Return ONLY JSON, no explanation.`;

  const result = await model.generateContent(prompt);
  const text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
  return JSON.parse(text);
}

// Batch translation function
async function batchTranslateSchemes(schemes: any[], targetLanguage: 'hi' | 'bho'): Promise<any[]> {
  if (schemes.length === 0) return schemes;
  
  const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
  
  const languageName = targetLanguage === 'bho' ? 'Bhojpuri (भोजपुरी)' : 'Hindi (हिंदी)';
  const languageInstructions = targetLanguage === 'bho' 
    ? `AUTHENTIC BHOJPURI GUIDELINES:
- Use these Bhojpuri words: के, सकेला, बा, बानी, रहल, जाला, आवेला, करेला, खातिर, रउआ, हमार, हमरा
- Examples:
  * "ई योजना के फायदा बा" (This scheme's benefit)
  * "जवन लोग आवेदन कर सकेला" (Who can apply)  
  * "रउआ खातिर ई योजना बा" (This scheme is for you)
  * "पइसा मिलेला" (Money will be given)
- Use Bhojpuri grammar and natural speech patterns
- Keep government terms clear`
    : `STANDARD HINDI GUIDELINES:
- Use formal Hindi (मानक हिंदी)
- Clear and simple language
- Keep government terminology accurate`;

  const schemesJson = schemes.map((s, idx) => ({
    id: idx,
    name: s.name,
    eligibility: s.eligibility,
    benefits: s.benefits
  }));
  
  const prompt = `Translate ALL these government schemes to ${languageName}. Return ONLY a JSON array:

Schemes to translate:
${JSON.stringify(schemesJson, null, 2)}

${languageInstructions}

Return format (MUST be valid JSON array):
[
  {
    "id": 0,
    "name": "translated name in ${languageName}",
    "eligibility": "translated eligibility in ${languageName}",
    "benefits": "translated benefits in ${languageName}"
  },
  {
    "id": 1,
    "name": "...",
    "eligibility": "...",
    "benefits": "..."
  }
]

IMPORTANT: 
- Return ONLY the JSON array, no markdown, no explanation
- Translate ALL schemes
- Keep the same ID numbers`;

  try {
    const result = await model.generateContent(prompt);
    let text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
    
    // Additional cleanup
    if (text.startsWith('[') && text.includes(']')) {
      const startIdx = text.indexOf('[');
      const endIdx = text.lastIndexOf(']') + 1;
      text = text.substring(startIdx, endIdx);
    }
    
    const translations = JSON.parse(text);
    
    // Merge translations back
    return schemes.map((scheme, idx) => {
      const translation = translations.find((t: any) => t.id === idx);
      if (translation) {
        return {
          ...scheme,
          name: translation.name,
          eligibility: translation.eligibility,
          benefits: translation.benefits
        };
      }
      return scheme;
    });
  } catch (error) {
    console.error(`Batch translation error for ${targetLanguage}:`, error);
    return schemes; // Return original if translation fails
  }
}

export async function POST(req: NextRequest) {
  try {
    const { query, language: userLanguage } = await req.json();

    if (!query) {
      return NextResponse.json({ error: 'Query required' }, { status: 400 });
    }

    // Extract user profile with state
    const profile = await extractUserProfile(query);
    const detectedLanguage = profile.language || userLanguage || 'en';
    const userState = profile.state?.toLowerCase() || 'all';

    console.log('User Profile:', profile);
    console.log('Detected Language:', detectedLanguage);
    console.log('User State:', userState);

    // Create embedding for semantic search
    const embeddingModel = genAI.getGenerativeModel({ 
      model: "text-embedding-004"
    });
    
    const embeddingResult = await embeddingModel.embedContent(query);
    const queryVector = embeddingResult.embedding.values;

    if (!queryVector) {
      throw new Error('Failed to create embedding');
    }

    // Build metadata filter with state support
    const filter: any = {};
    
    // Gender filter
    if (profile.gender && profile.gender !== 'all') {
      filter.$or = [
        { targetGender: profile.gender },
        { targetGender: 'all' },
        { targetGender: { $exists: false } }
      ];
    }

    // State filter
    if (userState !== 'all') {
      const stateFilter = {
        $or: [
          { targetState: 'all' },
          { targetState: userState },
          { targetState: { $exists: false } }
        ]
      };
      
      if (filter.$or) {
        filter.$and = [
          { $or: filter.$or },
          stateFilter
        ];
        delete filter.$or;
      } else {
        filter.$or = stateFilter.$or;
      }
    }

    console.log('Filter:', JSON.stringify(filter, null, 2));

    // Vector search
    const collection = db.collection(process.env.ASTRA_DB_COLLECTION!);
    const cursor = collection.find(filter, {
      sort: { $vector: queryVector },
      limit: 25,
      includeSimilarity: true
    });

    const results = await cursor.toArray();
    console.log(`Found ${results.length} schemes after vector search`);

    // Age filtering
    let filtered = results.filter((r: any) => {
      if (r.$similarity <= 0.5) {
        return false;
      }

      if (profile.age && r.targetAge) {
        const targetAge = r.targetAge.toLowerCase();
        
        if (targetAge === 'all ages' || targetAge === 'all') {
          return true;
        }

        const ageMatch = targetAge.match(/(\d+)-(\d+)/);
        if (ageMatch) {
          const minAge = parseInt(ageMatch[1]);
          const maxAge = parseInt(ageMatch[2]);
          
          if (profile.age < minAge || profile.age > maxAge) {
            return false;
          }
        }
      }

      return true;
    });

    console.log(`After filtering: ${filtered.length} schemes`);

    if (filtered.length === 0) {
      let noResultMsg = '';
      if (detectedLanguage === 'bho') {
        noResultMsg = 'माफ करीं, रउआ खातिर कवनो योजना ना मिलल।';
      } else if (detectedLanguage === 'hi') {
        noResultMsg = 'क्षमा करें, आपकी प्रोफाइल से मेल खाने वाली कोई योजना नहीं मिली।';
      } else {
        noResultMsg = 'Sorry, no schemes found matching your profile.';
      }
        
      return NextResponse.json({ 
        schemes: [{
          id: 'no-results',
          name: noResultMsg,
          eligibility: '',
          benefits: '',
          applyLink: ''
        }],
        language: detectedLanguage
      });
    }

    // STEP 1: Get top 6 schemes and format them in ENGLISH first
    let schemes: any[] = filtered.slice(0, 6).map((r: any) => ({
      id: r._id || 'scheme-' + Math.random(),
      name: r.name, // Always start with English
      eligibility: r.eligibility,
      benefits: r.benefits,
      applyLink: r.applyLink,
      category: r.category,
      targetGender: r.targetGender,
      targetAge: r.targetAge,
      targetState: r.targetState || 'all',
      schemeLevel: r.schemeLevel || 'central',
      similarity: r.$similarity?.toFixed(3),
      // Store original language versions for fallback
      nameHindi: r.nameHindi,
      nameBhojpuri: r.nameBhojpuri,
      eligibilityHindi: r.eligibilityHindi,
      eligibilityBhojpuri: r.eligibilityBhojpuri,
      benefitsHindi: r.benefitsHindi,
      benefitsBhojpuri: r.benefitsBhojpuri
    }));

    // STEP 2: If language is Hindi or Bhojpuri, check database first, then translate ALL at once
    if (detectedLanguage === 'hi') {
      console.log('Processing Hindi translation...');
      
      // First, use existing Hindi translations from database
      schemes = schemes.map(s => {
        if (s.nameHindi && containsHindi(s.nameHindi)) {
          return {
            ...s,
            name: s.nameHindi,
            eligibility: s.eligibilityHindi || s.eligibility,
            benefits: s.benefitsHindi || s.benefits
          };
        }
        return s;
      });
      
      // Find schemes that still need translation (no Hindi in database)
      const needsTranslation = schemes.filter(s => isEnglish(s.name));
      
      if (needsTranslation.length > 0) {
        console.log(`Translating ${needsTranslation.length} schemes to Hindi...`);
        const translated = await batchTranslateSchemes(needsTranslation, 'hi');
        
        // Update the schemes array
        schemes = schemes.map(s => {
          if (isEnglish(s.name)) {
            const translatedScheme = translated.find(t => t.id === s.id);
            return translatedScheme || s;
          }
          return s;
        });
      }
    } else if (detectedLanguage === 'bho') {
      console.log('Processing Bhojpuri translation...');
      
      // First, use existing Bhojpuri translations from database
      schemes = schemes.map(s => {
        if (s.nameBhojpuri && containsHindi(s.nameBhojpuri)) {
          return {
            ...s,
            name: s.nameBhojpuri,
            eligibility: s.eligibilityBhojpuri || s.eligibility,
            benefits: s.benefitsBhojpuri || s.benefits
          };
        }
        return s;
      });
      
      // Find schemes that still need translation (no Bhojpuri in database)
      const needsTranslation = schemes.filter(s => isEnglish(s.name));
      
      if (needsTranslation.length > 0) {
        console.log(`Translating ${needsTranslation.length} schemes to Bhojpuri...`);
        const translated = await batchTranslateSchemes(needsTranslation, 'bho');
        
        // Update the schemes array
        schemes = schemes.map(s => {
          if (isEnglish(s.name)) {
            const translatedScheme = translated.find(t => t.id === s.id);
            return translatedScheme || s;
          }
          return s;
        });
      }
    }

    // STEP 3: Clean up - remove database fields from response
    schemes = schemes.map(({ 
      nameHindi, 
      nameBhojpuri, 
      eligibilityHindi, 
      eligibilityBhojpuri, 
      benefitsHindi, 
      benefitsBhojpuri, 
      ...rest 
    }) => rest);

    console.log('Final schemes:', schemes.map(s => ({
      name: s.name.substring(0, 60),
      isTranslated: containsHindi(s.name),
      language: detectedLanguage
    })));

    return NextResponse.json({ 
      schemes,
      language: detectedLanguage,
      userState: userState
    });

  } catch (error: any) {
    console.error('Query error:', error);
    return NextResponse.json({ 
      error: error.message || 'Internal error' 
    }, { status: 500 });
  }}