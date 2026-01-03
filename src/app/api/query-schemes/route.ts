

// // import { NextRequest, NextResponse } from 'next/server';
// // import { DataAPIClient } from '@datastax/astra-db-ts';
// // import { GoogleGenerativeAI } from '@google/generative-ai';

// // const client = new DataAPIClient(process.env.ASTRA_DB_APPLICATION_TOKEN!);
// // const db = client.db(process.env.ASTRA_DB_API_ENDPOINT!, {
// //   keyspace: process.env.ASTRA_DB_KEYSPACE
// // });
// // const genAI = new GoogleGenerativeAI(process.env.GOOGLE_GEMINI_API_KEY!);

// // // Extract user profile from query
// // async function extractUserProfile(query: string) {
// //   const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
  
// //   const prompt = `Extract user profile from this query. Return ONLY a JSON object, no markdown:
  
// // Query: "${query}"

// // {
// //   "age": <number or null>,
// //   "gender": "male" | "female" | "all",
// //   "language": "hi" | "en"
// // }

// // Rules:
// // - age: extract exact number, if not mentioned use null
// // - gender: "male" if query has male/man/boy/पुरुष/लड़का, "female" if female/woman/girl/महिला/लड़की, otherwise "all"
// // - language: "hi" if query contains Hindi/Devanagari characters, else "en"

// // Return ONLY JSON, no explanation.`;

// //   const result = await model.generateContent(prompt);
// //   const text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
// //   return JSON.parse(text);
// // }

// // export async function POST(req: NextRequest) {
// //   try {
// //     const { query, language: userLanguage } = await req.json();

// //     if (!query) {
// //       return NextResponse.json({ error: 'Query required' }, { status: 400 });
// //     }

// //     // Extract user profile first
// //     const profile = await extractUserProfile(query);
// //     const detectedLanguage = profile.language || userLanguage || 'en';

// //     console.log('User Profile:', profile);

// //     // Create embedding for semantic search
// //     const embeddingModel = genAI.getGenerativeModel({ 
// //       model: "text-embedding-004"
// //     });
    
// //     const embeddingResult = await embeddingModel.embedContent(query);
// //     const queryVector = embeddingResult.embedding.values;

// //     if (!queryVector) {
// //       throw new Error('Failed to create embedding');
// //     }

// //     // Build metadata filter based on your ingest-scheme.ts structure
// //     const filter: any = {};
    
// //     // Gender filter: Match targetGender field from your ingestion
// //     if (profile.gender && profile.gender !== 'all') {
// //       filter.$or = [
// //         { targetGender: profile.gender },
// //         { targetGender: 'all' },
// //         { targetGender: { $exists: false } }
// //       ];
// //     }

// //     console.log('Filter:', JSON.stringify(filter));

// //     // Vector search with metadata filter
// //     const collection = db.collection(process.env.ASTRA_DB_COLLECTION!);
// //     const cursor = collection.find(filter, {
// //       sort: { $vector: queryVector },
// //       limit: 20,
// //       includeSimilarity: true
// //     });

// //     const results = await cursor.toArray();
// //     console.log(`Found ${results.length} schemes after vector search`);

// //     // Additional client-side filtering for age
// //     let filtered = results.filter((r: any) => {
// //       // Similarity threshold
// //       if (r.$similarity <= 0.5) {
// //         return false;
// //       }

// //       // Age filtering based on targetAge field from your ingestion
// //       if (profile.age && r.targetAge) {
// //         const targetAge = r.targetAge.toLowerCase();
        
// //         // Skip if it says "all ages"
// //         if (targetAge === 'all ages' || targetAge === 'all') {
// //           return true;
// //         }

// //         // Parse age range like "18-60"
// //         const ageMatch = targetAge.match(/(\d+)-(\d+)/);
// //         if (ageMatch) {
// //           const minAge = parseInt(ageMatch[1]);
// //           const maxAge = parseInt(ageMatch[2]);
          
// //           if (profile.age < minAge || profile.age > maxAge) {
// //             console.log(`Filtered out ${r.name}: age ${profile.age} not in ${minAge}-${maxAge}`);
// //             return false;
// //           }
// //         }
// //       }

// //       return true;
// //     });

// //     console.log(`After age/similarity filter: ${filtered.length} schemes`);

// //     if (filtered.length === 0) {
// //       const noResultMsg = detectedLanguage === 'hi' 
// //         ? 'क्षमा करें, आपकी प्रोफाइल से मेल खाने वाली कोई योजना नहीं मिली।' 
// //         : 'Sorry, no schemes found matching your profile.';
        
// //       return NextResponse.json({ 
// //         schemes: [{
// //           id: 'no-results',
// //           name: noResultMsg,
// //           eligibility: '',
// //           benefits: '',
// //           applyLink: ''
// //         }]
// //       });
// //     }

// //     // Format results in correct language
// //     const schemes = filtered.slice(0, 6).map((r: any) => ({
// //       id: r._id || 'scheme-' + Math.random(),
// //       name: detectedLanguage === 'hi' ? (r.nameHindi || r.name) : r.name,
// //       eligibility: detectedLanguage === 'hi' ? (r.eligibilityHindi || r.eligibility) : r.eligibility,
// //       benefits: detectedLanguage === 'hi' ? (r.benefitsHindi || r.benefits) : r.benefits,
// //       applyLink: r.applyLink,
// //       category: r.category,
// //       // Debug info
// //       targetGender: r.targetGender,
// //       targetAge: r.targetAge,
// //       similarity: r.$similarity?.toFixed(3)
// //     }));

// //     console.log('Returning schemes:', schemes.map(s => ({
// //       name: s.name, 
// //       gender: s.targetGender, 
// //       age: s.targetAge
// //     })));

// //     return NextResponse.json({ 
// //       schemes,
// //       language: detectedLanguage
// //     });

// //   } catch (error: any) {
// //     console.error('Query error:', error);
// //     return NextResponse.json({ 
// //       error: error.message || 'Internal error' 
// //     }, { status: 500 });
// //   }
// // }

// import { NextRequest, NextResponse } from 'next/server';
// import { DataAPIClient } from '@datastax/astra-db-ts';
// import { GoogleGenerativeAI } from '@google/generative-ai';

// const client = new DataAPIClient(process.env.ASTRA_DB_APPLICATION_TOKEN!);
// const db = client.db(process.env.ASTRA_DB_API_ENDPOINT!, {
//   keyspace: process.env.ASTRA_DB_KEYSPACE
// });
// const genAI = new GoogleGenerativeAI(process.env.GOOGLE_GEMINI_API_KEY!);

// // Helper function to check if text contains Hindi characters
// function containsHindi(text: string): boolean {
//   return /[\u0900-\u097F]/.test(text);
// }

// // Helper function to get text in the correct language
// function getLocalizedText(hindiText: string | undefined, englishText: string, targetLanguage: string): string {
//   if (targetLanguage === 'hi') {
//     // If Hindi field exists and actually contains Hindi characters, use it
//     if (hindiText && hindiText.trim() && containsHindi(hindiText)) {
//       return hindiText;
//     }
//     // Otherwise, we need to translate the English text
//     return englishText; // Will be translated below
//   }
//   return englishText;
// }

// // Extract user profile from query
// async function extractUserProfile(query: string) {
//   const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
  
//   const prompt = `Extract user profile from this query. Return ONLY a JSON object, no markdown:
  
// Query: "${query}"

// {
//   "age": <number or null>,
//   "gender": "male" | "female" | "all",
//   "language": "hi" | "en"
// }

// Rules:
// - age: extract exact number, if not mentioned use null
// - gender: "male" if query has male/man/boy/पुरुष/लड़का, "female" if female/woman/girl/महिला/लड़की, otherwise "all"
// - language: "hi" if query contains Hindi/Devanagari characters, else "en"

// Return ONLY JSON, no explanation.`;

//   const result = await model.generateContent(prompt);
//   const text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
//   return JSON.parse(text);
// }

// // Translate scheme to target language
// async function translateScheme(scheme: any, targetLanguage: string) {
//   if (targetLanguage === 'en') {
//     return scheme; // Already in English
//   }

//   // Check if translation is needed
//   const needsTranslation = 
//     !containsHindi(scheme.name) || 
//     !containsHindi(scheme.eligibility) || 
//     !containsHindi(scheme.benefits);

//   if (!needsTranslation) {
//     return scheme; // Already has proper Hindi
//   }

//   const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
  
//   const prompt = `Translate this government scheme information to Hindi. Return ONLY a JSON object:

// {
//   "name": "${scheme.name}",
//   "eligibility": "${scheme.eligibility}",
//   "benefits": "${scheme.benefits}"
// }

// Translate to:
// {
//   "name": "योजना का नाम हिंदी में",
//   "eligibility": "पात्रता हिंदी में",
//   "benefits": "लाभ हिंदी में"
// }

// IMPORTANT: Return ONLY the JSON object with Hindi translations, no markdown, no explanation.`;

//   try {
//     const result = await model.generateContent(prompt);
//     const text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
//     const translated = JSON.parse(text);
    
//     return {
//       ...scheme,
//       name: translated.name,
//       eligibility: translated.eligibility,
//       benefits: translated.benefits
//     };
//   } catch (error) {
//     console.error('Translation error:', error);
//     return scheme; // Return original if translation fails
//   }
// }

// export async function POST(req: NextRequest) {
//   try {
//     const { query, language: userLanguage } = await req.json();

//     if (!query) {
//       return NextResponse.json({ error: 'Query required' }, { status: 400 });
//     }

//     // Extract user profile first
//     const profile = await extractUserProfile(query);
//     const detectedLanguage = profile.language || userLanguage || 'en';

//     console.log('User Profile:', profile);
//     console.log('Detected Language:', detectedLanguage);

//     // Create embedding for semantic search
//     const embeddingModel = genAI.getGenerativeModel({ 
//       model: "text-embedding-004"
//     });
    
//     const embeddingResult = await embeddingModel.embedContent(query);
//     const queryVector = embeddingResult.embedding.values;

//     if (!queryVector) {
//       throw new Error('Failed to create embedding');
//     }

//     // Build metadata filter
//     const filter: any = {};
    
//     // Gender filter
//     if (profile.gender && profile.gender !== 'all') {
//       filter.$or = [
//         { targetGender: profile.gender },
//         { targetGender: 'all' },
//         { targetGender: { $exists: false } }
//       ];
//     }

//     console.log('Filter:', JSON.stringify(filter));

//     // Vector search with metadata filter
//     const collection = db.collection(process.env.ASTRA_DB_COLLECTION!);
//     const cursor = collection.find(filter, {
//       sort: { $vector: queryVector },
//       limit: 20,
//       includeSimilarity: true
//     });

//     const results = await cursor.toArray();
//     console.log(`Found ${results.length} schemes after vector search`);

//     // Additional client-side filtering for age
//     let filtered = results.filter((r: any) => {
//       // Similarity threshold
//       if (r.$similarity <= 0.5) {
//         return false;
//       }

//       // Age filtering
//       if (profile.age && r.targetAge) {
//         const targetAge = r.targetAge.toLowerCase();
        
//         if (targetAge === 'all ages' || targetAge === 'all') {
//           return true;
//         }

//         const ageMatch = targetAge.match(/(\d+)-(\d+)/);
//         if (ageMatch) {
//           const minAge = parseInt(ageMatch[1]);
//           const maxAge = parseInt(ageMatch[2]);
          
//           if (profile.age < minAge || profile.age > maxAge) {
//             console.log(`Filtered out ${r.name}: age ${profile.age} not in ${minAge}-${maxAge}`);
//             return false;
//           }
//         }
//       }

//       return true;
//     });

//     console.log(`After age/similarity filter: ${filtered.length} schemes`);

//     if (filtered.length === 0) {
//       const noResultMsg = detectedLanguage === 'hi' 
//         ? 'क्षमा करें, आपकी प्रोफाइल से मेल खाने वाली कोई योजना नहीं मिली।' 
//         : 'Sorry, no schemes found matching your profile.';
        
//       return NextResponse.json({ 
//         schemes: [{
//           id: 'no-results',
//           name: noResultMsg,
//           eligibility: '',
//           benefits: '',
//           applyLink: ''
//         }]
//       });
//     }

//     // Format results with proper language handling
//     let schemes = filtered.slice(0, 6).map((r: any) => {
//       let name, eligibility, benefits;
      
//       if (detectedLanguage === 'hi') {
//         // Use Hindi fields if they exist and contain Hindi, otherwise use English
//         name = (r.nameHindi && containsHindi(r.nameHindi)) ? r.nameHindi : r.name;
//         eligibility = (r.eligibilityHindi && containsHindi(r.eligibilityHindi)) ? r.eligibilityHindi : r.eligibility;
//         benefits = (r.benefitsHindi && containsHindi(r.benefitsHindi)) ? r.benefitsHindi : r.benefits;
//       } else {
//         name = r.name;
//         eligibility = r.eligibility;
//         benefits = r.benefits;
//       }

//       return {
//         id: r._id || 'scheme-' + Math.random(),
//         name,
//         eligibility,
//         benefits,
//         applyLink: r.applyLink,
//         category: r.category,
//         targetGender: r.targetGender,
//         targetAge: r.targetAge,
//         similarity: r.$similarity?.toFixed(3),
//         needsTranslation: detectedLanguage === 'hi' && !containsHindi(name)
//       };
//     });

//     // Translate any schemes that don't have proper Hindi content
//     if (detectedLanguage === 'hi') {
//       const schemesToTranslate = schemes.filter(s => s.needsTranslation);
      
//       if (schemesToTranslate.length > 0) {
//         console.log(`Translating ${schemesToTranslate.length} schemes to Hindi...`);
        
//         // Translate in parallel (but be mindful of rate limits)
//         const translationPromises = schemesToTranslate.map(scheme => 
//           translateScheme(scheme, 'hi')
//         );
        
//         const translatedSchemes = await Promise.all(translationPromises);
        
//         // Replace the schemes that were translated
//         schemes = schemes.map(s => {
//           if (s.needsTranslation) {
//             const translated = translatedSchemes.find(t => t.id === s.id);
//             if (translated) {
//               const { needsTranslation, ...rest } = translated;
//               return rest;
//             }
//           }
//           const { needsTranslation, ...rest } = s;
//           return rest;
//         });
//       }
//     }

//     console.log('Returning schemes:', schemes.map(s => ({
//       name: s.name.substring(0, 50), 
//       hasHindi: containsHindi(s.name),
//       gender: s.targetGender, 
//       age: s.targetAge
//     })));

//     return NextResponse.json({ 
//       schemes,
//       language: detectedLanguage
//     });

//   } catch (error: any) {
//     console.error('Query error:', error);
//     return NextResponse.json({ 
//       error: error.message || 'Internal error' 
//     }, { status: 500 });
//   }
// }

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
  const bhojpuriKeywords = ['के', 'सकेला', 'बा', 'बानी', 'रहल', 'जाला', 'आवेला', 'करेला'];
  
  if (!hindiPattern.test(text)) {
    return 'en';
  }
  
  // Check for Bhojpuri-specific words
  const hasBhojpuri = bhojpuriKeywords.some(word => text.includes(word));
  if (hasBhojpuri) {
    return 'bho';
  }
  
  return 'hi';
}

function containsHindi(text: string): boolean {
  return /[\u0900-\u097F]/.test(text);
}

// Extract user profile including state
async function extractUserProfile(query: string) {
  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash-exp" });
  
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
  * "bho" if query contains Bhojpuri words like: के, सकेला, बा, बानी, रहल, जाला, आवेला, करेला
  * "hi" if query contains Hindi/Devanagari characters but not Bhojpuri
  * "en" otherwise
- state: extract state name if mentioned (bihar/बिहार, uttar pradesh/उत्तर प्रदेश, jharkhand/झारखंड, etc). If not mentioned, use "all"

Common Bhojpuri phrases:
- "हम बिहार से बानी" = I am from Bihar
- "का योजना बा" = What schemes are there
- "हमके चाही" = I need

Return ONLY JSON, no explanation.`;

  const result = await model.generateContent(prompt);
  const text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
  return JSON.parse(text);
}

// Translate scheme to target language
async function translateScheme(scheme: any, targetLanguage: string) {
  if (targetLanguage === 'en') {
    return scheme;
  }

  const needsTranslation = targetLanguage === 'bho' 
    ? !scheme.nameBhojpuri || !containsHindi(scheme.nameBhojpuri)
    : !containsHindi(scheme.name);

  if (!needsTranslation) {
    return scheme;
  }

  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash-exp" });
  
  const languageName = targetLanguage === 'bho' ? 'Bhojpuri (भोजपुरी)' : 'Hindi (हिंदी)';
  const exampleText = targetLanguage === 'bho' 
    ? 'ई योजना के फायदा... (authentic Bhojpuri with words like: के, सकेला, बा, बानी, रहल)'
    : 'यह योजना का लाभ...';

  const prompt = `Translate this government scheme information to ${languageName}. Return ONLY a JSON object:

Original scheme:
{
  "name": "${scheme.name}",
  "eligibility": "${scheme.eligibility}",
  "benefits": "${scheme.benefits}"
}

Translate to ${languageName}:
{
  "name": "योजना का नाम ${languageName} में",
  "eligibility": "पात्रता ${languageName} में",
  "benefits": "लाभ ${languageName} में"
}

${targetLanguage === 'bho' ? `
IMPORTANT FOR BHOJPURI:
- Use authentic Bhojpuri vocabulary and grammar
- Use words like: के, सकेला, बा, बानी, रहल, जाला, आवेला, करेला
- Example: "${exampleText}"
- Write in Devanagari script
` : ''}

Return ONLY the JSON object with ${languageName} translations, no markdown, no explanation.`;

  try {
    const result = await model.generateContent(prompt);
    const text = result.response.text().replace(/```json\n?|\n?```/g, '').trim();
    const translated = JSON.parse(text);
    
    return {
      ...scheme,
      name: translated.name,
      eligibility: translated.eligibility,
      benefits: translated.benefits
    };
  } catch (error) {
    console.error('Translation error:', error);
    return scheme;
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

    // State filter: Show central schemes + state-specific schemes
    if (userState !== 'all') {
      const stateFilter = {
        $or: [
          { targetState: 'all' },           // Central schemes
          { targetState: userState },        // User's state schemes
          { targetState: { $exists: false } } // Legacy schemes without state field
        ]
      };
      
      // Combine with existing filter
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

    // Vector search with metadata filter
    const collection = db.collection(process.env.ASTRA_DB_COLLECTION!);
    const cursor = collection.find(filter, {
      sort: { $vector: queryVector },
      limit: 25,
      includeSimilarity: true
    });

    const results = await cursor.toArray();
    console.log(`Found ${results.length} schemes after vector search`);

    // Additional client-side filtering for age
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
        }]
      });
    }

    // Format results with proper language handling
    let schemes = filtered.slice(0, 6).map((r: any) => {
      let name, eligibility, benefits;
      
      if (detectedLanguage === 'bho') {
        // Bhojpuri
        name = (r.nameBhojpuri && containsHindi(r.nameBhojpuri)) ? r.nameBhojpuri : r.name;
        eligibility = (r.eligibilityBhojpuri && containsHindi(r.eligibilityBhojpuri)) ? r.eligibilityBhojpuri : r.eligibility;
        benefits = (r.benefitsBhojpuri && containsHindi(r.benefitsBhojpuri)) ? r.benefitsBhojpuri : r.benefits;
      } else if (detectedLanguage === 'hi') {
        // Hindi
        name = (r.nameHindi && containsHindi(r.nameHindi)) ? r.nameHindi : r.name;
        eligibility = (r.eligibilityHindi && containsHindi(r.eligibilityHindi)) ? r.eligibilityHindi : r.eligibility;
        benefits = (r.benefitsHindi && containsHindi(r.benefitsHindi)) ? r.benefitsHindi : r.benefits;
      } else {
        // English
        name = r.name;
        eligibility = r.eligibility;
        benefits = r.benefits;
      }

      return {
        id: r._id || 'scheme-' + Math.random(),
        name,
        eligibility,
        benefits,
        applyLink: r.applyLink,
        category: r.category,
        targetGender: r.targetGender,
        targetAge: r.targetAge,
        targetState: r.targetState || 'all',
        schemeLevel: r.schemeLevel || 'central',
        similarity: r.$similarity?.toFixed(3),
        needsTranslation: (detectedLanguage === 'bho' && !containsHindi(name)) || 
                         (detectedLanguage === 'hi' && !containsHindi(name))
      };
    });

    // Translate any schemes that don't have proper language content
    if (detectedLanguage !== 'en') {
      const schemesToTranslate = schemes.filter(s => s.needsTranslation);
      
      if (schemesToTranslate.length > 0) {
        console.log(`Translating ${schemesToTranslate.length} schemes to ${detectedLanguage}...`);
        
        const translationPromises = schemesToTranslate.map(scheme => 
          translateScheme(scheme, detectedLanguage)
        );
        
        const translatedSchemes = await Promise.all(translationPromises);
        
        schemes = schemes.map(s => {
          if (s.needsTranslation) {
            const translated = translatedSchemes.find(t => t.id === s.id);
            if (translated) {
              const { needsTranslation, ...rest } = translated;
              return rest;
            }
          }
          const { needsTranslation, ...rest } = s;
          return rest;
        });
      }
    }

    console.log('Returning schemes:', schemes.map(s => ({
      name: s.name.substring(0, 50), 
      state: s.targetState,
      level: s.schemeLevel,
      hasCorrectLanguage: containsHindi(s.name) || detectedLanguage === 'en'
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
  }
}