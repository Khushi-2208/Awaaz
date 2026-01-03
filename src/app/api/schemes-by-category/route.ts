import { NextRequest, NextResponse } from 'next/server';
import { DataAPIClient } from '@datastax/astra-db-ts';

const client = new DataAPIClient(process.env.ASTRA_DB_APPLICATION_TOKEN!);
const db = client.db(process.env.ASTRA_DB_API_ENDPOINT!, {
  keyspace: process.env.ASTRA_DB_KEYSPACE
});

export async function POST(req: NextRequest) {
  try {
    const { category, language = 'en' } = await req.json();

    if (!category) {
      return NextResponse.json({ error: 'Category required' }, { status: 400 });
    }

    console.log('Fetching schemes for category:', category);
    console.log('Language:', language);

    const collection = db.collection(process.env.ASTRA_DB_COLLECTION!);
    
    // Query by category
    const filter = {
      category: category.toLowerCase()
    };

    const cursor = collection.find(filter, {
      limit: 50
    });

    const results = await cursor.toArray();
    console.log(`Found ${results.length} schemes for category: ${category}`);

    // Format results based on language
    const schemes = results.map((r: any) => {
      let name, eligibility, benefits;
      
      if (language === 'bho') {
        name = r.nameBhojpuri || r.nameHindi || r.name;
        eligibility = r.eligibilityBhojpuri || r.eligibilityHindi || r.eligibility;
        benefits = r.benefitsBhojpuri || r.benefitsHindi || r.benefits;
      } else if (language === 'hi') {
        name = r.nameHindi || r.name;
        eligibility = r.eligibilityHindi || r.eligibility;
        benefits = r.benefitsHindi || r.benefits;
      } else {
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
        targetState: r.targetState || 'all',
        schemeLevel: r.schemeLevel || 'central',
        targetAge: r.targetAge,
        targetGender: r.targetGender
      };
    });

    return NextResponse.json({ 
      schemes,
      category,
      language
    });

  } catch (error: any) {
    console.error('Category schemes error:', error);
    return NextResponse.json({ 
      error: error.message || 'Internal error' 
    }, { status: 500 });
  }
}