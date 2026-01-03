'use client';

import React, { useState, useEffect } from 'react';
import { useParams, useRouter } from 'next/navigation';
import Image from 'next/image';
import { 
  ArrowLeft, 
  Volume2, 
  GraduationCap, 
  Heart, 
  Briefcase, 
  Users, 
  Home, 
  Building2, 
  Wallet, 
  Shield,
  Smartphone,
  Droplet
} from 'lucide-react';

interface Scheme {
  id: string;
  name: string;
  eligibility: string;
  benefits: string;
  applyLink: string;
  category?: string;
  targetState?: string;
  schemeLevel?: string;
}

const CATEGORY_INFO: Record<string, {
  title: string;
  titleHindi: string;
  titleBhojpuri: string;
  description: string;
  icon: any;
  color: string;
}> = {
  education: {
    title: 'Education',
    titleHindi: 'शिक्षा',
    titleBhojpuri: 'शिक्षा',
    description: 'Scholarships, loans, and skill development programs',
    icon: GraduationCap,
    color: 'from-blue-500 to-blue-600'
  },
  healthcare: {
    title: 'Healthcare',
    titleHindi: 'स्वास्थ्य',
    titleBhojpuri: 'सेहत',
    description: 'Insurance, medicine, and maternity benefits',
    icon: Heart,
    color: 'from-red-500 to-red-600'
  },
  employment: {
    title: 'Livelihood',
    titleHindi: 'आजीविका',
    titleBhojpuri: 'रोजगार',
    description: 'Employment, MNREGA, and business loans',
    icon: Briefcase,
    color: 'from-green-500 to-green-600'
  },
  women: {
    title: 'Women Power',
    titleHindi: 'महिला सशक्तिकरण',
    titleBhojpuri: 'महिला शक्ति',
    description: 'Pension, safety, and self-help group funding',
    icon: Users,
    color: 'from-pink-500 to-pink-600'
  },
  housing: {
    title: 'Housing',
    titleHindi: 'आवास',
    titleBhojpuri: 'घर',
    description: 'Affordable housing and water connection schemes',
    icon: Home,
    color: 'from-purple-500 to-purple-600'
  },
  pension: {
    title: 'Social Security',
    titleHindi: 'सामाजिक सुरक्षा',
    titleBhojpuri: 'सामाजिक सुरक्षा',
    description: 'Old age pensions and disability support',
    icon: Shield,
    color: 'from-yellow-600 to-yellow-700'
  },
  banking: {
    title: 'Digital Literacy',
    titleHindi: 'डिजिटल साक्षरता',
    titleBhojpuri: 'डिजिटल साक्षरता',
    description: 'Free computer training and device access',
    icon: Smartphone,
    color: 'from-indigo-500 to-indigo-600'
  },
  agriculture: {
    title: 'Sanitation',
    titleHindi: 'स्वच्छता',
    titleBhojpuri: 'सफाई',
    description: 'Toilet construction and clean water initiatives',
    icon: Droplet,
    color: 'from-teal-500 to-teal-600'
  }
};

export default function CategorySchemesPage() {
  const params = useParams();
  const router = useRouter();
  const category = params.category as string;
  
  const [schemes, setSchemes] = useState<Scheme[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isSpeaking, setIsSpeaking] = useState<string | null>(null);
  const [language, setLanguage] = useState<'en' | 'hi' | 'bho'>('en');

  const categoryInfo = CATEGORY_INFO[category.toLowerCase()] || {
    title: category,
    titleHindi: category,
    titleBhojpuri: category,
    description: 'Government schemes',
    icon: Building2,
    color: 'from-gray-500 to-gray-600'
  };

  const Icon = categoryInfo.icon;

  useEffect(() => {
    fetchSchemes();
  }, [category, language]);

  const fetchSchemes = async () => {
    setIsLoading(true);
    try {
      const response = await fetch('/api/schemes-by-category', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ category, language })
      });

      const data = await response.json();
      
      if (data.error) {
        throw new Error(data.error);
      }

      setSchemes(data.schemes || []);
    } catch (error) {
      console.error('Error fetching schemes:', error);
      setSchemes([]);
    } finally {
      setIsLoading(false);
    }
  };

  const speakText = (text: string, schemeId: string) => {
    if (typeof window === 'undefined') return;
    
    const synth = window.speechSynthesis;
    
    if (synth.speaking) {
      synth.cancel();
      setIsSpeaking(null);
      return;
    }

    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = language === 'en' ? 'en-US' : 'hi-IN';
    utterance.rate = 0.85;
    
    utterance.onend = () => setIsSpeaking(null);
    utterance.onerror = () => setIsSpeaking(null);
    
    setIsSpeaking(schemeId);
    synth.speak(utterance);
  };

  const getCategoryTitle = () => {
    switch (language) {
      case 'hi': return categoryInfo.titleHindi;
      case 'bho': return categoryInfo.titleBhojpuri;
      default: return categoryInfo.title;
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 via-blue-50 to-indigo-50">
      {/* Header */}
      <header className="bg-white shadow-sm sticky top-0 z-50 border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3">
          <div className="flex justify-between items-center">
            <div className="relative h-17 w-32">
              <Image
                src="/logo.png"
                alt="Awaaz Logo"
                fill
                className="object-contain"
                priority
              />
            </div>
            <div className="flex items-center gap-3">
              {/* Language Selector */}
              <select
                value={language}
                onChange={(e) => setLanguage(e.target.value as any)}
                className="px-4 py-2 border-2 border-gray-200 rounded-lg text-sm font-medium focus:outline-none focus:border-blue-500"
              >
                <option value="en">English</option>
                <option value="hi">हिंदी</option>
                <option value="bho">भोजपुरी</option>
              </select>
              <button
                onClick={() => router.push('/')}
                className="flex items-center gap-2 px-5 py-2.5 bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-full hover:from-blue-700 hover:to-indigo-700 transition-all duration-200 shadow-sm hover:shadow-md font-medium text-sm"
              >
                <ArrowLeft className="w-4 h-4" />
                <span>Back to Home</span>
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Hero Section */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 md:py-12">
        <div className={`bg-gradient-to-r ${categoryInfo.color} text-white rounded-3xl p-8 md:p-12 mb-8 shadow-2xl`}>
          <div className="flex items-center gap-6">
            <div className="bg-white/20 p-6 rounded-2xl backdrop-blur-sm">
              <Icon className="w-16 h-16" />
            </div>
            <div>
              <h1 className="text-4xl md:text-5xl font-bold mb-3">
                {getCategoryTitle()}
              </h1>
              <p className="text-lg md:text-xl text-white/90">
                {categoryInfo.description}
              </p>
              <p className="text-sm text-white/80 mt-2">
                {schemes.length} schemes available
              </p>
            </div>
          </div>
        </div>

        {/* Loading State */}
        {isLoading && (
          <div className="text-center py-16">
            <div className="inline-block">
              <div className="w-20 h-20 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin"></div>
              <p className="mt-6 text-lg text-gray-700 font-medium animate-pulse">
                Loading schemes...
              </p>
            </div>
          </div>
        )}

        {/* Schemes Grid */}
        {!isLoading && schemes.length > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 md:gap-8">
            {schemes.map((scheme, index) => (
              <div
                key={scheme.id}
                className="bg-white rounded-2xl shadow-lg overflow-hidden hover:shadow-2xl transition-all transform hover:-translate-y-2 border border-gray-100"
                style={{ animationDelay: `${index * 100}ms` }}
              >
                {/* Header */}
                <div className={`bg-gradient-to-r ${categoryInfo.color} text-white p-6 relative overflow-hidden`}>
                  <div className="absolute top-0 right-0 w-32 h-32 bg-white opacity-10 rounded-full -mr-16 -mt-16"></div>
                  <div className="relative z-10">
                    <div className="flex justify-between items-start mb-4">
                      <div className="bg-white/20 p-3 rounded-xl backdrop-blur-sm">
                        <Icon className="w-8 h-8" />
                      </div>
                      <button
                        onClick={() => speakText(
                          `${scheme.name}. Eligibility: ${scheme.eligibility}. Benefits: ${scheme.benefits}`,
                          scheme.id
                        )}
                        className={`p-3 rounded-full transition-all ${
                          isSpeaking === scheme.id 
                            ? 'bg-red-500 scale-110 animate-pulse' 
                            : 'bg-white/20 hover:bg-white/30 backdrop-blur-sm'
                        }`}
                      >
                        <Volume2 className="w-5 h-5" />
                      </button>
                    </div>
                    <h4 className="text-xl font-bold leading-tight mb-3">{scheme.name}</h4>
                    <div className="flex flex-wrap gap-2">
                      {scheme.schemeLevel && (
                        <span className={`inline-block px-3 py-1 rounded-full text-xs font-medium backdrop-blur-sm ${
                          scheme.schemeLevel === 'central' 
                            ? 'bg-yellow-500/30 text-yellow-100' 
                            : 'bg-green-500/30 text-green-100'
                        }`}>
                          {scheme.schemeLevel === 'central' ? '🇮🇳 CENTRAL' : '📍 STATE'}
                        </span>
                      )}
                      {scheme.targetState && scheme.targetState !== 'all' && (
                        <span className="inline-block px-3 py-1 bg-blue-500/30 text-blue-100 rounded-full text-xs font-medium backdrop-blur-sm">
                          {scheme.targetState.toUpperCase()}
                        </span>
                      )}
                    </div>
                  </div>
                </div>

                {/* Details */}
                <div className="p-6">
                  {scheme.eligibility && (
                    <div className="mb-5">
                      <h5 className="font-bold text-gray-900 mb-2 flex items-center gap-2">
                        <div className={`w-2 h-2 bg-gradient-to-r ${categoryInfo.color} rounded-full`}></div>
                        {language === 'en' ? 'Eligibility' : language === 'hi' ? 'पात्रता' : 'पात्रता'}
                      </h5>
                      <p className="text-gray-700 text-sm leading-relaxed">{scheme.eligibility}</p>
                    </div>
                  )}
                  
                  {scheme.benefits && (
                    <div className="mb-6">
                      <h5 className="font-bold text-gray-900 mb-2 flex items-center gap-2">
                        <div className={`w-2 h-2 bg-gradient-to-r ${categoryInfo.color} rounded-full`}></div>
                        {language === 'en' ? 'Benefits' : language === 'hi' ? 'लाभ' : 'फायदा'}
                      </h5>
                      <p className="text-gray-700 text-sm leading-relaxed">{scheme.benefits}</p>
                    </div>
                  )}
                  
                  {scheme.applyLink && (
                    <a
                      href={scheme.applyLink}
                      target="_blank"
                      rel="noopener noreferrer"
                      className={`block w-full text-center bg-gradient-to-r ${categoryInfo.color} text-white px-6 py-3.5 rounded-xl font-semibold hover:shadow-lg transition-all transform hover:scale-105`}
                    >
                      {language === 'en' ? 'Apply Now →' : language === 'hi' ? 'आवेदन करें →' : 'आवेदन करीं →'}
                    </a>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Empty State */}
        {!isLoading && schemes.length === 0 && (
          <div className="text-center py-20">
            <div className="inline-block mb-6">
              <div className={`w-32 h-32 bg-gradient-to-br ${categoryInfo.color} opacity-20 rounded-full flex items-center justify-center`}>
                <Icon className="w-16 h-16 text-gray-600" />
              </div>
            </div>
            <h3 className="text-3xl font-bold text-gray-800 mb-4">
              No Schemes Found
            </h3>
            <p className="text-lg text-gray-600 max-w-2xl mx-auto">
              We couldn't find any schemes in this category. Please check back later or try another category.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}