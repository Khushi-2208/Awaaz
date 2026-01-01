'use client';

import React, { useState, useEffect, useRef } from 'react';
import Image from 'next/image';
import { Mic, MicOff, Send, Volume2, LogOut, Search, GraduationCap, Heart, Briefcase, Users, Home, Building2, Wallet, Baby } from 'lucide-react';

interface Scheme {
  id: string;
  name: string;
  eligibility: string;
  benefits: string;
  applyLink: string;
  category?: string;
}

export default function Page() {
  const [isListening, setIsListening] = useState(false);
  const [inputText, setInputText] = useState('');
  const [schemes, setSchemes] = useState<Scheme[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isSpeaking, setIsSpeaking] = useState<string | null>(null);
  const recognitionRef = useRef<any>(null);
  const synthesisRef = useRef<SpeechSynthesis | null>(null);

  useEffect(() => {
    if (typeof window !== 'undefined') {
      synthesisRef.current = window.speechSynthesis;
      
      if ('webkitSpeechRecognition' in window || 'SpeechRecognition' in window) {
        const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
        recognitionRef.current = new SpeechRecognition();
        recognitionRef.current.continuous = true;
        recognitionRef.current.interimResults = true;
        recognitionRef.current.lang = 'hi-IN';

        recognitionRef.current.onresult = (event: any) => {
          let finalTranscript = '';
          for (let i = event.resultIndex; i < event.results.length; i++) {
            if (event.results[i].isFinal) {
              finalTranscript += event.results[i][0].transcript;
            }
          }
          if (finalTranscript) {
            setInputText(prev => (prev + ' ' + finalTranscript).trim());
          }
        };

        recognitionRef.current.onerror = (event: any) => {
          console.error('Speech recognition error:', event.error);
          setIsListening(false);
        };
      }
    }
  }, []);

  const detectLanguage = (text: string): string => {
    const hindiPattern = /[\u0900-\u097F]/;
    return hindiPattern.test(text) ? 'hi' : 'en';
  };

  const toggleListening = () => {
    if (!recognitionRef.current) {
      alert('Speech recognition is not supported in your browser');
      return;
    }

    if (isListening) {
      recognitionRef.current.stop();
      setIsListening(false);
    } else {
      const detectedLang = detectLanguage(inputText);
      recognitionRef.current.lang = detectedLang === 'hi' ? 'hi-IN' : 'en-IN';
      recognitionRef.current.start();
      setIsListening(true);
    }
  };

  const speakText = (text: string, schemeId: string) => {
    if (!synthesisRef.current) return;

    if (synthesisRef.current.speaking) {
      synthesisRef.current.cancel();
      setIsSpeaking(null);
      return;
    }

    const utterance = new SpeechSynthesisUtterance(text);
    const lang = detectLanguage(text);
    utterance.lang = lang === 'hi' ? 'hi-IN' : 'en-US';
    utterance.rate = 0.9;
    
    utterance.onend = () => setIsSpeaking(null);
    utterance.onerror = () => setIsSpeaking(null);
    
    setIsSpeaking(schemeId);
    synthesisRef.current.speak(utterance);
  };

  const handleSubmit = async () => {
    if (!inputText.trim()) return;

    const userMessage = inputText.trim();
    const detectedLang = detectLanguage(userMessage);
    
    setInputText('');
    setIsLoading(true);
    setSchemes([]);

    try {
      const response = await fetch('/api/query-schemes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: userMessage, language: detectedLang })
      });

      const data = await response.json();
      
      if (data.error) {
        throw new Error(data.error);
      }

      setSchemes(data.schemes || []);
    } catch (error) {
      console.error('Error fetching schemes:', error);
      const errorMsg = detectedLang === 'hi' 
        ? 'क्षमा करें, कुछ गलत हो गया। कृपया पुनः प्रयास करें।'
        : 'Sorry, something went wrong. Please try again.';
      
      setSchemes([{
        id: 'error',
        name: errorMsg,
        eligibility: '',
        benefits: '',
        applyLink: ''
      }]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleLogout = () => {
    if (confirm('Are you sure you want to logout?')) {
      setSchemes([]);
      setInputText('');
    }
  };

  const getCategoryIcon = (category?: string) => {
    const iconClass = "w-10 h-10";
    switch (category?.toLowerCase()) {
      case 'education': return <GraduationCap className={iconClass} />;
      case 'healthcare': return <Heart className={iconClass} />;
      case 'employment':
      case 'entrepreneurship': return <Briefcase className={iconClass} />;
      case 'women': return <Users className={iconClass} />;
      case 'housing': return <Home className={iconClass} />;
      case 'agriculture': return <Building2 className={iconClass} />;
      case 'banking':
      case 'pension': return <Wallet className={iconClass} />;
      case 'children': return <Baby className={iconClass} />;
      default: return <Search className={iconClass} />;
    }
  };

  const getCategoryColor = (category?: string) => {
    switch (category?.toLowerCase()) {
      case 'education': return { bg: 'from-blue-500 to-blue-600', text: 'text-blue-600' };
      case 'healthcare': return { bg: 'from-red-500 to-red-600', text: 'text-red-600' };
      case 'employment':
      case 'entrepreneurship': return { bg: 'from-green-500 to-green-600', text: 'text-green-600' };
      case 'women': return { bg: 'from-pink-500 to-pink-600', text: 'text-pink-600' };
      case 'housing': return { bg: 'from-purple-500 to-purple-600', text: 'text-purple-600' };
      case 'agriculture': return { bg: 'from-green-600 to-green-700', text: 'text-green-700' };
      case 'banking':
      case 'pension': return { bg: 'from-indigo-500 to-indigo-600', text: 'text-indigo-600' };
      case 'children': return { bg: 'from-yellow-500 to-yellow-600', text: 'text-yellow-600' };
      default: return { bg: 'from-gray-500 to-gray-600', text: 'text-gray-600' };
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
          className="object-contain "
          priority
        />
      </div>
      <button
        onClick={handleLogout}
        className="flex items-center gap-2 px-5 py-2.5 bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-full hover:from-blue-700 hover:to-indigo-700 transition-all duration-200 shadow-sm hover:shadow-md font-medium text-sm"
      >
        <LogOut className="w-4 h-4" />
        <span>Logout</span>
      </button>
    </div>
  </div>
</header>

      {/* Hero Section */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 md:py-12">
        <div className="text-center mb-8 md:mb-12">
          <h2 className="text-4xl md:text-5xl font-bold text-gray-900 mb-4 leading-tight">
            Government Schemes,<br />Simplified.
          </h2>
          <p className="text-lg md:text-xl text-gray-600 max-w-3xl mx-auto leading-relaxed">
            Bridging the gap between you and your rights using AI-powered voice navigation.
            <br className="hidden md:block" />
            <span className="text-orange-600 font-medium">No forms. No typing. Just speak.</span>
          </p>
        </div>

        {/* Voice Input Section */}
        <div className="max-w-4xl mx-auto mb-12">
          <div className="bg-white rounded-3xl shadow-2xl p-6 md:p-8 border border-gray-100">
            <div className="flex flex-col md:flex-row items-center gap-4">
              <button
                onClick={toggleListening}
                className={`w-16 h-16 md:w-20 md:h-20 rounded-full transition-all shadow-xl flex items-center justify-center ${
                  isListening 
                    ? 'bg-gradient-to-br from-orange-500 to-red-500 text-white animate-pulse scale-110 ring-4 ring-orange-200' 
                    : 'bg-gradient-to-br from-orange-500 to-orange-600 text-white hover:scale-105 hover:shadow-2xl'
                }`}
              >
                {isListening ? <MicOff className="w-8 h-8 md:w-10 md:h-10" /> : <Mic className="w-8 h-8 md:w-10 md:h-10" />}
              </button>
              
              <input
                type="text"
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    handleSubmit();
                  }
                }}
                placeholder="Tell me about yourself... / अपने बारे में बताएं..."
                className="flex-1 text-gray-900 px-5 md:px-6 py-3 md:py-4 border-2 border-gray-200 rounded-2xl focus:outline-none focus:border-blue-500 text-base md:text-lg transition-all"
              />
              
              <button
                onClick={handleSubmit}
                disabled={isLoading || !inputText.trim()}
                className="w-16 h-16 md:w-20 md:h-20 bg-gradient-to-br from-green-500 to-green-600 text-white rounded-full hover:scale-105 disabled:opacity-50 disabled:cursor-not-allowed disabled:scale-100 transition-all shadow-xl flex items-center justify-center"
              >
                <Send className="w-7 h-7 md:w-9 md:h-9" />
              </button>
            </div>
            <p className="text-center text-xs md:text-sm text-gray-500 mt-4">
              🎤 Click mic to speak or type directly • माइक पर क्लिक करें या सीधे टाइप करें
            </p>
          </div>
        </div>

        {/* Loading State */}
        {isLoading && (
          <div className="text-center py-16">
            <div className="inline-block">
              <div className="w-20 h-20 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin"></div>
              <p className="mt-6 text-lg text-gray-700 font-medium animate-pulse">
                🔍 Searching for your schemes...
              </p>
            </div>
          </div>
        )}

        {/* Schemes Grid */}
        {!isLoading && schemes.length > 0 && (
          <div className="animate-fadeIn">
            <div className="text-center mb-10">
              <h3 className="text-3xl md:text-4xl font-bold text-gray-900 mb-2">
                {schemes[0].id === 'error' || schemes[0].id === 'no-results' 
                  ? '🔍 Search Results' 
                  : '✨ Your Eligible Schemes'}
              </h3>
              <p className="text-gray-600">
                {schemes[0].id === 'error' || schemes[0].id === 'no-results' 
                  ? 'Try refining your search with more details' 
                  : `Found ${schemes.length} scheme${schemes.length > 1 ? 's' : ''} matching your profile`}
              </p>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 md:gap-8">
              {schemes.map((scheme, index) => {
                const colors = getCategoryColor(scheme.category);
                return (
                  <div
                    key={scheme.id}
                    className="bg-white rounded-2xl shadow-lg overflow-hidden hover:shadow-2xl transition-all transform hover:-translate-y-2 border border-gray-100"
                    style={{ animationDelay: `${index * 100}ms` }}
                  >
                    {/* Category Header */}
                    <div className={`bg-gradient-to-r ${colors.bg} text-white p-6 relative overflow-hidden`}>
                      <div className="absolute top-0 right-0 w-32 h-32 bg-white opacity-10 rounded-full -mr-16 -mt-16"></div>
                      <div className="relative z-10">
                        <div className="flex justify-between items-start mb-4">
                          <div className="bg-white/20 p-3 rounded-xl backdrop-blur-sm">
                            {getCategoryIcon(scheme.category)}
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
                        <h4 className="text-xl md:text-2xl font-bold leading-tight">{scheme.name}</h4>
                        {scheme.category && (
                          <span className="inline-block mt-2 px-3 py-1 bg-white/20 rounded-full text-xs font-medium backdrop-blur-sm">
                            {scheme.category.toUpperCase()}
                          </span>
                        )}
                      </div>
                    </div>

                    {/* Scheme Details */}
                    <div className="p-6">
                      {scheme.eligibility && (
                        <div className="mb-5">
                          <h5 className={`font-bold text-gray-900 mb-2 flex items-center gap-2 ${colors.text}`}>
                            <div className={`w-2 h-2 ${colors.bg} bg-gradient-to-r rounded-full`}></div>
                            Eligibility / पात्रता
                          </h5>
                          <p className="text-gray-700 text-sm leading-relaxed">{scheme.eligibility}</p>
                        </div>
                      )}
                      
                      {scheme.benefits && (
                        <div className="mb-6">
                          <h5 className={`font-bold text-gray-900 mb-2 flex items-center gap-2 ${colors.text}`}>
                            <div className={`w-2 h-2 ${colors.bg} bg-gradient-to-r rounded-full`}></div>
                            Benefits / लाभ
                          </h5>
                          <p className="text-gray-700 text-sm leading-relaxed">{scheme.benefits}</p>
                        </div>
                      )}
                      
                      {scheme.applyLink && scheme.id !== 'error' && scheme.id !== 'no-results' && (
                        <a
                          href={scheme.applyLink}
                          target="_blank"
                          rel="noopener noreferrer"
                          className={`block w-full text-center bg-gradient-to-r ${colors.bg} text-white px-6 py-3.5 rounded-xl font-semibold hover:shadow-lg transition-all transform hover:scale-105`}
                        >
                          Apply Now / आवेदन करें →
                        </a>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Empty State */}
        {!isLoading && schemes.length === 0 && (
          <div className="text-center py-20">
            <div className="inline-block mb-6">
              <div className="w-32 h-32 bg-gradient-to-br from-blue-100 to-indigo-100 rounded-full flex items-center justify-center">
                <Search className="w-16 h-16 text-blue-600" />
              </div>
            </div>
            <h3 className="text-3xl font-bold text-gray-800 mb-4">
              Start Your Search
            </h3>
            <p className="text-lg text-gray-600 max-w-2xl mx-auto leading-relaxed">
              Tell us about yourself using voice or text, and we'll find the government schemes you're eligible for.
              <br />
              <span className="text-blue-600 font-medium">For example:</span> "I am 30 years old male and want health schemes" or "मैं एक किसान हूं"
            </p>
          </div>
        )}
      </div>

      {/* Footer */}
      <footer className="bg-white mt-20 border-t border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 text-center">
          <p className="text-gray-600 mb-2">
            © 2026 Awaaz. Empowering citizens through accessible government schemes.
          </p>
          <p className="text-sm text-gray-500">
            Made with ❤️ for every Indian citizen
          </p>
        </div>
      </footer>

      <style jsx>{`
        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(20px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        .animate-fadeIn {
          animation: fadeIn 0.6s ease-out;
        }

        .animate-fadeIn > div > div {
          animation: fadeIn 0.8s ease-out backwards;
        }
      `}</style>
    </div>
  );
}