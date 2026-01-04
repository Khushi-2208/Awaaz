'use client';

import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  Mic, MicOff, Send, Volume2, Search, GraduationCap, Heart, 
  Briefcase, Users, Building2, Wallet, Sparkles, Languages, 
  ArrowRight, Globe
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

export default function Dashboard() {
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
      
      const loadVoices = () => {
        const voices = window.speechSynthesis.getVoices();
        console.log('Voices loaded:', voices.length);
      };
      
      loadVoices();
      if (window.speechSynthesis.onvoiceschanged !== undefined) {
        window.speechSynthesis.onvoiceschanged = loadVoices;
      }
      
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
          console.error('Speech error:', event.error);
          setIsListening(false);
        };
      }
    }
  }, []);

  const detectLanguage = (text: string): string => {
    const hindiPattern = /[\u0900-\u097F]/;
    if (!hindiPattern.test(text)) return 'en';
    
    // Enhanced Bhojpuri detection
    const bhojpuriKeywords = ['के', 'सकेला', 'बा', 'बानी', 'रहल', 'जाला', 'आवेला', 'करेला', 'खातिर', 'रउआ', 'का', 'हमार', 'हमरा', 'हमके'];
    const hasBhojpuri = bhojpuriKeywords.some(word => text.includes(word));
    return hasBhojpuri ? 'bho' : 'hi';
  };

  const toggleListening = () => {
    if (!recognitionRef.current) {
      alert('Speech recognition is not supported in this browser.');
      return;
    }

    if (isListening) {
      recognitionRef.current.stop();
      setIsListening(false);
    } else {
      recognitionRef.current.lang = 'hi-IN'; 
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

    const lang = detectLanguage(text);
    const utterance = new SpeechSynthesisUtterance(text);
    
    const voices = synthesisRef.current.getVoices();
    let selectedVoice = null;
    
    if (lang === 'hi' || lang === 'bho') {
      utterance.lang = 'hi-IN';
      selectedVoice = voices.find(v => v.lang === 'hi-IN') || voices.find(v => v.name.includes('Hindi'));
    } else {
      utterance.lang = 'en-IN'; 
      selectedVoice = voices.find(v => v.lang === 'en-IN') || voices.find(v => v.lang === 'en-US');
    }

    if (selectedVoice) utterance.voice = selectedVoice;
    utterance.rate = 0.9; 

    utterance.onstart = () => setIsSpeaking(schemeId);
    utterance.onend = () => setIsSpeaking(null);
    utterance.onerror = () => setIsSpeaking(null);
    
    synthesisRef.current.speak(utterance);
  };

  const handleSubmit = async (overrideText?: string) => {
    const textToSubmit = overrideText || inputText;
    if (!textToSubmit.trim()) return;

    const detectedLang = detectLanguage(textToSubmit);
    
    if(!overrideText) setInputText(''); 
    setIsLoading(true);
    setSchemes([]);

    try {
      const response = await fetch('/api/query-schemes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: textToSubmit, language: detectedLang })
      });

      const data = await response.json();
      if (data.error) throw new Error(data.error);
      setSchemes(data.schemes || []);

    } catch (error) {
      console.error(error);
      let errorMsg = 'Sorry, something went wrong.';
      if (detectedLang === 'bho') errorMsg = 'माफ करीं, कुछ गलती हो गइल।';
      else if (detectedLang === 'hi') errorMsg = 'क्षमा करें, कुछ गलत हो गया।';
      
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

  const getCategoryColor = (category?: string) => {
    switch (category?.toLowerCase()) {
      case 'education': return 'bg-blue-100 text-blue-700 border-blue-200';
      case 'healthcare': return 'bg-rose-100 text-rose-700 border-rose-200';
      case 'agriculture': return 'bg-green-100 text-green-700 border-green-200';
      case 'women': return 'bg-pink-100 text-pink-700 border-pink-200';
      case 'housing': return 'bg-teal-100 text-teal-700 border-teal-200';
      case 'social security': return 'bg-yellow-100 text-yellow-700 border-yellow-200';
      case 'digital literacy': return 'bg-violet-400 text-violet-700 border-violet-200';
      case 'sanitation': return 'bg-cyan-200 text-cyan-800 border-cyan-100';
      default: return 'bg-slate-100 text-slate-700 border-slate-200';
    }
  };

  const getCategoryIcon = (category?: string) => {
    const props = { size: 20 };
    switch (category?.toLowerCase()) {
      case 'education': return <GraduationCap {...props} />;
      case 'healthcare': return <Heart {...props} />;
      case 'agriculture': return <Building2 {...props} />;
      case 'women': return <Users {...props} />;
      case 'banking': return <Wallet {...props} />;
      default: return <Briefcase {...props} />;
    }
  };

  return (
    <div className="min-h-screen bg-[#FFFBF2] font-sans">
      
      <div className="relative bg-slate-900 pb-40 pt-16 px-4 overflow-hidden rounded-b-[3rem]">
        
        <div className="absolute top-0 left-0 w-full h-full overflow-hidden opacity-20 pointer-events-none">
          <div className="absolute top-[-50px] right-[-50px] w-96 h-96 bg-orange-500 rounded-full blur-[100px]" />
          <div className="absolute bottom-[-50px] left-[-50px] w-96 h-96 bg-blue-500 rounded-full blur-[100px]" />
        </div>

        <div className="relative z-10 max-w-4xl mx-auto text-center mt-6">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <h1 className="text-4xl md:text-6xl font-bold text-white mb-8 leading-tight">
              <span className="block text-transparent bg-clip-text bg-gradient-to-r from-orange-400 to-yellow-300">
                Namaste / नमस्ते / प्रणाम
              </span>
              Welcome to Awaaz
            </h1>
            
            <div className="text-slate-300 text-lg md:text-xl mb-12 max-w-3xl mx-auto space-y-2 leading-relaxed">
              <p>Ask about government schemes in your own language.</p>
              <p className="font-medium text-slate-200">सरकारी योजनाओं के बारे में अपनी भाषा में पूछें।</p>
              <p className="font-medium text-yellow-300">सरकारी योजना के बारे में रउआ अपना भाषा में पूछ सकत बानी।</p>
            </div>
          </motion.div>

          <motion.div 
            initial={{ scale: 0.95, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            transition={{ delay: 0.2 }}
            className="bg-white rounded-[2.5rem] p-4 shadow-2xl shadow-orange-900/30"
          >
            <div className="relative flex items-center bg-slate-50 rounded-[2rem] border-2 border-slate-200 focus-within:border-orange-400 focus-within:ring-4 focus-within:ring-orange-100 transition-all p-3">
              
              <button
                onClick={toggleListening}
                className={`flex-shrink-0 w-24 h-24 md:w-28 md:h-28 rounded-[1.5rem] flex items-center justify-center transition-all shadow-lg ${
                  isListening 
                  ? 'bg-red-500 text-white animate-pulse' 
                  : 'bg-gradient-to-br from-orange-500 to-orange-600 text-white hover:scale-105'
                }`}
              >
                {isListening ? <MicOff size={48} /> : <Mic size={48} />}
              </button>

              <input
                type="text"
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSubmit()}
                placeholder={isListening ? "बोलिये... / Listening..." : "Type or speak here..."}
                className="flex-1 bg-transparent border-none focus:ring-0 text-xl md:text-3xl px-6 text-slate-800 placeholder:text-slate-400 h-full font-medium"
              />

              <button
                onClick={() => handleSubmit()}
                disabled={!inputText.trim() || isLoading}
                className="flex-shrink-0 w-20 h-20 mr-2 bg-slate-900 text-white rounded-[1.5rem] flex items-center justify-center hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-slate-900 transition-colors shadow-md"
              >
                {isLoading ? <span className="animate-spin text-2xl">⟳</span> : <Send size={32} />}
              </button>
            </div>
            
            <div className="flex flex-wrap gap-3 justify-center mt-6 px-2 pb-2">
              <span className="text-sm text-slate-400 uppercase font-bold tracking-wider mr-2 pt-2">Try:</span>
              <QuickChip text="Education Loan" onClick={() => handleSubmit("I need education loan")} />
              <QuickChip text="वृद्धावस्था पेंशन" onClick={() => handleSubmit("मुझे वृद्धावस्था पेंशन चाहिए")} />
              <QuickChip text="हमरा खातिर योजना" onClick={() => handleSubmit("हम बिहार से बानी, का योजना बा")} />
            </div>
          </motion.div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 -mt-10 relative z-20 pb-20">
        
        {isLoading && (
          <div className="bg-white rounded-3xl p-12 text-center shadow-xl border border-orange-100 min-h-[400px] flex flex-col items-center justify-center mt-12">
            <div className="w-20 h-20 border-8 border-orange-100 border-t-orange-600 rounded-full animate-spin mb-8"></div>
            <h3 className="text-2xl font-bold text-slate-800 animate-pulse">Searching Government Database...</h3>
            <p className="text-slate-500 mt-2 text-lg">रउआ खातिर सही योजना खोजत बानी...</p>
          </div>
        )}

        {!isLoading && schemes.length > 0 && (
          <div className="space-y-8 mt-12">
            <div className="flex items-center justify-between mb-6 px-2">
              <h2 className="text-3xl font-bold flex items-center gap-3 text-slate-800">
                <div className="p-2 bg-yellow-100 rounded-lg text-yellow-600"><Sparkles size={24}/></div>
                Found {schemes.length} Schemes
              </h2>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
              <AnimatePresence>
                {schemes.map((scheme, index) => (
                  <SchemeCard 
                    key={index} 
                    scheme={scheme} 
                    index={index} 
                    onSpeak={speakText}
                    isSpeaking={isSpeaking === scheme.id}
                    getCategoryColor={getCategoryColor}
                    getCategoryIcon={getCategoryIcon}
                    detectLanguage={detectLanguage}
                  />
                ))}
              </AnimatePresence>
            </div>
          </div>
        )}

        {!isLoading && schemes.length === 0 && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 mt-12">
              
            <div className="lg:col-span-2 space-y-8">
              <div className="bg-white rounded-3xl p-10 shadow-lg border border-slate-100">
                <div className="flex items-center gap-4 mb-8">
                  <div className="p-4 bg-blue-50 text-blue-600 rounded-2xl">
                    <Languages size={32} />
                  </div>
                  <div>
                    <h3 className="text-2xl font-bold text-slate-900">We speak your language</h3>
                    <p className="text-slate-500">रउआ के भाषा में बात करीला</p>
                  </div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <LanguageCard lang="English" script="Hello" sub="Ask naturally" color="bg-orange-50 text-orange-800 border-orange-100" />
                  <LanguageCard lang="Hindi" script="नमस्ते" sub="हिंदी में पूछें" color="bg-green-50 text-green-800 border-green-100" />
                  <LanguageCard lang="Bhojpuri" script="प्रणाम" sub="भोजपुरी में पूछा" color="bg-purple-50 text-purple-800 border-purple-100" />
                </div>
              </div>

              <div className="bg-gradient-to-br from-slate-900 to-slate-800 rounded-3xl p-10 text-white shadow-xl relative overflow-hidden">
                <div className="relative z-10">
                  <h3 className="text-3xl font-bold mb-6">Need help getting started?</h3>
                  
                  <div className="space-y-6 mb-8">
                    {/* English */}
                    <div className="bg-white/10 p-4 rounded-xl backdrop-blur-sm border border-white/10">
                      <p className="text-sm text-slate-400 uppercase font-bold mb-1">English</p>
                      <p className="text-lg">"I am a student from Bihar looking for education loan."</p>
                    </div>

                    {/* Hindi */}
                    <div className="bg-white/10 p-4 rounded-xl backdrop-blur-sm border border-white/10">
                      <p className="text-sm text-slate-400 uppercase font-bold mb-1">Hindi (हिंदी)</p>
                      <p className="text-lg">"मैं बिहार का एक छात्र हूँ और मुझे शिक्षा लोन चाहिए।"</p>
                    </div>

                    {/* Bhojpuri */}
                    <div className="bg-white/10 p-4 rounded-xl backdrop-blur-sm border border-white/10">
                      <p className="text-sm text-slate-400 uppercase font-bold mb-1">Bhojpuri (भोजपुरी)</p>
                      <p className="text-lg">"हम बिहार के छात्र बानी, हमरा पढ़ाई खातिर लोन चाही।"</p>
                    </div>
                  </div>

                  <button onClick={toggleListening} className="bg-white text-slate-900 px-8 py-4 rounded-full font-bold hover:bg-orange-400 hover:text-white transition-all flex items-center gap-3 text-lg shadow-lg">
                    <Mic size={24} /> बोला शुरू करा / Start Speaking
                  </button>
                </div>
                <div className="absolute right-[-20px] bottom-[-20px] opacity-10 rotate-12">
                  <Mic size={250} />
                </div>
              </div>
            </div>

            <div className="bg-white rounded-3xl p-8 shadow-lg border border-slate-100 h-fit">
              <h3 className="text-xl font-bold text-slate-800 mb-6 flex items-center gap-2">
                <Search size={22} className="text-orange-500" /> Browse Topics / विषय
              </h3>
              <div className="space-y-4">
                <TopicRow icon={<GraduationCap size={20} />} text="Education / शिक्षा / पढ़ाई" onClick={() => handleSubmit("Education schemes")} />
                <TopicRow icon={<Heart size={20} />} text="Health / स्वास्थ्य / सेहत" onClick={() => handleSubmit("Health schemes")} />
                <TopicRow icon={<Building2 size={20} />} text="Farming / कृषि / खेती" onClick={() => handleSubmit("Agriculture schemes")} />
                <TopicRow icon={<Users size={20} />} text="Women / महिला / औरत" onClick={() => handleSubmit("Women schemes")} />
                <TopicRow icon={<Wallet size={20} />} text="Pension / पेंशन" onClick={() => handleSubmit("Pension schemes")} />
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

const QuickChip = ({ text, onClick }: { text: string, onClick: () => void }) => (
  <button 
    onClick={onClick}
    className="px-4 py-2 bg-slate-100 hover:bg-orange-100 hover:text-orange-700 text-slate-600 rounded-full text-sm font-medium transition-colors border border-slate-200 hover:border-orange-200"
  >
    {text}
  </button>
);

const LanguageCard = ({ lang, script, sub, color }: any) => (
  <div className={`p-6 rounded-2xl ${color} border hover:shadow-md transition-all cursor-default`}>
    <div className="text-3xl font-bold mb-2">{script}</div>
    <div className="font-bold text-lg opacity-90">{lang}</div>
    <div className="text-sm opacity-70 mt-1">{sub}</div>
  </div>
);

const TopicRow = ({ icon, text, onClick }: any) => (
  <button 
    onClick={onClick}
    className="w-full flex items-center justify-between p-4 hover:bg-orange-50 rounded-xl transition-all group text-left border border-transparent hover:border-orange-100"
  >
    <div className="flex items-center gap-4 text-slate-600 group-hover:text-orange-700 font-medium text-lg">
      <div className="bg-slate-100 p-2 rounded-lg group-hover:bg-white transition-colors">{icon}</div>
      <span>{text}</span>
    </div>
    <ArrowRight size={18} className="text-slate-300 group-hover:text-orange-500 transform group-hover:translate-x-1 transition-all"/>
  </button>
);

const SchemeCard = ({ scheme, index, onSpeak, isSpeaking, getCategoryColor, getCategoryIcon, detectLanguage }: any) => {
  const styles = getCategoryColor(scheme.category);
  
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.1 }}
      className="bg-white rounded-3xl shadow-md border border-slate-100 overflow-hidden hover:shadow-2xl transition-all duration-300 group flex flex-col h-full"
    >
      <div className="p-8 pb-4">
        <div className="flex justify-between items-start mb-6">
          <div className={`px-4 py-2 rounded-full text-xs font-bold flex items-center gap-2 uppercase tracking-wide border ${styles}`}>
            {getCategoryIcon(scheme.category)}
            {scheme.category || 'General'}
          </div>
          <button 
            onClick={() => {
              const lang = detectLanguage(scheme.name);
              let text = '';
              if (lang === 'bho') text = `${scheme.name}. एकर पात्रता बा: ${scheme.eligibility}. एकर फायदा बा: ${scheme.benefits}`;
              else if (lang === 'hi') text = `${scheme.name}. इसकी पात्रता है: ${scheme.eligibility}. इसके लाभ हैं: ${scheme.benefits}`;
              else text = `${scheme.name}. Eligibility is: ${scheme.eligibility}. Benefits are: ${scheme.benefits}`;
              
              onSpeak(text, scheme.id);
            }}
            className={`p-3 rounded-full transition-all shadow-sm ${isSpeaking ? 'bg-orange-500 text-white animate-pulse' : 'bg-slate-50 text-slate-500 hover:bg-slate-100'}`}
          >
            <Volume2 size={22} />
          </button>
        </div>
        <h3 className="text-2xl font-bold text-slate-900 leading-tight mb-2 group-hover:text-orange-600 transition-colors">
          {scheme.name}
        </h3>
        {scheme.targetState && (
          <span className="text-sm font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
            <Globe size={14} /> {scheme.targetState}
          </span>
        )}
      </div>

      <div className="px-8 py-6 border-t border-slate-50 bg-slate-50/50 flex-grow space-y-6">
        <div>
          <h4 className="text-xs font-bold text-slate-400 uppercase mb-2 tracking-widest">Eligibility / पात्रता</h4>
          <p className="text-slate-700 leading-relaxed line-clamp-3 hover:line-clamp-none transition-all">
            {scheme.eligibility}
          </p>
        </div>
        <div>
          <h4 className="text-xs font-bold text-slate-400 uppercase mb-2 tracking-widest">Benefits / लाभ / फायदा</h4>
          <p className="text-slate-700 leading-relaxed line-clamp-3 hover:line-clamp-none transition-all">
            {scheme.benefits}
          </p>
        </div>
      </div>

      <div className="p-6 bg-white border-t border-slate-100">
        <a 
          href={scheme.applyLink} 
          target="_blank" 
          rel="noreferrer"
          className="flex items-center justify-center gap-2 w-full py-4 rounded-xl bg-slate-900 text-white font-bold text-lg hover:bg-orange-600 transition-colors shadow-lg hover:shadow-orange-500/30"
        >
          Apply Now <ArrowRight size={20} />
        </a>
      </div>
    </motion.div>
  );
};