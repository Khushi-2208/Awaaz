'use client';

import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence, useInView, useMotionValue, useSpring } from 'framer-motion';
import {
  Mic, Play, Users, FileText, IndianRupee, ShieldCheck, Globe,
  ArrowRight, Building2, GraduationCap,
  Stethoscope, Tractor, HeartHandshake, Smartphone, Quote, ChevronRight, ChevronDown, ChevronUp
} from 'lucide-react';
import Link from 'next/link';

const HERO_SLIDES = [
  {
    id: 1,
    title: "Government Schemes, Simplified.",
    subtitle: "Bridging the gap between you and your rights using AI-powered voice navigation.",
    image: "/hero/1.jpg",
    accent: "text-orange-400"
  },
  {
    id: 2,
    title: "Empowering Rural India.",
    subtitle: "No forms. No typing. Just speak in your local language to access benefits.",
    image: "/hero/2.jpg",
    accent: "text-blue-400"
  },
  {
    id: 3,
    title: "Technology for Good.",
    subtitle: "Helping over 1.2 Lakh citizens find the support they need instantly.",
    image: "/hero/3.jpg",
    accent: "text-green-400"
  },
  {
    id: 4,
    title: "A Voice for Everyone.",
    subtitle: "Ensuring no one is left behind in the digital revolution.",
    image: "/hero/4.jpg",
    accent: "text-yellow-400"
  }
];

const PROGRAMMES = [
  { id: 1, title: "Education", desc: "Scholarships, loans, and skill development.", color: "bg-blue-600", icon: GraduationCap },
  { id: 2, title: "Healthcare", desc: "Insurance, medicine, and maternity benefits.", color: "bg-rose-600", icon: Stethoscope },
  { id: 3, title: "Livelihood", desc: "Employment, MNREGA, and business loans.", color: "bg-green-600", icon: Tractor },
  { id: 4, title: "Women", desc: "Pension, safety, and self-help group funding.", color: "bg-orange-600", icon: Users },
  { id: 5, title: "Housing", desc: "Affordable housing and water connection schemes.", color: "bg-teal-600", icon: Building2 },
  { id: 6, title: "Social Security", desc: "Old age pensions and disability support.", color: "bg-yellow-600", icon: ShieldCheck },
  { id: 7, title: "Digital Literacy", desc: "Free computer training and device access.", color: "bg-indigo-600", icon: Smartphone },
  { id: 8, title: "Sanitation", desc: "Toilet construction and clean water initiatives.", color: "bg-cyan-600", icon: Globe },
];

const TESTIMONIALS = [
  { name: "Ramesh Kumar", location: "Begusarai", role: "Farmer", quote: "I didn't know I was eligible for the PM-Kisan scheme. I just spoke to the app, and it told me exactly what documents to submit." },
  { name: "Sunita Devi", location: "Patna", role: "Homemaker", quote: "Applying for my daughter's scholarship was always difficult. With Awaaz, I could understand the process in Bhojpuri." },
  { name: "Amit Singh", location: "CSC Operator", role: "Agent", quote: "This tool helps me serve 3x more people every day. The voice feature makes data entry so much faster." },
  { name: "Rajesh Yadav", location: "Gaya", role: "Laborer", quote: "I got my MNREGA wages on time because I could track the status using just my voice." },
  { name: "Priya Kumari", location: "Muzaffarpur", role: "Student", quote: "Found the right education loan for my engineering degree without visiting the bank 10 times." },
];

export default function Home() {
  const [currentSlide, setCurrentSlide] = useState(0);
  const [showAllProgrammes, setShowAllProgrammes] = useState(false);

  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentSlide((prev) => (prev + 1) % HERO_SLIDES.length);
    }, 4000);
    return () => clearInterval(timer);
  }, []);

  return (
    <main className="font-sans bg-[#FFFBF2] selection:bg-orange-200 selection:text-orange-900">
      <section className="relative h-[90vh] w-full flex items-center overflow-hidden bg-slate-900">
        <div className="absolute inset-0 z-0">
          <video autoPlay loop muted playsInline className="absolute inset-0 w-full h-full object-cover">
            <source src="/hero/vid.mp4" type="video/mp4" />
          </video>
          <div className="absolute inset-0 bg-linear-to-r from-black/80 via-black/40 to-transparent z-10" />
        </div>

        <div className="relative z-20 w-full max-w-7xl mx-auto px-6 md:px-12 grid grid-cols-1 lg:grid-cols-2">
          <div className="text-left">
            <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.8 }}>
              <h1 className="text-5xl md:text-7xl font-bold text-white mb-6 leading-tight drop-shadow-lg">
                Government Schemes, <br /> Simplified.
              </h1>

              <p className="text-xl text-white/90 mb-10 max-w-lg leading-relaxed font-medium drop-shadow-md">
                Bridging the gap between you and your rights using AI-powered voice navigation. No forms. No typing. Just speak.
              </p>

              <div className="flex flex-wrap gap-4">
                <Link href="/dashboard">
                  <button className="bg-[#E65100] hover:bg-[#FF6D00] text-white text-lg font-bold px-8 py-4 rounded-full flex items-center gap-3 transition-all shadow-lg hover:shadow-orange-500/40 hover:-translate-y-1">
                    <Mic className="animate-pulse" /> Speak to Search
                  </button>
                </Link>

                <a
                  href="#schemes"
                  className="bg-white/10 hover:bg-white/20 border border-white text-white text-lg font-bold px-8 py-4 rounded-full flex items-center gap-3 transition-all backdrop-blur-sm hover:-translate-y-1"
                >
                  Explore Schemes <ArrowRight size={20} />
                </a>
              </div>
            </motion.div>
          </div>
        </div>
      </section>

      <div className="bg-[#3E2723] text-white py-12 border-b-4 border-orange-600">
        <div className="max-w-7xl mx-auto px-4 grid grid-cols-1 md:grid-cols-3 gap-8 text-center divide-y md:divide-y-0 md:divide-x divide-white/10">
          <AnimatedStat icon={FileText} value={120} label="Schemes Listed" suffix="+" />
          <AnimatedStat icon={Users} value={15000} label="Citizens Helped" suffix="+" />
          <AnimatedStat icon={IndianRupee} value={7} label="Benefits Unlocked" suffix=" Cr+" />
        </div>
      </div>

      <section className="py-24 bg-white">
        <div className="max-w-6xl mx-auto px-4 text-center">
          <div className="mb-12">
            <h2 className="text-4xl font-bold text-slate-900 mb-4">A Glimpse of Change</h2>
            <p className="text-slate-500 max-w-2xl mx-auto">
              See how technology is impacting lives across rural India.
            </p>
          </div>

          <div className="relative w-full aspect-video bg-slate-900 rounded-2xl overflow-hidden shadow-2xl group">
            <AnimatePresence mode='wait'>
              <motion.div
                key={currentSlide}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.8 }}
                className="absolute inset-0"
              >
                <div
                  className="absolute inset-0 bg-cover bg-center bg-no-repeat transition-transform duration-4000 ease-in-out transform scale-100 group-hover:scale-105"
                  style={{ backgroundImage: `url(${HERO_SLIDES[currentSlide].image})` }}
                />
                <div className="absolute inset-0 bg-linear-to-t from-black/90 via-black/40 to-transparent" />
                <div className="absolute bottom-0 left-0 w-full p-8 md:p-12 text-left">
                  <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3 }}>
                    <h3 className={`text-3xl md:text-5xl font-bold mb-3 ${HERO_SLIDES[currentSlide].accent}`}>
                      {HERO_SLIDES[currentSlide].title}
                    </h3>
                    <p className="text-white/90 text-lg md:text-xl max-w-2xl">
                      {HERO_SLIDES[currentSlide].subtitle}
                    </p>
                  </motion.div>
                </div>
              </motion.div>
            </AnimatePresence>

            <div className="absolute bottom-8 right-8 flex gap-2 z-20">
              {HERO_SLIDES.map((_, index) => (
                <div
                  key={index}
                  className={`h-2 rounded-full transition-all duration-300 cursor-pointer shadow-sm ${index === currentSlide ? 'w-8 bg-orange-500' : 'w-2 bg-white/50 hover:bg-white'}`}
                  onClick={() => setCurrentSlide(index)}
                />
              ))}
            </div>
          </div>
        </div>
      </section>

      <section id="schemes" className="py-24 bg-[#FFFBF2] scroll-mt-32">
        <div className="max-w-7xl mx-auto px-4">
          <div className="text-center mb-16">
            <h2 className="text-4xl font-bold text-slate-900 mb-4">Our Programmes</h2>
            <p className="text-xl text-slate-500 max-w-2xl mx-auto">
              Holistic development through targeted welfare schemes.
            </p>
          </div>

          <motion.div layout className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            <AnimatePresence>
              {PROGRAMMES.slice(0, showAllProgrammes ? PROGRAMMES.length : 4).map((prog) => (
                <ProgrammeCard
                  key={prog.id}
                  title={prog.title}
                  desc={prog.desc}
                  color={prog.color}
                  icon={prog.icon}
                />
              ))}
            </AnimatePresence>
          </motion.div>

          <div className="mt-12 text-center">
            <button
              onClick={() => setShowAllProgrammes(!showAllProgrammes)}
              className="inline-flex items-center gap-2 px-8 py-3 border-2 border-slate-900 text-slate-900 font-bold rounded-full hover:bg-slate-900 hover:text-white transition-all"
            >
              {showAllProgrammes ? 'Show Less' : 'View All Categories'}
              {showAllProgrammes ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
            </button>
          </div>
        </div>
      </section>

      <section className="py-24 bg-white overflow-hidden">
        <div className="mb-16 px-4">
          <h2 className="text-3xl md:text-5xl font-bold text-center text-slate-900 mb-4">
            Stories of Hope
          </h2>
          <p className="text-center text-slate-500">Voices from across the state.</p>
        </div>

        <div className="relative w-full">
          <div className="absolute left-0 top-0 bottom-0 w-20 bg-linear-to-r from-white to-transparent z-10 pointer-events-none" />
          <div className="absolute right-0 top-0 bottom-0 w-20 bg-linear-to-l from-white to-transparent z-10 pointer-events-none" />

          <div className="flex overflow-hidden">
            <motion.div
              className="flex gap-8 px-4"
              animate={{ x: ["0%", "-50%"] }}
              transition={{ repeat: Infinity, ease: "linear", duration: 30 }}
              whileHover={{ animationPlayState: "paused" }}
            >
              {[...TESTIMONIALS, ...TESTIMONIALS].map((item, index) => (
                <div key={index} className="w-87.5 md:w-100 shrink-0">
                  <TestimonialCard {...item} />
                </div>
              ))}
            </motion.div>
          </div>
        </div>
      </section>

      <section id="faqs" className="py-8 bg-[#FFFBF2] scroll-mt-32">
        <div className="max-w-3xl mx-auto px-4">
          <h2 className="text-xl font-bold text-center mb-4 text-slate-900">
            Common Questions
          </h2>

          <div className="space-y-2">
            <FaqItem q="Is Awaaz free to use?" a="Yes, searching for schemes and checking eligibility is completely free for all citizens." />
            <FaqItem q="Do I need internet to use the app?" a="Yes, an active internet connection is required to access the latest database of schemes and use the AI voice features." />
            <FaqItem q="What languages are supported?" a="Currently we support Hindi, Bhojpuri, Maithili, and English. More languages coming soon." />
            <FaqItem q="Who can use Awaaz?" a="Awaaz is designed for all Indian citizens, especially farmers, workers, students, women, and senior citizens looking for government benefits." />
            <FaqItem q="Is my personal data safe?" a="Yes, we take privacy seriously. Your information is used only to show relevant schemes and is not shared without your consent." />
            <FaqItem q="Can I apply for schemes directly through Awaaz?" a="Currently, Awaaz helps you discover schemes and understand the application process. Direct in-app applications will be added soon." />
            <FaqItem q="Are state-level schemes included?" a="Yes, Awaaz includes both central and state government schemes, based on your location." />
            <FaqItem q="How often is the scheme information updated?" a="Our database is updated regularly to ensure you get the latest and most accurate scheme information." />
          </div>
        </div>
      </section>
    </main>
  );
}

const AnimatedStat = ({ icon: Icon, value, label, suffix }: any) => {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: "-100px" });
  const motionValue = useMotionValue(0);
  const springValue = useSpring(motionValue, { duration: 3000 });
  const [displayValue, setDisplayValue] = useState(0);

  useEffect(() => {
    if (isInView) {
      motionValue.set(value);
    }
  }, [isInView, value, motionValue]);

  useEffect(() => {
    springValue.on("change", (latest) => {
      setDisplayValue(Math.floor(latest));
    });
  }, [springValue]);

  return (
    <div ref={ref} className="p-4 flex flex-col items-center justify-center">
      <div className="mb-4 text-orange-400 opacity-80">
        <Icon size={40} />
      </div>
      <div className="text-5xl font-bold mb-2">
        {displayValue}{suffix}
      </div>
      <div className="text-orange-100/70 font-medium uppercase tracking-widest text-sm">
        {label}
      </div>
    </div>
  );
};

const ProgrammeCard = ({ title, desc, color, count, icon: Icon }: any) => (
  <Link href={`/schemes/${title}`}>
    <motion.div
      layout
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.9 }}
      transition={{ duration: 0.3 }}
      className="group bg-white rounded-xl overflow-hidden shadow-md hover:shadow-2xl transition-all duration-300 cursor-pointer hover:-translate-y-2"
    >
      <div className={`h-40 ${color} relative flex items-center justify-center`}>
        <div className="absolute inset-0 bg-black/10 group-hover:bg-black/0 transition-all" />
        <Icon className="text-white/80 group-hover:text-white group-hover:scale-110 transition-transform duration-500" size={64} />
        <div className="absolute bottom-0 left-0 w-full p-4 bg-linear-to-t from-black/50 to-transparent">
          <span className="text-white font-bold text-sm tracking-wide">{count}</span>
        </div>
      </div>
      <div className="p-6">
        <h3 className="text-xl font-bold text-slate-900 mb-2 group-hover:text-orange-600 transition-colors">
          {title}
        </h3>
        <p className="text-slate-600 text-sm leading-relaxed mb-4">
          {desc}
        </p>
        <div className="flex items-center text-orange-600 font-bold text-sm gap-1">
          Read More <ChevronRight size={16} className="group-hover:translate-x-1 transition-transform" />
        </div>
      </div>
    </motion.div>
  </Link>
);

const TestimonialCard = ({ name, location, role, quote }: any) => (
  <div className="bg-slate-50 p-8 rounded-2xl shadow-md border border-slate-100 flex flex-col h-full hover:bg-white hover:shadow-xl hover:border-orange-200 transition-all duration-300">
    <div className="mb-6">
      <Quote size={40} className="text-orange-300 rotate-180" />
    </div>
    <p className="text-slate-700 text-lg italic mb-6 grow leading-relaxed">"{quote}"</p>
    <div className="mt-auto border-t border-slate-200 pt-4">
      <div className="flex items-center justify-between">
        <div>
          <h5 className="font-bold text-slate-900 text-lg">{name}</h5>
          <p className="text-xs text-slate-500 uppercase tracking-wide font-bold">{role} • {location}</p>
        </div>
        <div className="opacity-20 text-orange-600">
          <HeartHandshake size={32} />
        </div>
      </div>
    </div>
  </div>
);

const FaqItem = ({ q, a }: any) => (
  <details className="group bg-white rounded-md p-3 cursor-pointer border border-slate-200 shadow-sm open:ring-1 open:ring-orange-100 transition-all">
    <summary className="flex justify-between items-center font-semibold text-slate-800 list-none text-sm">
      {q}
      <span className="transition group-open:rotate-180 bg-slate-100 rounded-full p-0.5">
        <ArrowRight size={12} className="rotate-90 text-orange-500" />
      </span>
    </summary>
    <p className="text-slate-600 mt-2 leading-relaxed text-sm pl-2 border-l-2 border-orange-500 ml-1">
      {a}
    </p>
  </details>
);
