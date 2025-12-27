'use client';

import React from 'react';
import { motion } from 'framer-motion';
import {
  Mic,
  Users,
  ShieldCheck,
  Globe,
  HeartHandshake,
  Building2
} from 'lucide-react';

export default function AboutPage() {
  return (
    <main className="bg-[#FFFBF2] text-slate-900 font-sans">

      {/* --- HERO --- */}
      <section className="py-28 bg-slate-900 text-white relative overflow-hidden">
        <div className="absolute inset-0 bg-linear-to-br from-black/80 to-black/40" />

        <div className="relative z-10 max-w-5xl mx-auto px-6 text-center">
          <motion.h1
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8 }}
            className="text-5xl md:text-6xl font-bold mb-6"
          >
            About Aawaz
          </motion.h1>

          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-xl text-white/90 max-w-3xl mx-auto leading-relaxed"
          >
            Aawaz exists to ensure that every citizen—regardless of literacy,
            language, or location—can access their rightful government benefits
            using just their voice.
          </motion.p>
        </div>
      </section>

      {/* --- OUR STORY --- */}
      <section className="py-24 bg-white">
        <div className="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-16 items-center">
          
          <motion.div
            initial={{ opacity: 0, x: -30 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.6 }}
          >
            <h2 className="text-4xl font-bold mb-6">Our Story</h2>
            <p className="text-slate-600 leading-relaxed mb-4">
              Millions of welfare schemes exist in India, yet crores of citizens
              remain unaware or unable to access them due to complex forms,
              language barriers, and digital literacy challenges.
            </p>
            <p className="text-slate-600 leading-relaxed">
              Aawaz was built to change that. By combining AI, voice technology,
              and localized languages, we bring government schemes directly to
              people—no paperwork confusion, no intermediaries.
            </p>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, x: 30 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.6 }}
            className="bg-[#FFF3E0] rounded-3xl p-10 shadow-lg"
          >
            <ul className="space-y-6">
              <StoryPoint icon={Mic} text="Voice-first access to schemes" />
              <StoryPoint icon={Globe} text="Regional language support" />
              <StoryPoint icon={ShieldCheck} text="Trusted & verified information" />
              <StoryPoint icon={Users} text="Built for rural & underserved India" />
            </ul>
          </motion.div>

        </div>
      </section>

      {/* --- MISSION & VALUES --- */}
      <section className="py-24 bg-[#FFFBF2]">
        <div className="max-w-6xl mx-auto px-6 text-center mb-16">
          <h2 className="text-4xl font-bold mb-4">Our Mission</h2>
          <p className="text-xl text-slate-600 max-w-3xl mx-auto">
            To democratize access to government welfare by making information
            simple, inclusive, and voice-enabled.
          </p>
        </div>

        <div className="max-w-6xl mx-auto px-6 grid md:grid-cols-3 gap-8">
          <ValueCard
            icon={HeartHandshake}
            title="Inclusivity"
            desc="Designed for every citizen, regardless of literacy or language."
          />
          <ValueCard
            icon={ShieldCheck}
            title="Trust"
            desc="Accurate, verified, and transparent scheme information."
          />
          <ValueCard
            icon={Building2}
            title="Impact"
            desc="Real outcomes that improve livelihoods and dignity allowing citizens to access benefits."
          />
        </div>
      </section>

      {/* --- IMPACT --- */}
      <section className="py-24 bg-slate-900 text-white">
        <div className="max-w-6xl mx-auto px-6 text-center">
          <h2 className="text-4xl font-bold mb-6">Our Impact So Far</h2>

          <div className="grid md:grid-cols-3 gap-10 mt-12">
            <ImpactStat value="15,000+" label="Citizens Helped" />
            <ImpactStat value="120+" label="Schemes Covered" />
            <ImpactStat value="₹7 Cr+" label="Benefits Unlocked" />
          </div>
        </div>
      </section>

    </main>
  );
}

/* --- SUB COMPONENTS --- */

const StoryPoint = ({ icon: Icon, text }: any) => (
  <li className="flex items-center gap-4">
    <div className="bg-orange-500/10 text-orange-600 p-3 rounded-full">
      <Icon size={24} />
    </div>
    <span className="font-semibold text-slate-800">{text}</span>
  </li>
);

const ValueCard = ({ icon: Icon, title, desc }: any) => (
  <motion.div
    whileHover={{ y: -6 }}
    className="bg-white rounded-2xl p-8 shadow-md hover:shadow-xl transition-all"
  >
    <div className="text-orange-500 mb-4">
      <Icon size={40} />
    </div>
    <h3 className="text-xl font-bold mb-2">{title}</h3>
    <p className="text-slate-600 leading-relaxed">{desc}</p>
  </motion.div>
);

const ImpactStat = ({ value, label }: any) => (
  <div>
    <div className="text-5xl font-bold text-orange-400 mb-2">{value}</div>
    <div className="uppercase tracking-widest text-sm text-white/70">
      {label}
    </div>
  </div>
);
