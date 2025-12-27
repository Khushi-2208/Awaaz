'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import Image from 'next/image';
import { motion } from 'framer-motion';
import { Menu, X, ChevronRight } from 'lucide-react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export default function Navbar() {
  const [isScrolled, setIsScrolled] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const navLinks = [
    { label: 'Home', href: '/' },
    { label: 'About', href: '/about' },
    { label: 'FAQs', href: '/#faqs' },
    { label: 'Contact', href: '/#contact' },
  ];

  useEffect(() => {
    const handleScroll = () => setIsScrolled(window.scrollY > 20);
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  return (
    <nav
      className={cn(
        "fixed top-0 w-full z-50 transition-all duration-300 border-b border-transparent",
        isScrolled
          ? "bg-white/80 backdrop-blur-md shadow-sm border-gray-200 py-3"
          : "bg-transparent py-5"
      )}
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex justify-between items-center">
        
        {/* Logo */}
        <Link href="/" className="flex items-center gap-3 group">
          <motion.div
            whileHover={{ scale: 1.08, rotate: 2 }}
            transition={{ type: 'spring', stiffness: 300 }}
            className={cn(
              "relative w-14 h-14 rounded-full p-0.5",
              isScrolled
                ? "bg-linear-to-br from-blue-600 to-indigo-600 shadow-md"
                : "bg-linear-to-br from-white/80 to-white/40 shadow-lg"
            )}
          >
            <div className="relative w-full h-full rounded-full bg-white flex items-center justify-center overflow-hidden">
              <Image
                src="/logo.png"
                alt="Awaaz Logo"
                fill
                className="object-contain p-2"
                priority
              />
            </div>
          </motion.div>

          <span
            className={cn(
              "font-bold text-2xl tracking-tight transition-colors",
              isScrolled ? "text-blue-900" : "text-white"
            )}
          >
            Awaaz
          </span>
        </Link>

        {/* Desktop Nav */}
        <div className="hidden md:flex items-center space-x-8">
          {navLinks.map((item) => (
            <Link
              key={item.label}
              href={item.href}
              className={cn(
                "text-sm font-medium transition-colors hover:text-orange-500",
                isScrolled ? "text-gray-600" : "text-gray-200"
              )}
            >
              {item.label}
            </Link>
          ))}

          <Link href="/auth">
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2.5 rounded-full font-semibold shadow-md transition-all flex items-center gap-2"
            >
              Login / Signup <ChevronRight size={16} />
            </motion.button>
          </Link>
        </div>

        {/* Mobile Toggle */}
        <button
          onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
          className={cn("md:hidden p-2", isScrolled ? "text-gray-900" : "text-white")}
        >
          {mobileMenuOpen ? <X /> : <Menu />}
        </button>
      </div>

      {/* Mobile Menu */}
      {mobileMenuOpen && (
        <motion.div
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          className="md:hidden bg-white border-t border-gray-100 absolute w-full left-0 shadow-xl"
        >
          <div className="flex flex-col p-4 space-y-4">
            {navLinks.map((item) => (
              <Link
                key={item.label}
                href={item.href}
                className="text-gray-800 font-medium text-lg px-2 py-2 hover:bg-gray-50 rounded-lg"
                onClick={() => setMobileMenuOpen(false)}
              >
                {item.label}
              </Link>
            ))}

            <Link href="/auth" onClick={() => setMobileMenuOpen(false)}>
              <button className="w-full bg-blue-600 text-white py-3 rounded-lg font-bold mt-2">
                Login / Signup
              </button>
            </Link>
          </div>
        </motion.div>
      )}
    </nav>
  );
}
