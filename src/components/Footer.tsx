import React from 'react';
import Link from 'next/link';
import Image from 'next/image';
import { Facebook, Instagram, Linkedin, Mail, Phone, X } from 'lucide-react';

export default function Footer() {
  return (
    <footer
      id="contact"
      className="bg-slate-900 text-white pt-20 pb-10 border-t border-slate-800 scroll-mt-32"
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 grid grid-cols-1 md:grid-cols-4 gap-12 mb-16">
        
        <div className="col-span-1 md:col-span-2">
          <div className="flex items-center gap-3 mb-6">
            
            <div className="relative w-10 h-10 rounded-full bg-white flex items-center justify-center overflow-hidden">
              <Image
                src="/logo.png"
                alt="Awaaz Logo"
                fill
                className="object-contain p-1.5"
              />
            </div>

            <span className="text-2xl font-bold">Awaaz</span>
          </div>

          <p className="text-slate-400 max-w-sm text-lg leading-relaxed mb-6">
            Empowering the elderly and illiterate through voice-first technology.
            Built for <strong>TechSprint 25 @ NIT Patna</strong>.
          </p>

          {/* Social Media Links */}
          <div className="flex gap-4">
            <SocialLink href="https://x.com" icon={<X size={20} />} />
            <SocialLink href="https://facebook.com" icon={<Facebook size={20} />} />
            <SocialLink href="https://instagram.com" icon={<Instagram size={20} />} />
            <SocialLink href="https://linkedin.com" icon={<Linkedin size={20} />} />
          </div>
        </div>

        {/* Quick Links */}
        <div>
          <h4 className="text-lg font-bold mb-6 text-white">Quick Links</h4>
          <ul className="space-y-4 text-slate-400">
            <li>
              <Link
                href="/about"
                className="hover:text-orange-500 transition-colors"
              >
                About
              </Link>
            </li>
            <li>
              <Link
                href="/#schemes"
                className="hover:text-orange-500 transition-colors"
              >
                Schemes
              </Link>
            </li>
          </ul>
        </div>

        {/* Contact Info */}
        <div>
          <h4 className="text-lg font-bold mb-6 text-white">Contact</h4>
          <ul className="space-y-4 text-slate-400">
            <li className="flex items-center gap-3">
              <Mail size={18} className="text-orange-500" />
              <span>help@awaaz.in</span>
            </li>
            <li className="flex items-center gap-3">
              <Phone size={18} className="text-orange-500" />
              <span>+91 1800-123-4567</span>
            </li>
            <li className="mt-4 text-sm text-slate-500">
              NIT Patna Campus,<br />
              Ashok Rajpath, Patna, Bihar 800005
            </li>
          </ul>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 pt-8 border-t border-slate-800 flex flex-col md:flex-row justify-between items-center text-slate-500 text-sm">
        <p>© 2025 Awaaz Project. Open Source License.</p>
        <p>Made with ❤️ for India.</p>
      </div>
    </footer>
  );
}


const SocialLink = ({
  href,
  icon,
}: {
  href: string;
  icon: React.ReactNode;
}) => (
  <a
    href={href}
    target="_blank"
    rel="noopener noreferrer"
    className="w-10 h-10 rounded-full bg-slate-800 flex items-center justify-center hover:bg-orange-500 hover:text-white transition-colors cursor-pointer"
  >
    {icon}
  </a>
);
