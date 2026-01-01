"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import {
  GoogleAuthProvider,
  signInWithPopup,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendPasswordResetEmail,
} from "firebase/auth";
import { auth } from "@/lib/firebase";
import { Mail, Lock, ArrowRight, ShieldCheck, Eye, EyeOff } from "lucide-react";

export default function AuthPage() {
  const router = useRouter();

  const [showPassword, setShowPassword] = useState(false);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isSignup, setIsSignup] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const handleGoogleLogin = async () => {
    setError("");
    setMessage("");
    setLoading(true);
    try {
      const provider = new GoogleAuthProvider();
      await signInWithPopup(auth, provider);
      router.push("/");
    } catch {
      setError("Google login failed. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const handleEmailAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setMessage("");
    setLoading(true);
    try {
      if (isSignup) {
        await createUserWithEmailAndPassword(auth, email, password);
      } else {
        await signInWithEmailAndPassword(auth, email, password);
      }
      router.push("/");
    } catch {
      setError("Invalid email or password.");
    } finally {
      setLoading(false);
    }
  };

  const handleForgotPassword = async () => {
    setError("");
    setMessage("");

    if (!email) {
      setError("Please enter your email first.");
      return;
    }

    try {
      await sendPasswordResetEmail(auth, email);
      setMessage("Password reset link sent to your email.");
    } catch {
      setError("Failed to send reset email. Check the address.");
    }
  };

  const handleGuest = () => {
    localStorage.setItem("guest", "true");
    router.push("/");
  };

  return (
    <main className="min-h-screen bg-[#FFFBF2] pt-32 px-4">
      <div className="max-w-5xl mx-auto grid grid-cols-1 md:grid-cols-2 gap-10 items-start">

        {/* LEFT */}
        <div className="hidden md:block">
          <h1 className="text-4xl font-extrabold text-slate-900 mb-4">
            Welcome to Awaaz
          </h1>
          <p className="text-slate-700 text-lg leading-relaxed mb-6">
            Awaaz helps citizens discover and understand government schemes
            using voice — without complicated forms or technical language.
          </p>

          <ul className="space-y-4 text-slate-700">
            <li className="flex items-center gap-3">
              <ShieldCheck className="text-orange-600" />
              No spam. No ads. No data selling.
            </li>
            <li className="flex items-center gap-3">
              <ShieldCheck className="text-orange-600" />
              Login is optional — explore freely.
            </li>
            <li className="flex items-center gap-3">
              <ShieldCheck className="text-orange-600" />
              Secure sign-in powered by Google.
            </li>
          </ul>
        </div>

        {/* RIGHT */}
        <div className="bg-white rounded-3xl shadow-xl border border-orange-200 p-8">
          <h2 className="text-2xl font-bold text-slate-900 mb-1">
            {isSignup ? "Create your account" : "Sign in to Awaaz"}
          </h2>
          <p className="text-slate-600 text-sm mb-6">
            Login only if you want to save visited schemes.
          </p>

          <button
            onClick={handleGoogleLogin}
            disabled={loading}
            className="w-full flex items-center justify-center gap-3 border border-slate-300 py-3 rounded-full font-semibold text-slate-800 hover:bg-slate-50 transition mb-5"
          >
            Continue with Google
          </button>

          <div className="flex items-center gap-3 my-6">
            <div className="flex-1 h-px bg-slate-200" />
            <span className="text-xs text-slate-400 font-semibold">OR</span>
            <div className="flex-1 h-px bg-slate-200" />
          </div>

          <form onSubmit={handleEmailAuth} className="space-y-4">
            <div className="relative">
              <Mail className="absolute left-4 top-3.5 text-slate-400" size={18} />
              <input
                type="email"
                name="email"
                placeholder="Email address"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                autoComplete="email"
                className="w-full bg-slate-50 border border-slate-300 rounded-full px-12 py-3 text-slate-900 outline-none focus:ring-2 focus:ring-orange-300"
              />
            </div>

            <div className="relative">
              <Lock className="absolute left-4 top-3.5 text-slate-400" size={18} />
              <input
                type={showPassword ? "text" : "password"}
                name="password"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete={isSignup ? "new-password" : "current-password"}
                className="w-full bg-slate-50 border border-slate-300 rounded-full px-12 pr-12 py-3 text-slate-900 outline-none focus:ring-2 focus:ring-orange-300"
              />

              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-4 top-3.5 text-slate-400 hover:text-slate-600"
              >
                {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
              </button>
            </div>

            {!isSignup && (
              <div className="text-right">
                <button
                  type="button"
                  onClick={handleForgotPassword}
                  className="text-sm text-orange-600 hover:underline font-semibold"
                >
                  Forgot password?
                </button>
              </div>
            )}

            <button
              type="submit"
              disabled={loading}
              className="w-full bg-[#E65100] hover:bg-[#FF6D00] text-white py-3 rounded-full font-bold transition shadow-md"
            >
              {isSignup ? "Create Account" : "Login"}
            </button>
          </form>

          <p className="text-center text-sm text-slate-600 mt-5">
            {isSignup ? "Already have an account?" : "New here?"}{" "}
            <button
              onClick={() => setIsSignup(!isSignup)}
              className="text-orange-600 font-bold"
            >
              {isSignup ? "Login" : "Create account"}
            </button>
          </p>

          <div className="mt-8 text-center">
            <button
              onClick={handleGuest}
              className="inline-flex items-center gap-2 text-sm text-slate-600 hover:text-orange-600 font-medium"
            >
              Continue without login <ArrowRight size={14} />
            </button>
          </div>

          {error && (
            <p className="mt-4 text-center text-sm text-red-600">{error}</p>
          )}

          {message && (
            <p className="mt-4 text-center text-sm text-green-600">{message}</p>
          )}
        </div>
      </div>
    </main>
  );
}
