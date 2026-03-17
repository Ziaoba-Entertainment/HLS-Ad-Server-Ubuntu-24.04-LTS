import React, { useState, useEffect } from 'react';
import { 
  Play, 
  Settings, 
  BarChart3, 
  Activity, 
  Server, 
  Video, 
  Database, 
  CheckCircle2, 
  AlertCircle,
  ExternalLink,
  RefreshCw,
  ShieldCheck
} from 'lucide-react';
import { motion } from 'motion/react';

interface StatusData {
  status: string;
  service: string;
  version?: string;
  redis_status?: string;
  db_status?: string;
  active_ads?: number;
  total_ads?: number;
  total_impressions?: number;
  uptime_seconds?: number;
}

const App: React.FC = () => {
  const [adServerStatus, setAdServerStatus] = useState<StatusData | null>(null);
  const [adminStatus, setAdminStatus] = useState<StatusData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = async () => {
    setLoading(true);
    try {
      const [adRes, adminRes] = await Promise.all([
        fetch('/api/status').then(r => r.ok ? r.json() : null).catch(() => null),
        fetch('/admin/api/status').then(r => r.ok ? r.json() : null).catch(() => null)
      ]);
      setAdServerStatus(adRes);
      setAdminStatus(adminRes);
      setError(null);
    } catch (err) {
      setError("Failed to connect to backend services.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(fetchStatus, 30000);
    return () => clearInterval(interval);
  }, []);

  const StatusBadge = ({ status }: { status?: string }) => {
    const isOk = status === 'ok';
    return (
      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
        isOk ? 'bg-emerald-100 text-emerald-800' : 'bg-rose-100 text-rose-800'
      }`}>
        {isOk ? <CheckCircle2 className="w-3 h-3 mr-1" /> : <AlertCircle className="w-3 h-3 mr-1" />}
        {status || 'unknown'}
      </span>
    );
  };

  return (
    <div className="min-h-screen bg-[#f5f5f0] text-[#1a1a1a] font-serif">
      {/* Header */}
      <header className="border-b border-black/10 bg-white/50 backdrop-blur-md sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-6 py-4 flex justify-between items-center">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-black rounded-xl flex items-center justify-center">
              <Server className="text-white w-6 h-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight font-sans">Stream-Ziaoba</h1>
              <p className="text-xs text-black/50 font-sans uppercase tracking-widest">Unified Media Control</p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <button 
              onClick={fetchStatus}
              className="p-2 hover:bg-black/5 rounded-full transition-colors"
              title="Refresh Status"
            >
              <RefreshCw className={`w-5 h-5 ${loading ? 'animate-spin' : ''}`} />
            </button>
            <div className="h-6 w-px bg-black/10" />
            <div className="flex items-center gap-2">
              <span className="text-xs font-sans font-medium opacity-50 uppercase">System Status:</span>
              <StatusBadge status={adServerStatus?.status} />
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-12">
        <motion.div 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="grid grid-cols-1 lg:grid-cols-3 gap-8"
        >
          {/* Main Integration Card */}
          <div className="lg:col-span-2 space-y-8">
            <section className="bg-white rounded-3xl p-8 shadow-sm border border-black/5">
              <div className="flex items-center justify-between mb-8">
                <h2 className="text-3xl font-light italic">Integration Overview</h2>
                <ShieldCheck className="text-emerald-500 w-8 h-8" />
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="p-6 rounded-2xl bg-[#f9f9f7] border border-black/5">
                  <div className="flex items-center gap-3 mb-4">
                    <Activity className="text-indigo-500" />
                    <h3 className="font-sans font-semibold">Ad Stitching Middleware</h3>
                  </div>
                  <div className="space-y-3 text-sm font-sans">
                    <div className="flex justify-between">
                      <span className="opacity-50">Status</span>
                      <StatusBadge status={adServerStatus?.status} />
                    </div>
                    <div className="flex justify-between">
                      <span className="opacity-50">Redis (DB 1)</span>
                      <StatusBadge status={adServerStatus?.redis_status} />
                    </div>
                    <div className="flex justify-between">
                      <span className="opacity-50">Active Ads</span>
                      <span className="font-mono font-bold">{adServerStatus?.active_ads ?? 0}</span>
                    </div>
                  </div>
                  <div className="mt-6">
                    <a 
                      href="/stream/movies/test/master.m3u8" 
                      target="_blank"
                      className="inline-flex items-center gap-2 text-xs font-bold uppercase tracking-wider text-indigo-600 hover:text-indigo-700"
                    >
                      Test Stream <ExternalLink className="w-3 h-3" />
                    </a>
                  </div>
                </div>

                <div className="p-6 rounded-2xl bg-[#f9f9f7] border border-black/5">
                  <div className="flex items-center gap-3 mb-4">
                    <Database className="text-amber-500" />
                    <h3 className="font-sans font-semibold">Admin Dashboard</h3>
                  </div>
                  <div className="space-y-3 text-sm font-sans">
                    <div className="flex justify-between">
                      <span className="opacity-50">Status</span>
                      <StatusBadge status={adminStatus?.status} />
                    </div>
                    <div className="flex justify-between">
                      <span className="opacity-50">Total Impressions</span>
                      <span className="font-mono font-bold">{adminStatus?.total_impressions ?? 0}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="opacity-50">Uptime</span>
                      <span className="font-mono">{adminStatus?.uptime_seconds ? `${Math.floor(adminStatus.uptime_seconds / 3600)}h ${Math.floor((adminStatus.uptime_seconds % 3600) / 60)}m` : 'N/A'}</span>
                    </div>
                  </div>
                  <div className="mt-6">
                    <a 
                      href="/admin/" 
                      target="_blank"
                      className="inline-flex items-center gap-2 text-xs font-bold uppercase tracking-wider text-amber-600 hover:text-amber-700"
                    >
                      Open Admin UI <ExternalLink className="w-3 h-3" />
                    </a>
                  </div>
                </div>
              </div>

              <div className="mt-8 p-6 rounded-2xl bg-black text-white">
                <div className="flex items-center gap-3 mb-4">
                  <Video className="text-emerald-400" />
                  <h3 className="font-sans font-semibold">Transcoder Pipeline</h3>
                </div>
                <p className="text-sm opacity-70 mb-6 font-sans">
                  The hardware-accelerated transcoding engine is active. All ad plays are reported to the central registry for campaign tracking.
                </p>
                <div className="flex gap-4">
                  <a 
                    href="/transcoder/" 
                    target="_blank"
                    className="px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg text-xs font-bold uppercase tracking-widest transition-colors flex items-center gap-2"
                  >
                    Transcoder UI <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
              </div>
            </section>

            <section className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="bg-white p-6 rounded-3xl shadow-sm border border-black/5">
                <div className="w-10 h-10 bg-indigo-50 rounded-xl flex items-center justify-center mb-4">
                  <Play className="text-indigo-600 w-5 h-5" />
                </div>
                <h4 className="font-sans font-bold text-sm mb-1">Movies</h4>
                <p className="text-xs opacity-50 font-sans">/srv/vod/hls/movies</p>
              </div>
              <div className="bg-white p-6 rounded-3xl shadow-sm border border-black/5">
                <div className="w-10 h-10 bg-amber-50 rounded-xl flex items-center justify-center mb-4">
                  <BarChart3 className="text-amber-600 w-5 h-5" />
                </div>
                <h4 className="font-sans font-bold text-sm mb-1">TV Shows</h4>
                <p className="text-xs opacity-50 font-sans">/srv/vod/hls/tv</p>
              </div>
              <div className="bg-white p-6 rounded-3xl shadow-sm border border-black/5">
                <div className="w-10 h-10 bg-emerald-50 rounded-xl flex items-center justify-center mb-4">
                  <Settings className="text-emerald-600 w-5 h-5" />
                </div>
                <h4 className="font-sans font-bold text-sm mb-1">Ad Assets</h4>
                <p className="text-xs opacity-50 font-sans">/srv/vod/ads</p>
              </div>
            </section>
          </div>

          {/* Sidebar Info */}
          <div className="space-y-8">
            <section className="bg-white rounded-3xl p-8 shadow-sm border border-black/5">
              <h3 className="text-xl font-medium mb-6">System Manual</h3>
              <div className="space-y-4 font-sans text-sm">
                <div className="pb-4 border-b border-black/5">
                  <p className="font-bold mb-1">HLS Delivery</p>
                  <p className="opacity-60">Standardized 4-layer ABR stack with 6s segments.</p>
                </div>
                <div className="pb-4 border-b border-black/5">
                  <p className="font-bold mb-1">Ad Stitching</p>
                  <p className="opacity-60">Dynamic manifest manipulation via port 8083.</p>
                </div>
                <div className="pb-4 border-b border-black/5">
                  <p className="font-bold mb-1">Reporting</p>
                  <p className="opacity-60">Real-time play tracking integrated with Transcoder API.</p>
                </div>
                <div>
                  <p className="font-bold mb-1">Redis Registry</p>
                  <p className="opacity-60">DB 1 used for ad metadata and rotation logic.</p>
                </div>
              </div>
            </section>

            <section className="bg-indigo-600 rounded-3xl p-8 shadow-sm text-white">
              <h3 className="text-xl font-medium mb-4">Quick Links</h3>
              <ul className="space-y-3 font-sans text-sm">
                <li><a href="/sonarr/" target="_blank" className="hover:underline flex items-center justify-between">Sonarr <ExternalLink className="w-3 h-3" /></a></li>
                <li><a href="/radarr/" target="_blank" className="hover:underline flex items-center justify-between">Radarr <ExternalLink className="w-3 h-3" /></a></li>
                <li><a href="/prowlarr/" target="_blank" className="hover:underline flex items-center justify-between">Prowlarr <ExternalLink className="w-3 h-3" /></a></li>
              </ul>
            </section>
          </div>
        </motion.div>
      </main>

      {error && (
        <div className="fixed bottom-6 right-6 bg-rose-600 text-white px-6 py-3 rounded-2xl shadow-lg flex items-center gap-3 font-sans animate-bounce">
          <AlertCircle className="w-5 h-5" />
          <span className="text-sm font-medium">{error}</span>
        </div>
      )}
    </div>
  );
};

export default App;
