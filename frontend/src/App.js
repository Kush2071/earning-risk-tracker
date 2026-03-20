/* eslint-disable react-hooks/exhaustive-deps */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, BarChart, Bar, Cell, ReferenceLine } from 'recharts';
import axios from 'axios';

const API = 'https://earning-risk-tracker-production.up.railway.app';
const DEFAULT_WATCHLIST = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN'];
const REFRESH_INTERVAL  = 40000;

const COLORS = {
  positive: '#1D9E75',
  negative: '#D85A30',
  neutral:  '#888780',
  blue:     '#378ADD',
  amber:    '#BA7517',
  purple:   '#7F77DD',
};

function SentimentBar({ score }) {
  const color = score > 0.05 ? COLORS.positive : score < -0.05 ? COLORS.negative : COLORS.neutral;
  const width = Math.min(Math.abs(score) * 300, 100);
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <div style={{ width: 100, height: 7, background: '#eee', borderRadius: 4, overflow: 'hidden' }}>
        <div style={{ width: `${width}%`, height: '100%', background: color, borderRadius: 4 }} />
      </div>
      <span style={{ fontSize: 11, color }}>{score > 0 ? '+' : ''}{score?.toFixed(3)}</span>
    </div>
  );
}

function MiniBar({ value, max, color, bg }) {
  return (
    <div style={{ width: '100%', height: 6, background: bg || '#eee', borderRadius: 3, overflow: 'hidden' }}>
      <div style={{ width: `${Math.min((value / max) * 100, 100)}%`, height: '100%', background: color, borderRadius: 3 }} />
    </div>
  );
}

function isMarketOpen() {
  const now = new Date();
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  const hours = now.getHours();
  const minutes = now.getMinutes();
  const totalMinutes = hours * 60 + minutes;
  const marketOpen  = 9 * 60 + 30;
  const marketClose = 16 * 60;
  return totalMinutes >= marketOpen && totalMinutes < marketClose;
}

function PriceChart({ symbol, refreshTick }) {
  const [data, setData]       = useState([]);
  const [range, setRange]     = useState('1D');
  const marketOpen            = isMarketOpen();

  const RANGES = ['1D', '5D', '1M'];

  const activeTick = marketOpen ? refreshTick : 0;

  useEffect(() => {
    axios.get(`${API}/prices/${symbol}?limit=200`).then(res => {
      const all = res.data.reverse();
      const now = new Date();

      let filtered;
      if (range === '1D') {
        const todayStr = now.toISOString().slice(0, 10);
        filtered = all.filter(d => d.timestamp.slice(0, 10) === todayStr);
        if (filtered.length === 0) {
          const lastDay = all.length > 0 ? all[all.length - 1].timestamp.slice(0, 10) : null;
          filtered = lastDay ? all.filter(d => d.timestamp.slice(0, 10) === lastDay) : all.slice(-20);
        }
      } else if (range === '5D') {
        const cutoff = new Date(now - 5 * 24 * 60 * 60 * 1000);
        filtered = all.filter(d => new Date(d.timestamp) >= cutoff);
        if (filtered.length === 0) filtered = all.slice(-30);
      } else {
        filtered = all;
      }

      setData(filtered.map(d => {
        const utc = new Date(d.timestamp + 'Z');
        const hh  = String(utc.getHours()).padStart(2, '0');
        const min = String(utc.getMinutes()).padStart(2, '0');
        const mm  = String(utc.getMonth() + 1).padStart(2, '0');
        const dd  = String(utc.getDate()).padStart(2, '0');
        return {
          name:  range === '1D' ? `${hh}:${min}` : `${mm}-${dd}`,
          price: d.price,
        };
      }));
    });
  }, [symbol, range, activeTick]); // eslint-disable-line react-hooks/exhaustive-deps

  if (data.length === 0) return (
    <div style={{ height: 120, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#ccc', fontSize: 12 }}>
      No price data
    </div>
  );

  const minP       = Math.min(...data.map(d => d.price)) * 0.995;
  const maxP       = Math.max(...data.map(d => d.price)) * 1.005;
  const firstPrice = data[0]?.price;
  const lastPrice  = data[data.length - 1]?.price;
  const lineColor  = (lastPrice != null && firstPrice != null)
    ? (lastPrice >= firstPrice ? COLORS.positive : COLORS.negative)
    : COLORS.blue;

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
        <span style={{ fontSize: 9, color: marketOpen ? COLORS.positive : '#bbb' }}>
          {marketOpen ? 'Market open' : 'Market closed'}
        </span>
        <div style={{ display: 'flex', gap: 4 }}>
          {RANGES.map(r => (
            <button key={r} onClick={() => setRange(r)} style={{
              padding: '2px 8px', borderRadius: 4, border: '1px solid #eee', cursor: 'pointer',
              background: range === r ? COLORS.blue : '#f7f7f5',
              color: range === r ? '#fff' : '#888',
              fontSize: 10, fontWeight: 500
            }}>{r}</button>
          ))}
        </div>
      </div>
      <div style={{ width: '100%', height: 120 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
            <XAxis
              dataKey="name"
              tick={{ fontSize: 9, fill: '#bbb' }}
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              domain={[minP, maxP]}
              tick={{ fontSize: 9, fill: '#bbb' }}
              tickLine={false}
              axisLine={false}
              width={44}
              tickFormatter={v => `$${v.toFixed(0)}`}
            />
            <Line
              type="monotone"
              dataKey="price"
              stroke={lineColor}
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
            <Tooltip
              formatter={v => [`$${Number(v).toFixed(2)}`, 'Price']}
              labelFormatter={l => range === '1D' ? `${marketOpen ? 'Today' : 'Last session'} ${l}` : `Date: ${l}`}
              contentStyle={{ fontSize: 11, borderRadius: 6 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function StockCard({ symbol, onRemove, refreshTick }) {
  const [prices, setPrices]       = useState([]);
  const [sentiment, setSentiment] = useState(null);
  const [risk, setRisk]           = useState(null);
  const [move, setMove]           = useState(null);
  const [earnings, setEarnings]   = useState(null);
  const [livePrice, setLivePrice] = useState(null);

  useEffect(() => {
    // Live price — keeps old value while fetching so no flicker
    axios.get(`${API}/live-price/${symbol}`).then(res => {
      if (res.data && res.data.price) {
        setLivePrice(prev => ({ ...prev, ...res.data }));
      }
    }).catch(() => {});

    axios.get(`${API}/prices/${symbol}`).then(res => setPrices(res.data));
    axios.get(`${API}/risk/${symbol}`).then(res => setRisk(res.data));
    axios.get(`${API}/expected-move/${symbol}?days=1`).then(res => setMove(res.data));
  }, [symbol, refreshTick]);

  useEffect(() => {
    axios.get(`${API}/sentiment/${symbol}`).then(res => res.data.length > 0 && setSentiment(res.data[0].score));
    axios.get(`${API}/earnings`).then(res => {
      const match = res.data.find(e => e.symbol === symbol);
      if (match) setEarnings(match);
    });
  }, [symbol]);

  const price      = livePrice?.price ?? (prices.length > 0 ? prices[0].price : null);
  const prevClose  = livePrice?.prev_close ?? (prices.length > 1 ? prices[1].price : null);
  const changeDol  = livePrice?.change ?? (price != null && prevClose != null ? price - prevClose : null);
  const changePct  = livePrice?.change_pct ?? (changeDol != null && prevClose ? (changeDol / prevClose) * 100 : null);
  const isPositive = changeDol != null ? changeDol >= 0 : true;

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e5e5', borderRadius: 12, padding: 20, position: 'relative' }}>
      <button onClick={() => onRemove(symbol)} style={{
        position: 'absolute', top: 12, right: 12, background: 'none',
        border: 'none', cursor: 'pointer', color: '#ccc', fontSize: 16, lineHeight: 1, padding: 2
      }}>×</button>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 2, paddingRight: 20 }}>
        <span style={{ fontWeight: 500, fontSize: 17 }}>{symbol}</span>
        <div style={{ textAlign: 'right' }}>
          <div style={{ fontWeight: 500, fontSize: 18, color: COLORS.blue }}>
            {price != null ? `$${Number(price).toFixed(2)}` : '—'}
          </div>
          {changeDol != null && changePct != null && (
            <div style={{ fontSize: 11, color: isPositive ? COLORS.positive : COLORS.negative, fontWeight: 500 }}>
              {isPositive ? '+' : ''}{Number(changeDol).toFixed(2)} ({isPositive ? '+' : ''}{Number(changePct).toFixed(2)}%)
            </div>
          )}
        </div>
      </div>

      {prevClose != null && (
        <div style={{ fontSize: 11, color: '#bbb', marginBottom: 8 }}>
          Prev close ${Number(prevClose).toFixed(2)}
        </div>
      )}

      <PriceChart symbol={symbol} refreshTick={refreshTick} />

      <div style={{ fontSize: 12, color: '#555', marginTop: 12 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
          <span>Sentiment</span>
          {sentiment !== null ? <SentimentBar score={sentiment} /> : <span>—</span>}
        </div>

        {risk?.volatility_30d && (
          <>
            <div style={{ height: 1, background: '#f0f0f0', margin: '8px 0' }} />
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span>Volatility</span>
              <span style={{ color: COLORS.amber, fontWeight: 500 }}>{(risk.volatility_30d * 100).toFixed(1)}%</span>
            </div>
            <div style={{ marginBottom: 8 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                <span>1-day VaR 95%</span>
                <span style={{ color: COLORS.negative, fontWeight: 500 }}>-${risk.var_95?.toFixed(2)}</span>
              </div>
              <MiniBar value={risk.var_95} max={20} color={COLORS.negative} bg="#fce8e2" />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span>Sharpe ratio</span>
              <span style={{ fontWeight: 500, color: risk.sharpe > 0 ? COLORS.positive : COLORS.negative }}>
                {risk.sharpe?.toFixed(2)}
              </span>
            </div>
          </>
        )}

        {move && move.expected_move_dollar > 0 && (
          <>
            <div style={{ height: 1, background: '#f0f0f0', margin: '8px 0' }} />
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span>Expected move</span>
              <span style={{ fontWeight: 500, color: COLORS.purple }}>
                ±${move.expected_move_dollar} ({move.expected_move_pct}%)
              </span>
            </div>
            <div style={{ fontSize: 11, color: '#aaa' }}>Range: ${move.range_low} — ${move.range_high}</div>
          </>
        )}

        {earnings && (
          <>
            <div style={{ height: 1, background: '#f0f0f0', margin: '8px 0' }} />
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span>Next earnings</span>
              <span style={{ fontWeight: 500 }}>{earnings.date?.slice(0, 10)}</span>
            </div>
            {earnings.surprise_pct != null && (
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Last surprise</span>
                <span style={{ fontWeight: 500, color: earnings.surprise_pct > 0 ? COLORS.positive : COLORS.negative }}>
                  {earnings.surprise_pct > 0 ? '+' : ''}{earnings.surprise_pct?.toFixed(2)}%
                </span>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

function SearchBar({ watchlist, onAdd }) {
  const [query, setQuery]             = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [status, setStatus]           = useState('');
  const [loading, setLoading]         = useState(false);
  const [showDrop, setShowDrop]       = useState(false);
  const dropRef                       = useRef(null);

  useEffect(() => {
    const handleClick = e => {
      if (dropRef.current && !dropRef.current.contains(e.target)) setShowDrop(false);
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  useEffect(() => {
    if (query.length < 1) { setSuggestions([]); setShowDrop(false); return; }
    const timer = setTimeout(() => {
      axios.get(`${API}/search?q=${query}`)
        .then(res => {
          setSuggestions(res.data);
          if (res.data.length > 0) setShowDrop(true);
        })
        .catch(() => setSuggestions([]));
    }, 350);
    return () => clearTimeout(timer);
  }, [query]);

  const handleSelect = async (symbol) => {
    setShowDrop(false);
    setSuggestions([]);
    const s = symbol.trim().toUpperCase();
    if (!s) return;
    if (watchlist.includes(s)) { setStatus(`${s} already in watchlist`); setQuery(''); return; }
    setLoading(true);
    setStatus(`Fetching ${s}...`);
    try {
      await axios.post(`${API}/ingest/symbol/${s}`);
      onAdd(s);
      setQuery('');
      setStatus(`${s} added`);
    } catch (e) {
      setStatus(`Could not add ${s}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ position: 'relative' }} ref={dropRef}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <input
          value={query}
          onChange={e => { setQuery(e.target.value); setStatus(''); }}
          onKeyDown={e => {
            if (e.key === 'Enter') handleSelect(query);
            if (e.key === 'Escape') setShowDrop(false);
          }}
          onFocus={() => suggestions.length > 0 && setShowDrop(true)}
          placeholder="Search company or ticker..."
          style={{ padding: '8px 12px', borderRadius: 8, border: '1px solid #ddd', fontSize: 13, width: 220 }}
        />
        <button onClick={() => handleSelect(query)} disabled={loading} style={{
          padding: '8px 16px', borderRadius: 8, border: 'none',
          background: COLORS.blue, color: '#fff', cursor: 'pointer', fontSize: 13, fontWeight: 500
        }}>
          {loading ? '...' : '+ Add'}
        </button>
        {status && (
          <span style={{ fontSize: 12, color: status.includes('added') ? COLORS.positive : '#888' }}>
            {status}
          </span>
        )}
      </div>

      {showDrop && suggestions.length > 0 && (
        <div style={{
          position: 'absolute', top: '100%', left: 0,
          background: '#fff', border: '1px solid #e5e5e5', borderRadius: 8,
          boxShadow: '0 4px 16px rgba(0,0,0,0.12)', zIndex: 1000, marginTop: 4, minWidth: 300
        }}>
          {suggestions.map((s, i) => (
            <div key={i} onClick={() => handleSelect(s.symbol)}
              style={{
                padding: '10px 14px', cursor: 'pointer', display: 'flex',
                justifyContent: 'space-between', alignItems: 'center',
                borderBottom: i < suggestions.length - 1 ? '1px solid #f5f5f5' : 'none',
                fontSize: 13, transition: 'background 0.1s'
              }}
              onMouseEnter={e => e.currentTarget.style.background = '#f7f7f5'}
              onMouseLeave={e => e.currentTarget.style.background = '#fff'}
            >
              <span style={{ fontWeight: 500, color: COLORS.blue, minWidth: 60 }}>{s.symbol}</span>
              <span style={{ color: '#666', fontSize: 12, textAlign: 'right',
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 200 }}>
                {s.name}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function RiskTable({ watchlist }) {
  const [data, setData] = useState([]);
  useEffect(() => {
    const symbols = watchlist.join(',');
    axios.get(`${API}/risk?symbols=${symbols}`).then(res => setData(res.data));
  }, [watchlist]);

  const maxVol = Math.max(...data.map(d => d.volatility_30d || 0));
  const maxVar = Math.max(...data.map(d => d.var_95 || 0));
  const sharpeColor = s => {
    if (s == null) return COLORS.neutral;
    if (s > 1)  return COLORS.positive;
    if (s > 0)  return '#8BC34A';
    if (s > -1) return COLORS.amber;
    return COLORS.negative;
  };

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e5e5', borderRadius: 12, padding: 20 }}>
      <div style={{ fontWeight: 500, fontSize: 15, marginBottom: 4 }}>Risk summary</div>
      <div style={{ fontSize: 12, color: '#aaa', marginBottom: 16 }}>
        Sharpe &gt; 1 = good · 0–1 = acceptable · &lt; 0 = negative risk-adjusted return
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid #eee', color: '#888' }}>
            <th style={{ textAlign: 'left',  padding: '6px 8px', fontWeight: 500 }}>Symbol</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Volatility</th>
            <th style={{ textAlign: 'left',  padding: '6px 8px', fontWeight: 500, width: 110 }}>Vol bar</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>VaR 95%</th>
            <th style={{ textAlign: 'left',  padding: '6px 8px', fontWeight: 500, width: 110 }}>VaR bar</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Sharpe</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Beta</th>
          </tr>
        </thead>
        <tbody>
          {data.map((r, i) => (
            <tr key={i} style={{ borderBottom: '1px solid #f5f5f5' }}>
              <td style={{ padding: '8px 8px', fontWeight: 500 }}>{r.symbol}</td>
              <td style={{ padding: '8px 8px', textAlign: 'right', color: COLORS.amber, fontWeight: 500 }}>
                {(r.volatility_30d * 100).toFixed(1)}%
              </td>
              <td style={{ padding: '8px 8px' }}>
                <MiniBar value={r.volatility_30d} max={maxVol} color={COLORS.amber} bg="#faeeda" />
              </td>
              <td style={{ padding: '8px 8px', textAlign: 'right', color: COLORS.negative, fontWeight: 500 }}>
                -${r.var_95?.toFixed(2)}
              </td>
              <td style={{ padding: '8px 8px' }}>
                <MiniBar value={r.var_95} max={maxVar} color={COLORS.negative} bg="#fce8e2" />
              </td>
              <td style={{ padding: '8px 8px', textAlign: 'right', fontWeight: 500 }}>
                <span style={{ color: sharpeColor(r.sharpe) }}>{r.sharpe?.toFixed(2) ?? '—'}</span>
              </td>
              <td style={{ padding: '8px 8px', textAlign: 'right' }}>{r.beta?.toFixed(2) ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ExpectedMovePanel({ watchlist }) {
  const [data, setData] = useState([]);
  const [days, setDays] = useState(1);

  const fetchData = useCallback((d) => {
    Promise.all(
      watchlist.map(s =>
        axios.get(`${API}/expected-move/${s}?days=${d}`)
          .then(res => ({ symbol: s, ...res.data }))
          .catch(() => ({ symbol: s, expected_move_dollar: null }))
      )
    ).then(setData);
  }, [watchlist]);

  useEffect(() => { 
    fetchData(days); 
}, [watchlist, days, fetchData]); // Add all dependencies

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e5e5', borderRadius: 12, padding: 20 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
        <div style={{ fontWeight: 500, fontSize: 15 }}>Expected move</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 12, color: '#888' }}>Days:</span>
          {[1, 3, 5, 10].map(d => (
            <button key={d} onClick={() => { setDays(d); fetchData(d); }} style={{
              padding: '3px 10px', borderRadius: 6, border: '1px solid #ddd', cursor: 'pointer',
              background: days === d ? COLORS.blue : '#fff',
              color: days === d ? '#fff' : '#555',
              fontSize: 12, fontWeight: 500
            }}>{d}d</button>
          ))}
        </div>
      </div>
      <div style={{ fontSize: 12, color: '#aaa', marginBottom: 16 }}>
        How much each stock could move based on historical volatility
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 12 }}>
        {data.filter(d => d.expected_move_dollar).map((d, i) => (
          <div key={i} style={{ textAlign: 'center', padding: 12, background: '#f7f7f5', borderRadius: 8 }}>
            <div style={{ fontWeight: 500, fontSize: 14, marginBottom: 4 }}>{d.symbol}</div>
            <div style={{ fontSize: 14, color: COLORS.purple, fontWeight: 500 }}>±${d.expected_move_dollar}</div>
            <div style={{ fontSize: 11, color: '#888' }}>{d.expected_move_pct}%</div>
            <div style={{ fontSize: 10, color: '#bbb', marginTop: 4 }}>${d.range_low} – ${d.range_high}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function PositionSizer({ watchlist }) {
  const [portfolio, setPortfolio] = useState(100000);
  const [riskPct, setRiskPct]     = useState(1);
  const [results, setResults]     = useState([]);

  const calculate = useCallback(() => {
    Promise.all(
      watchlist.map(s =>
        axios.get(`${API}/position-size/${s}?portfolio=${portfolio}&risk_pct=${riskPct / 100}`)
          .then(res => ({ symbol: s, ...res.data }))
          .catch(() => ({ symbol: s }))
      )
    ).then(r => setResults(r.filter(x => x.shares)));
  }, [watchlist, portfolio, riskPct]);

  useEffect(() => { 
    calculate(); 
}, [watchlist, calculate]); // Add calculate to dependencies

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e5e5', borderRadius: 12, padding: 20 }}>
      <div style={{ fontWeight: 500, fontSize: 15, marginBottom: 4 }}>Position sizing</div>
      <div style={{ fontSize: 12, color: '#aaa', marginBottom: 16 }}>
        How many shares to buy given your portfolio size and max risk per trade
      </div>
      <div style={{ display: 'flex', gap: 16, marginBottom: 20, flexWrap: 'wrap' }}>
        <div>
          <div style={{ fontSize: 11, color: '#888', marginBottom: 4 }}>Portfolio value ($)</div>
          <input type="number" value={portfolio} onChange={e => setPortfolio(Number(e.target.value))}
            style={{ padding: '6px 10px', borderRadius: 6, border: '1px solid #ddd', fontSize: 13, width: 140 }} />
        </div>
        <div>
          <div style={{ fontSize: 11, color: '#888', marginBottom: 4 }}>Max risk per trade (%)</div>
          <input type="number" value={riskPct} step="0.5" min="0.1" max="5"
            onChange={e => setRiskPct(Number(e.target.value))}
            style={{ padding: '6px 10px', borderRadius: 6, border: '1px solid #ddd', fontSize: 13, width: 100 }} />
        </div>
        <div style={{ display: 'flex', alignItems: 'flex-end' }}>
          <button onClick={calculate} style={{
            padding: '6px 16px', borderRadius: 6, border: 'none',
            background: COLORS.blue, color: '#fff', cursor: 'pointer', fontSize: 13, fontWeight: 500
          }}>Calculate</button>
        </div>
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid #eee', color: '#888' }}>
            <th style={{ textAlign: 'left',  padding: '6px 8px', fontWeight: 500 }}>Symbol</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Price</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>VaR/share</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Shares</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Position value</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Risk $</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Risk %</th>
          </tr>
        </thead>
        <tbody>
          {results.map((r, i) => (
            <tr key={i} style={{ borderBottom: '1px solid #f5f5f5' }}>
              <td style={{ padding: '8px 8px', fontWeight: 500 }}>{r.symbol}</td>
              <td style={{ padding: '8px 8px', textAlign: 'right' }}>${r.price?.toFixed(2)}</td>
              <td style={{ padding: '8px 8px', textAlign: 'right', color: COLORS.negative }}>-${r.var_95_per_share?.toFixed(2)}</td>
              <td style={{ padding: '8px 8px', textAlign: 'right', fontWeight: 500, color: COLORS.blue }}>{r.shares}</td>
              <td style={{ padding: '8px 8px', textAlign: 'right' }}>${r.position_value?.toLocaleString()}</td>
              <td style={{ padding: '8px 8px', textAlign: 'right', color: COLORS.negative }}>${r.risk_dollar?.toFixed(2)}</td>
              <td style={{ padding: '8px 8px', textAlign: 'right' }}>
                <span style={{ background: '#fce8e2', color: COLORS.negative, padding: '2px 6px', borderRadius: 4, fontSize: 11 }}>
                  {r.risk_pct_actual?.toFixed(2)}%
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SentimentPanel({ watchlist }) {
  const [data, setData] = useState([]);
  useEffect(() => {
    Promise.all(watchlist.map(s =>
      axios.get(`${API}/sentiment/${s}`)
        .then(res => ({ symbol: s, score: res.data[0]?.score ?? 0 }))
    )).then(setData);
  }, [watchlist]);

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e5e5', borderRadius: 12, padding: 20 }}>
      <div style={{ fontWeight: 500, fontSize: 15, marginBottom: 16 }}>Sentiment overview</div>
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
          <XAxis dataKey="symbol" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 10 }} domain={[-1, 1]} />
          <ReferenceLine y={0} stroke="#ddd" />
          <Tooltip formatter={v => [v.toFixed(3), 'Score']} contentStyle={{ fontSize: 11 }} />
          <Bar dataKey="score" radius={[4, 4, 0, 0]}>
            {data.map((d, i) => (
              <Cell key={i} fill={d.score > 0.05 ? COLORS.positive : d.score < -0.05 ? COLORS.negative : COLORS.neutral} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function EarningsTable() {
  const [rows, setRows]     = useState([]);
  const [filter, setFilter] = useState('');
  useEffect(() => { axios.get(`${API}/earnings`).then(res => setRows(res.data)); }, []);

  const filtered = rows.filter(r =>
    r.symbol.toLowerCase().includes(filter.toLowerCase()) ||
    (r.company || '').toLowerCase().includes(filter.toLowerCase())
  );

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e5e5', borderRadius: 12, padding: 20 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <span style={{ fontWeight: 500, fontSize: 15 }}>Upcoming earnings</span>
        <input placeholder="Filter by symbol..." value={filter}
          onChange={e => setFilter(e.target.value)}
          style={{ padding: '4px 10px', borderRadius: 6, border: '1px solid #ddd', fontSize: 13 }} />
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid #eee', color: '#888' }}>
            <th style={{ textAlign: 'left',  padding: '6px 8px', fontWeight: 500 }}>Symbol</th>
            <th style={{ textAlign: 'left',  padding: '6px 8px', fontWeight: 500 }}>Date</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>EPS est.</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>EPS actual</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Surprise %</th>
          </tr>
        </thead>
        <tbody>
          {filtered.slice(0, 20).map((r, i) => (
            <tr key={i} style={{ borderBottom: '1px solid #f5f5f5' }}>
              <td style={{ padding: '6px 8px', fontWeight: 500 }}>{r.symbol}</td>
              <td style={{ padding: '6px 8px', color: '#555' }}>{r.date?.slice(0, 10)}</td>
              <td style={{ padding: '6px 8px', textAlign: 'right' }}>{r.eps_estimate ?? '—'}</td>
              <td style={{ padding: '6px 8px', textAlign: 'right' }}>{r.eps_actual ?? '—'}</td>
              <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                {r.surprise_pct != null ? (
                  <span style={{ color: r.surprise_pct > 0 ? COLORS.positive : COLORS.negative, fontWeight: 500 }}>
                    {r.surprise_pct > 0 ? '+' : ''}{r.surprise_pct.toFixed(2)}%
                  </span>
                ) : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function App() {
  const [watchlist, setWatchlist] = useState(() => {
    const saved = localStorage.getItem('watchlist');
    return saved ? JSON.parse(saved) : DEFAULT_WATCHLIST;
  });
  const [refreshTick, setRefreshTick] = useState(0);
  const [lastUpdated, setLastUpdated] = useState(new Date().toLocaleTimeString());
  const [countdown, setCountdown]     = useState(40);

  useEffect(() => {
    localStorage.setItem('watchlist', JSON.stringify(watchlist));
  }, [watchlist]);

  useEffect(() => {
    const interval = setInterval(() => {
      setRefreshTick(t => t + 1);
      setLastUpdated(new Date().toLocaleTimeString());
      setCountdown(40);
    }, REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const tick = setInterval(() => setCountdown(c => c > 0 ? c - 1 : 0), 1000);
    return () => clearInterval(tick);
  }, []);

  const addSymbol    = s => { if (!watchlist.includes(s)) setWatchlist(prev => [...prev, s]); };
  const removeSymbol = s => setWatchlist(prev => prev.filter(x => x !== s));
  const refresh      = () => {
    setRefreshTick(t => t + 1);
    setLastUpdated(new Date().toLocaleTimeString());
    setCountdown(40);
  };

  return (
    <div style={{ minHeight: '100vh', background: '#f7f7f5', fontFamily: 'system-ui, sans-serif', padding: 24 }}>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 500 }}>Earnings Risk Tracker</h1>
          <p style={{ margin: 0, fontSize: 13, color: '#888' }}>
            15-min delayed · Updated {lastUpdated} · refreshing in {countdown}s
          </p>
        </div>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
          <SearchBar watchlist={watchlist} onAdd={addSymbol} />
          <button onClick={refresh} style={{
            padding: '8px 16px', borderRadius: 8, border: '1px solid #ddd',
            background: '#fff', cursor: 'pointer', fontSize: 13, fontWeight: 500
          }}>Refresh</button>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 16, marginBottom: 24 }}>
        {watchlist.map(s => <StockCard key={s} symbol={s} onRemove={removeSymbol} refreshTick={refreshTick} />)}
      </div>

      <div style={{ marginBottom: 24 }}><RiskTable watchlist={watchlist} /></div>
      <div style={{ marginBottom: 24 }}><ExpectedMovePanel watchlist={watchlist} /></div>
      <div style={{ marginBottom: 24 }}><PositionSizer watchlist={watchlist} /></div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: 16 }}>
        <SentimentPanel watchlist={watchlist} />
        <EarningsTable />
      </div>

    </div>
  );
}