/**
 * القناص V18.0 — SNIPER CRON PRO
 * Deno + Binance Spot REST + Telegram. تنبيهات فقط، لا ينفذ صفقات.
 * ENV: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
 * تشغيل مستمر: deno run --allow-net --allow-env sniper_v17.ts
 * Deno Deploy: Deno.cron يشغّل الفحص كل دقيقة. محليًا: حلقة مستمرة.
 */

type Kline = { openTime:number; high:number; low:number; close:number; closeTime:number; volume:number; quoteVolume:number; takerBuyBase:number };
type Candidate = { symbol:string; price:number; change24h:number; quoteVolume24h:number };
type Signal = { symbol:string; price:number; change24h:number; change5m:number; score:number; kind:string; rsi:number; volumeMultiplier:number; buyRatio:number; rangePct:number; bookRatio:number|null; reasons:string[]; candleTime:number };

const CONFIG = {
  quoteAsset: "USDT",
  minQuoteVolume24h: 250_000,
  maxQuoteVolume24h: 10_000_000_000,
  minChange24h: -20,
  maxChange24h: 300,
  candidateLimit: 120,
  candleLimit: 60,
  scanIntervalMs: 60_000,
  maxRuntimeMs: 52_000,
  batchSize: 8,
  batchDelayMs: 100,
  requestTimeoutMs: 4_500,
  retries: 1,
  minVolumeMultiplier: 1.5,
  maxAccumulationRangePct: 4.5, // 4.5% وليس 0.045%
  minTakerBuyRatio: 0.515,
  minScore: 62,
  maxSignalsPerCycle: 5,
  orderBookLimit: 20,
  orderBookTopCandidates: 15,
  useOrderBookAsHardFilter: false,
  minOrderBookRatio: 1.05,
  cooldownMs: 30 * 60_000,
  logRejected: true,
  sendDiagnosticsToTelegram: false,
};
const BASE_URL = "https://data-api.binance.vision";
const sentAt = new Map<string, number>(); // لا يستمر بين استدعاءات Cron المنفصلة
const rejects = new Map<string, number>();
const stablecoinBases = new Set([
  "USDT","USDC","FDUSD","BUSD","TUSD","USDP","DAI","USDE","USDS","USDD",
  "PYUSD","USD1","RLUSD","USDF","USDC.E","UST","USTC","FRAX","LUSD","GUSD",
  "EURC","EURI","AEUR","EUR","TRY","BRZ","XAUT","PAXG"
]);
const leveraged = new Set(["BTCUPUSDT","BTCDOWNUSDT","ETHUPUSDT","ETHDOWNUSDT","BNBUPUSDT","BNBDOWNUSDT","ADAUPUSDT","ADADOWNUSDT","LINKUPUSDT","LINKDOWNUSDT","XRPUPUSDT","XRPDOWNUSDT","DOTUPUSDT","DOTDOWNUSDT","LTCUPUSDT","LTCDOWNUSDT","BCHUPUSDT","BCHDOWNUSDT","EOSUPUSDT","EOSDOWNUSDT","TRXUPUSDT","TRXDOWNUSDT","FILUPUSDT","FILDOWNUSDT","UNIUPUSDT","UNIDOWNUSDT","SUSHIUPUSDT","SUSHIDOWNUSDT","YFIUPUSDT","YFIDOWNUSDT","AAVEUPUSDT","AAVEDOWNUSDT","XLMUPUSDT","XLMDOWNUSDT","XTZUPUSDT","XTZDOWNUSDT","ETCUPUSDT","ETCDOWNUSDT","COMPUPUSDT","COMPDOWNUSDT","1INCHUPUSDT","1INCHDOWNUSDT"]);
const sleep = (ms:number) => new Promise<void>(r => setTimeout(r, ms));
const num = (v:unknown) => { const n = Number(v); return Number.isFinite(n) ? n : NaN; };
const fmtPct = (v:number) => Number.isFinite(v) ? `${v >= 0 ? "+" : ""}${v.toFixed(2)}%` : "غير متاح";
function fmtPrice(v:number):string { if(!Number.isFinite(v)) return "غير متاح"; if(v>=1000)return v.toFixed(2); if(v>=1)return v.toFixed(5); if(v>=.01)return v.toFixed(6); return v.toPrecision(6); }
function reject(reason:string):null { rejects.set(reason,(rejects.get(reason)??0)+1); return null; }

async function fetchAPI(path:string, params:Record<string,string|number>={}, deadline=Infinity):Promise<any|null> {
  const q = new URLSearchParams(); for(const [k,v] of Object.entries(params)) q.set(k,String(v));
  for(let attempt=0; attempt<=CONFIG.retries; attempt++) {
    const remaining=deadline-Date.now(); if(remaining<=0)return null;
    try {
      const res=await fetch(`${BASE_URL}${path}?${q}`,{signal:AbortSignal.timeout(Math.max(1,Math.min(CONFIG.requestTimeoutMs,remaining)))});
      if(res.status===418||res.status===429){console.warn(`Binance rate limit HTTP ${res.status}`);return null;}
      if(!res.ok){console.warn(`Binance ${path}: HTTP ${res.status}`);if(res.status>=500&&attempt<CONFIG.retries){await sleep(180);continue;}return null;}
      return await res.json();
    } catch(e) { console.warn(`طلب ${path} فشل`,e); if(attempt<CONFIG.retries&&Date.now()<deadline)await sleep(180); }
  }
  return null;
}

async function sendTelegram(message:string):Promise<boolean> {
  const token=Deno.env.get("TELEGRAM_BOT_TOKEN"), chatId=Deno.env.get("TELEGRAM_CHAT_ID");
  if(!token||!chatId){console.error("ناقص TELEGRAM_BOT_TOKEN أو TELEGRAM_CHAT_ID");return false;}
  try {
    const res=await fetch(`https://api.telegram.org/bot${token}/sendMessage`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({chat_id:chatId,text:message,parse_mode:"HTML",disable_web_page_preview:true}),signal:AbortSignal.timeout(8000)});
    const data=await res.json(); if(!res.ok||data.ok!==true)console.error("Telegram:",data.description??res.status);
    return res.ok&&data.ok===true;
  } catch(e){console.error("إرسال Telegram فشل",e);return false;}
}

function parseKlines(raw:any):Kline[] {
  if(!Array.isArray(raw))return [];
  const now=Date.now(), out:Kline[]=[];
  for(const x of raw){
    if(!Array.isArray(x)||x.length<11)continue;
    const k:Kline={openTime:num(x[0]),high:num(x[2]),low:num(x[3]),close:num(x[4]),closeTime:num(x[6]),volume:num(x[5]),quoteVolume:num(x[7]),takerBuyBase:num(x[9])};
    if(![k.openTime,k.high,k.low,k.close,k.closeTime,k.volume,k.quoteVolume,k.takerBuyBase].every(Number.isFinite)||k.closeTime>=now||k.low<=0||k.close<=0||k.high<k.low||k.volume<0||k.quoteVolume<0||k.takerBuyBase<0||k.takerBuyBase>k.volume)continue;
    out.push(k);
  }
  return out;
}
function calcRSI(values:number[], period=14):number {
  if(values.length<period+1)return NaN;
  let gains=0,losses=0;
  for(let i=1;i<=period;i++){const d=values[i]-values[i-1];if(d>0)gains+=d;else losses-=d;}
  let ag=gains/period, al=losses/period;
  for(let i=period+1;i<values.length;i++){const d=values[i]-values[i-1];ag=(ag*(period-1)+Math.max(d,0))/period;al=(al*(period-1)+Math.max(-d,0))/period;}
  if(ag===0&&al===0)return 50;if(al===0)return 100;if(ag===0)return 0;return 100-100/(1+ag/al);
}
function buyRatio(cs:Kline[]):number {const total=cs.reduce((s,c)=>s+c.volume,0);return total>0?cs.reduce((s,c)=>s+c.takerBuyBase,0)/total:NaN;}

async function getCandidates(deadline:number):Promise<Candidate[]> {
  const data=await fetchAPI("/api/v3/ticker/24hr",{},deadline);
  if(!Array.isArray(data))throw new Error("تعذر جلب قائمة العملات من Binance");
  return data.filter((t:any)=>{
    const s=String(t.symbol??""), qv=num(t.quoteVolume), ch=num(t.priceChangePercent), p=num(t.lastPrice);
    const base=s.endsWith(CONFIG.quoteAsset)?s.slice(0,-CONFIG.quoteAsset.length):""; return s.endsWith(CONFIG.quoteAsset)&&/^[A-Z0-9]+USDT$/.test(s)&&!stablecoinBases.has(base)&&!leveraged.has(s)&&!(["NGNUSDT","TRYUSDT"].includes(s))&&[qv,ch,p].every(Number.isFinite)&&qv>=CONFIG.minQuoteVolume24h&&qv<=CONFIG.maxQuoteVolume24h&&ch>=CONFIG.minChange24h&&ch<=CONFIG.maxChange24h;
  }).sort((a:any,b:any)=>num(b.quoteVolume)-num(a.quoteVolume)).slice(0,CONFIG.candidateLimit).map((t:any)=>({symbol:String(t.symbol),price:num(t.lastPrice),change24h:num(t.priceChangePercent),quoteVolume24h:num(t.quoteVolume)}));
}
async function getBookRatio(symbol:string,deadline:number):Promise<number|null> {
  const d=await fetchAPI("/api/v3/depth",{symbol,limit:CONFIG.orderBookLimit},deadline);
  if(!d||!Array.isArray(d.bids)||!Array.isArray(d.asks))return null;
  const total=(levels:any[])=>levels.reduce((s,l)=>{const p=num(l?.[0]),q=num(l?.[1]);return s+(p>0&&q>0?p*q:0);},0);
  const bids=total(d.bids),asks=total(d.asks);return asks>0?bids/asks:null;
}

async function analyze(c:Candidate,deadline:number,checkBook:boolean):Promise<Signal|null> {
  const raw=await fetchAPI("/api/v3/klines",{symbol:c.symbol,interval:"5m",limit:CONFIG.candleLimit},deadline);
  const cs=parseKlines(raw);if(cs.length<30)return reject("شموع غير كافية");
  const last=cs.at(-1)!, prior=cs.at(-2)!;
  const recent=cs.slice(-3), baseline=cs.slice(-15,-3), range=cs.slice(-13,-1);
  if(baseline.length<12||range.length<12)return reject("فترة مرجعية غير كافية");
  const change5m=prior.close>0?(last.close-prior.close)/prior.close*100:0;
  const hi=Math.max(...range.map(x=>x.high)), lo=Math.min(...range.map(x=>x.low));
  const rangePct=lo>0?(hi-lo)/lo*100:999;
  const rq=recent.reduce((s,x)=>s+x.quoteVolume,0), bq=baseline.reduce((s,x)=>s+x.quoteVolume,0);
  const vm=bq>0?(rq/recent.length)/(bq/baseline.length):0, br=buyRatio(recent), rsi=calcRSI(cs.map(x=>x.close));
  const breakout=last.close>hi, near=hi>0&&(hi-last.close)/hi<=.012&&last.close<=hi;
  const accumulation=rangePct<=CONFIG.maxAccumulationRangePct&&vm>=1.25;
  const book=checkBook&&Date.now()<deadline?await getBookRatio(c.symbol,deadline):null;
  let score=0;const reasons:string[]=[];
  if(vm>=CONFIG.minVolumeMultiplier){score+=25;reasons.push(`حجم التداول ${vm.toFixed(2)}x من المعتاد`);}else if(vm>=1.25){score+=15;reasons.push(`ارتفاع الحجم ${vm.toFixed(2)}x`);}
  if(br>=.58){score+=25;reasons.push(`ضغط شراء قوي ${(br*100).toFixed(1)}%`);}else if(br>=CONFIG.minTakerBuyRatio){score+=15;reasons.push(`ضغط شراء إيجابي ${(br*100).toFixed(1)}%`);}
  if(accumulation){score+=18;reasons.push(`نطاق سعري ضيق ${rangePct.toFixed(2)}% مع حجم متزايد`);}
  if(near){score+=15;reasons.push("السعر قريب من مقاومة الفترة المرجعية");}
  if(breakout){score+=20;reasons.push("تجاوز أعلى قمة بالفترة المرجعية");}
  if(Number.isFinite(rsi)&&rsi>=45&&rsi<=72){score+=10;reasons.push(`RSI متوازن ${rsi.toFixed(1)}`);}else if(Number.isFinite(rsi)&&rsi>72&&rsi<=82){score+=4;reasons.push(`RSI مرتفع ${rsi.toFixed(1)}`);}
  if(change5m>0&&change5m<=1.8){score+=5;reasons.push(`حركة الشمعة الأخيرة ${fmtPct(change5m)}`);}
  if(book!==null&&book>=CONFIG.minOrderBookRatio){score+=5;reasons.push(`ميزان دفتر الأوامر ${book.toFixed(2)}x`);}if(book!==null&&book<.75)score-=8;
  if(change5m>3){score-=12;reasons.push("الحركة الأخيرة قوية وقد تكون متأخرة");}if(Number.isFinite(rsi)&&rsi>82)score-=10;
  if(c.change24h>25){score-=8;reasons.push("الارتفاع اليومي كبير؛ خطر ملاحقة الحركة أعلى");}
  if(CONFIG.useOrderBookAsHardFilter&&(book===null||book<CONFIG.minOrderBookRatio))return reject("دفتر الأوامر لا يطابق الشرط");
  if(score<CONFIG.minScore)return reject("الدرجة أقل من الحد");if(!Number.isFinite(br)||!Number.isFinite(rsi))return reject("مؤشر غير صالح");
  return {symbol:c.symbol,price:last.close,change24h:c.change24h,change5m,score:Math.max(0,Math.min(100,score)),kind:breakout?"اختراق":near?"قرب اختراق":"تجميع مبكر",rsi,volumeMultiplier:vm,buyRatio:br,rangePct,bookRatio:book,reasons,candleTime:last.openTime};
}
function esc(s:string):string{return s.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");}
function buildMessage(signals:Signal[]):string {
  const blocks=signals.map((s,i)=>[
    `${i+1}) <b>${esc(s.symbol)}</b> — <b>${s.score}/100</b>`,
    `🎯 النوع: <b>${esc(s.kind)}</b>`,
    `💰 السعر: <code>${fmtPrice(s.price)}</code>`,
    `⏱️ تغير آخر شمعة 5m: ${fmtPct(s.change5m)}`,
    `📈 تغير 24 ساعة: ${fmtPct(s.change24h)}`,
    `📊 الحجم مقابل السابق: ${s.volumeMultiplier.toFixed(2)}x`,
    `🛒 نسبة الشراء السوقي: ${(s.buyRatio*100).toFixed(1)}%`,
    `📉 RSI: ${s.rsi.toFixed(1)}`,
    `📚 دفتر الأوامر: ${s.bookRatio===null?"غير متاح":s.bookRatio.toFixed(2)+"x"}`,
    `🔎 الأسباب:\n${s.reasons.map(x=>"• "+esc(x)).join("\n")}`,
  ].join("\n"));
  return ["🎯 <b>القناص V18.0 — SNIPER CRON PRO</b>","تنبيهات آلية فقط؛ العملات المستقرة مستبعدة؛ ليست توصية شراء ولا ضمانًا للصعود.","",...blocks,"","⚠️ انتبه للسيولة والانزلاق السعري والمخاطر قبل أي قرار."].join("\n\n");
}

let cycleRunning = false;
async function runCycle():Promise<void> {
  if (cycleRunning) { console.warn("تخطي دورة: الفحص السابق ما زال يعمل."); return; }
  cycleRunning = true;
  try {
    await runCycleInternal();
  } finally {
    cycleRunning = false;
  }
}
async function runCycleInternal():Promise<void> {
  const start=Date.now(), deadline=start+CONFIG.maxRuntimeMs;rejects.clear();
  console.log(`\n🎯 القناص V18.0 — ${new Date().toISOString()}`);
  let candidates:Candidate[];
  try{candidates=await getCandidates(deadline);}catch(e){console.error(e);await sendTelegram("⚠️ القناص V18.0: تعذّر جلب قائمة العملات من Binance.");return;}
  console.log(`العملات المرشحة: ${candidates.length}`);
  const signals:Signal[]=[];const bookSet=new Set(candidates.slice(0,CONFIG.orderBookTopCandidates).map(x=>x.symbol));
  for(let i=0;i<candidates.length&&Date.now()<deadline;i+=CONFIG.batchSize){
    const batch=candidates.slice(i,i+CONFIG.batchSize);
    const results=await Promise.all(batch.map(c=>analyze(c,deadline,bookSet.has(c.symbol)).catch(e=>{console.error(`خطأ تحليل ${c.symbol}`,e);return null;})));
    for(const s of results)if(s)signals.push(s);
    if(i+CONFIG.batchSize<candidates.length)await sleep(Math.min(CONFIG.batchDelayMs,Math.max(0,deadline-Date.now())));
  }
  signals.sort((a,b)=>b.score-a.score);const now=Date.now();
  const selected=signals.filter(s=>now-(sentAt.get(s.symbol)??0)>=CONFIG.cooldownMs).slice(0,CONFIG.maxSignalsPerCycle);
  if(selected.length){if(await sendTelegram(buildMessage(selected))){for(const s of selected)sentAt.set(s.symbol,now);console.log(`أُرسلت ${selected.length} إشارة.`);}else console.error("لم يؤكد Telegram الإرسال؛ لم نسجل الإشارات كمرسلة.");}
  else console.log(`لا توجد إشارات جديدة بدرجة ${CONFIG.minScore}+.`);
  const elapsed=(Date.now()-start)/1000;const summary=[...rejects.entries()].sort((a,b)=>b[1]-a[1]).slice(0,5).map(([k,v])=>`${k}: ${v}`).join(" | ");
  console.log(`انتهى الفحص خلال ${elapsed.toFixed(1)} ثانية؛ إشارات مطابقة: ${signals.length}.`);if(CONFIG.logRejected)console.log(`أسباب الرفض: ${summary||"لا يوجد"}`);
  if(CONFIG.sendDiagnosticsToTelegram)await sendTelegram(`🧪 <b>تشخيص V18.0</b>\nالمرشحات: ${candidates.length}\nالإشارات: ${signals.length}\nالوقت: ${elapsed.toFixed(1)} ثانية\nأسباب الرفض: ${esc(summary||"لا يوجد")}`);
}
async function main():Promise<void>{
  console.log("تشغيل القناص V18.0 بشكل مستمر — تنبيهات فقط.");
  if(!Deno.env.get("TELEGRAM_BOT_TOKEN")||!Deno.env.get("TELEGRAM_CHAT_ID")){console.error("ضع TELEGRAM_BOT_TOKEN و TELEGRAM_CHAT_ID كمتغيرات بيئة.");Deno.exit(1);}
  while(true){const start=Date.now();await runCycle();await sleep(Math.max(1000,CONFIG.scanIntervalMs-(Date.now()-start)));}
}
// Deno Deploy Cron: تشغيل دورة فحص واحدة كل دقيقة.
Deno.cron("sniper-scan-every-minute", "* * * * *", async () => {
  console.log("⏰ بدأ الفحص المجدول كل دقيقة.");
  await runCycle();
});

if (import.meta.main) await main();

export default async function handler():Promise<void>{
  if(!Deno.env.get("TELEGRAM_BOT_TOKEN")||!Deno.env.get("TELEGRAM_CHAT_ID")){console.error("ناقص TELEGRAM_BOT_TOKEN أو TELEGRAM_CHAT_ID.");return;}
  await runCycle();
}
