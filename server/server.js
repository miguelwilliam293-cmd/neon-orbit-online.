'use strict';
// NEON ORBIT — servidor WebSocket + ranking, sem dependências npm.
// Para produção: execute atrás de proxy HTTPS (Render, Railway, VPS etc.).
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'ranking.json');
const PUBLIC_DIR = path.resolve(__dirname, '../public');
const MAX_ROOMS = 300;
const MAX_CLIENTS = 1000;
const rooms = new Map();
const clients = new Set();
const ipLimits = new Map();
let board = {solo:[],endless:[],online:[]};
try { const old = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); for(const k of Object.keys(board)) if(Array.isArray(old[k])) board[k] = old[k].slice(0,500); } catch (_) {}
function saveBoard() {
  try {
    fs.mkdirSync(DATA_DIR,{recursive:true});
    const tmp = DATA_FILE + '.' + process.pid + '.tmp';
    fs.writeFileSync(tmp,JSON.stringify(board,null,2)); fs.renameSync(tmp,DATA_FILE);
  } catch(e){console.error('Erro ao salvar ranking (disco persistente?):',e.message)}
}
function nameOf(x) { return String(x||'Orbital').normalize('NFKC').replace(/[^\p{L}\p{N} _-]/gu,'').trim().slice(0,16)||'Orbital'; }
function ipOf(req){return (req.socket.remoteAddress||'local').slice(0,70)}
function allow(ip,key,ms,limit=1){const k=ip+'|'+key, now=Date.now(), v=ipLimits.get(k)||[];const recent=v.filter(t=>now-t<ms); if(recent.length>=limit){ipLimits.set(k,recent);return false}recent.push(now);ipLimits.set(k,recent);return true}
function json(res,status,value){const s=JSON.stringify(value);res.writeHead(status,{'content-type':'application/json; charset=utf-8','cache-control':'no-store','access-control-allow-origin':'*','access-control-allow-methods':'GET, POST, OPTIONS','access-control-allow-headers':'Content-Type','x-content-type-options':'nosniff'});res.end(s)}
function addScore(data, mode, ip){
  if(!Object.hasOwn(board,mode))return {error:'Modo inválido'};
  const points=Number(data.score), stage=Number(data.stage), duration=Number(data.duration);
  // Reduz erros e spam, mas sem contas e servidor autoritativo NÃO é anticheat.
  if(!Number.isInteger(points)||points<0||points>1000000||!Number.isInteger(stage)||stage<1||stage>9999||!Number.isFinite(duration)||duration<4||duration>14400) return {error:'Pontuação inválida'};
  if(points>Math.max(150,Math.ceil(duration*800)))return {error:'Pontuação incompatível com a duração'};
  if(!allow(ip,'score',20000,3))return {error:'Envio muito frequente'};
  const name=nameOf(data.name);
  const safe={name,score:points,stage,duration:Math.round(duration),date:new Date().toISOString().slice(0,10),mode};
  // Para evitar duplicações, manter somente a melhor pontuação do mesmo apelido.
  const arr=board[mode];const before=arr.find(x=>x.name.toLocaleLowerCase()===name.toLocaleLowerCase());
  if(!before||before.score<points){board[mode]=[...arr.filter(x=>x.name.toLocaleLowerCase()!==name.toLocaleLowerCase()),safe].sort((a,b)=>b.score-a.score||b.stage-a.stage).slice(0,200);saveBoard()}
  return {ok:true,ranking:board[mode].slice(0,20)};
}
const mime = {'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.txt':'text/plain; charset=utf-8'};
const server = http.createServer((req,res)=>{
  const pathname=new URL(req.url,'http://localhost').pathname;
  if(req.method==='OPTIONS'){res.writeHead(204,{'access-control-allow-origin':'*','access-control-allow-methods':'GET, POST, OPTIONS','access-control-allow-headers':'Content-Type'});return res.end()}
  if(pathname==='/api/health')return json(res,200,{ok:true,rooms:rooms.size,online:clients.size});
  if(pathname==='/api/ranking'&&req.method==='GET'){
    const mode=new URL(req.url,'http://localhost').searchParams.get('mode')||'solo';
    if(!Object.hasOwn(board,mode))return json(res,400,{error:'Modo inválido'});
    return json(res,200,{mode,ranking:board[mode].slice(0,30)});
  }
  if(pathname==='/api/score'&&req.method==='POST'){
    let body='';req.on('data',chunk=>{body+=chunk.toString();if(body.length>2048)req.destroy()});
    req.on('end',()=>{try{const v=JSON.parse(body),result=addScore(v,v.mode==='endless'?'endless':'solo',ipOf(req));json(res,result.error?400:200,result)}catch(e){json(res,400,{error:'JSON inválido'})}});return;
  }
  if(req.method!=='GET')return json(res,405,{error:'Método não permitido'});
  let p=path.join(PUBLIC_DIR,pathname==='/'?'index.html':pathname);
  if(!p.startsWith(PUBLIC_DIR+path.sep))return json(res,403,{error:'Acesso negado'});
  fs.readFile(p,(err,data)=>{if(err)return json(res,404,{error:'Não encontrado'});res.writeHead(200,{'content-type':mime[path.extname(p)]||'application/octet-stream','cache-control':p.endsWith('.html')?'no-store':'public, max-age=120','x-content-type-options':'nosniff'});res.end(data)});
});

// Implementação pequena de RFC 6455 para mensagens JSON texto; clientes web sempre enviam frames mascarados.
class Peer {
  constructor(socket,req){this.socket=socket;this.ip=ipOf(req);this.buf=Buffer.alloc(0);this.frag=[];this.fragType=0;this.closed=false;this.room=null;this.slot=0;this.name='Orbital';this.lastInput=0;this.lastSnap=0;this.lastSeen=Date.now();socket.setNoDelay(true);socket.on('data',x=>this.receive(x));socket.on('error',()=>this.close());socket.on('close',()=>this.close());}
  send(obj){if(this.closed||!this.socket.writable)return;const body=Buffer.from(JSON.stringify(obj));if(body.length>65535)return;const h=body.length<126?Buffer.from([129,body.length]):Buffer.from([129,126,body.length>>8,body.length&255]);if(this.socket.writableLength>1024*1024){this.close();return}this.socket.write(Buffer.concat([h,body]));}
  control(op,data=Buffer.alloc(0)){if(this.closed||!this.socket.writable)return;this.socket.write(Buffer.concat([Buffer.from([128|op,data.length]),data]));}
  receive(data){this.lastSeen=Date.now();this.buf=Buffer.concat([this.buf,data]);if(this.buf.length>262144){this.close();return}
    while(this.buf.length>=2){const a=this.buf[0],b=this.buf[1],fin=!!(a&128),op=a&15,mask=!!(b&128);let n=b&127,off=2;
      if(n===126){if(this.buf.length<4)return;n=this.buf.readUInt16BE(2);off=4}
      if(n===127||n>65535||!mask){this.close();return}if(this.buf.length<off+4+n)return;
      const key=this.buf.subarray(off,off+4),payload=Buffer.from(this.buf.subarray(off+4,off+4+n));this.buf=this.buf.subarray(off+4+n);for(let i=0;i<n;i++)payload[i]^=key[i%4];
      if(op===8){this.close();return}if(op===9){this.control(10,payload);continue}if(op===10)continue;
      if(op===1||op===0){if(op===1){this.frag=[];this.fragType=1}else if(!this.fragType){this.close();return}this.frag.push(payload);if(!fin)continue;const str=Buffer.concat(this.frag);this.frag=[];this.fragType=0;if(str.length>65535){this.close();return}try{handle(this,JSON.parse(str.toString('utf8')))}catch(e){this.send({type:'error',message:'Mensagem inválida'})}}
      else {this.close();return}
    }
  }
  close(){if(this.closed)return;this.closed=true;try{this.socket.destroy()}catch(_){}clients.delete(this);leaveRoom(this)}
}
function players(room){return [...room.peers.values()].map(p=>({slot:p.slot,name:p.name})).sort((a,b)=>a.slot-b.slot)}
function sendRoom(room,obj){for(const p of room.peers.values())p.send(obj)}
function lobby(room){sendRoom(room,{type:'lobby',code:room.code,players:players(room),playing:room.playing})}
function leaveRoom(p){const r=p.room;if(!r)return;p.room=null;r.peers.delete(p.slot);
  if(p.slot===1){for(const q of r.peers.values()){q.room=null;q.send({type:'closed',message:'O criador da sala saiu.'})}rooms.delete(r.code)}
  else if(r.peers.size){sendRoom(r,{type:'left',slot:p.slot,name:p.name});lobby(r)}else rooms.delete(r.code);
}
function makeCode(){let c;do{c=crypto.randomBytes(4).toString('hex').slice(0,6).toUpperCase()}while(rooms.has(c));return c}
function handle(p,m){if(!m||typeof m!=='object'||Array.isArray(m))return;
  if(m.type==='ping'){if(Number.isFinite(m.stamp))p.send({type:'pong',stamp:m.stamp});return}
  if(m.type==='create'&&!p.room){if(rooms.size>=MAX_ROOMS){p.send({type:'error',message:'Salas lotadas'});return}const room={code:makeCode(),peers:new Map(),playing:false,created:Date.now(),started:0,lastScore:0,lastStage:1};p.slot=1;p.name=nameOf(m.name);p.room=room;room.peers.set(1,p);rooms.set(room.code,room);p.send({type:'joined',code:room.code,slot:1});lobby(room);return}
  if(m.type==='join'&&!p.room){const code=String(m.code||'').toUpperCase().replace(/[^A-F0-9]/g,'').slice(0,6),r=rooms.get(code);
    if(!r||r.playing||r.peers.size>=4){p.send({type:'error',message:'Sala não encontrada, cheia ou em andamento'});return}
    for(let slot=2;slot<=4;slot++)if(!r.peers.has(slot)){p.slot=slot;break}
    p.room=r;p.name=nameOf(m.name);r.peers.set(p.slot,p);p.send({type:'joined',code:r.code,slot:p.slot});lobby(r);return}
  const r=p.room;if(!r)return;
  if(m.type==='leave'){p.send({type:'closed',message:'Você saiu da sala'});leaveRoom(p);return}
  if(m.type==='start'&&p.slot===1&&!r.playing){if(r.peers.size<2){p.send({type:'error',message:'É preciso pelo menos 2 jogadores'});return}r.playing=true;r.started=Date.now();sendRoom(r,{type:'started',players:players(r),code:r.code});return}
  if(m.type==='input'&&r.playing&&p.slot!==1){const now=Date.now(),action=String(m.action||'');if(now-p.lastInput<25||!['flip','lane','nova'].includes(action))return;p.lastInput=now;const host=r.peers.get(1);if(host)host.send({type:'input',slot:p.slot,action,seq:Number.isInteger(m.seq)&&m.seq>0&&m.seq<=1000000000?m.seq:0});return}
  if(m.type==='snapshot'&&r.playing&&p.slot===1){const now=Date.now();if(now-p.lastSnap<65)return;p.lastSnap=now;const snap=m.data;
    if(!snap||typeof snap!=='object'||Array.isArray(snap)||!Number.isFinite(snap.score)||!Number.isInteger(snap.stage)||snap.stage<1||snap.stage>9999)return;
    r.lastScore=Math.max(0,Math.min(1000000,Math.floor(snap.score)));r.lastStage=snap.stage;
    for(const q of r.peers.values())if(q!==p&&q.socket.writableLength<20000)q.send({type:'snapshot',data:snap});return}
  if(m.type==='finish'&&r.playing&&p.slot===1){r.playing=false;const duration=(Date.now()-r.started)/1000;
    let result={ok:false};if(duration>=4){result=addScore({name:p.name,score:r.lastScore,stage:r.lastStage,duration},'online',p.ip)}
    sendRoom(r,{type:'finished',score:r.lastScore,stage:r.lastStage,rankingSaved:!!result.ok});lobby(r);return}
}
server.on('upgrade',(req,socket,head)=>{
  if(new URL(req.url,'http://localhost').pathname!=='/ws'||clients.size>=MAX_CLIENTS){socket.destroy();return}
  const key=req.headers['sec-websocket-key'];const upgrade=(req.headers.upgrade||'').toLowerCase();
  if(!key||!/^[-\w+/]{22}==$/.test(key)||upgrade!=='websocket'){socket.destroy();return}
  const hash=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+hash+'\r\n\r\n');
  const p=new Peer(socket,req);clients.add(p);if(head&&head.length)p.receive(head);
});
setInterval(()=>{for(const p of clients){if(Date.now()-p.lastSeen>120000)p.close();else p.control(9)}for(const [code,r] of rooms)if(!r.playing&&Date.now()-r.created>3600000){for(const p of r.peers.values())p.close();rooms.delete(code)}for(const [k,v] of ipLimits)if(v.every(t=>Date.now()-t>120000))ipLimits.delete(k)},30000).unref();
server.listen(PORT,'0.0.0.0',()=>console.log(`NEON ORBIT online pronto na porta ${PORT}`));