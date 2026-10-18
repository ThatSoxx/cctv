#!/usr/bin/env python3
"""
CCTV-DIY: Kamera-Server mit Weboberfläche (ein Programm, eine Datei)

Voraussetzungen:  sudo apt install ffmpeg python3-pip
                  pip install flask waitress
Start:            CCTV_PASSWORD='mind-8-zeichen' CAM_PASSWORD='kamera-pw' python3 cctv.py
Browser:          http://<server-ip>:8080
"""
import os, re, sys, time, signal, atexit, shutil, secrets, subprocess, threading
from datetime import datetime
from functools import wraps
from pathlib import Path
from urllib.parse import quote

from flask import Flask, request, session, redirect, send_from_directory, abort, jsonify, Response

# ======================= KONFIGURATION =======================
HOST, PORT = "0.0.0.0", 8080
BASE = Path(os.environ.get("CCTV_DATA", "/srv/cctv"))        # Aufnahmen, Logs, Schlüssel
REC_DIR, LOG_DIR, KEY_FILE = BASE / "recordings", BASE / "logs", BASE / "secret.key"
LIVE_DIR = Path("/dev/shm/cctv_live") if Path("/dev/shm").exists() else BASE / "live"  # RAM-Disk schont die Platte

PASSWORD = os.environ.get("CCTV_PASSWORD", "")               # Login der Weboberfläche
CAM_USER = os.environ.get("CAM_USER", "admin")
CAM_PASS = os.environ.get("CAM_PASSWORD", "")                # Passwort der Kameras

RECORD = True                 # Daueraufnahme an/aus
RECORD_STREAM = "main"        # "main" = volle Qualität (viel Platz), "sub" = klein (spart ~85 % Platz)
SEGMENT_SECONDS = 600         # eine Datei je 10 Minuten
RETENTION_DAYS = 7            # Aufnahmen älter als X Tage löschen
MIN_FREE_GB = 50              # zusätzlich: älteste löschen, wenn weniger frei

# URL-Muster. Standard = Reolink. Alternativen:
#   Hikvision: rtsp://{u}:{p}@{ip}:554/Streaming/Channels/101  (Sub: .../102)
#   Dahua:     rtsp://{u}:{p}@{ip}:554/cam/realmonitor?channel=1&subtype=0  (Sub: subtype=1)
URL_MAIN = "rtsp://{u}:{p}@{ip}:554/h264Preview_01_main"
URL_SUB = "rtsp://{u}:{p}@{ip}:554/h264Preview_01_sub"

CAMERAS = [  # (Name, IP-Adresse)
    ("Haustür", "192.168.10.11"), ("Einfahrt", "192.168.10.12"),
    ("Garage", "192.168.10.13"), ("Garten Süd", "192.168.10.14"),
    ("Garten Nord", "192.168.10.15"), ("Terrasse", "192.168.10.16"),
    ("Hinterhof", "192.168.10.17"), ("Seite West", "192.168.10.18"),
    ("Seite Ost", "192.168.10.19"), ("Carport", "192.168.10.20"),
]
FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
# =============================================================

STOP = threading.Event()
CAMS = {}
for i, (name, ip) in enumerate(CAMERAS, 1):
    u, p = quote(CAM_USER, safe=""), quote(CAM_PASS, safe="")
    CAMS[f"cam{i}"] = dict(name=name, main=URL_MAIN.format(u=u, p=p, ip=ip), sub=URL_SUB.format(u=u, p=p, ip=ip))


class Worker(threading.Thread):
    """Hält einen ffmpeg-Prozess am Laufen (Live-HLS + optional Aufnahme) und startet ihn bei Störung neu."""

    def __init__(self, cid, kind):
        super().__init__(daemon=True)
        self.cid, self.kind, self.url = cid, kind, CAMS[cid][kind]
        self.record = RECORD and RECORD_STREAM == kind
        self.playlist = LIVE_DIR / f"{cid}_{kind}.m3u8"
        self.proc = None

    def cmd(self):
        c = [FFMPEG, "-hide_banner", "-loglevel", "warning", "-rtsp_transport", "tcp", "-timeout", "15000000",
             "-i", self.url,
             "-an", "-c:v", "copy", "-f", "hls", "-hls_time", "2", "-hls_list_size", "6",
             "-hls_flags", "delete_segments+omit_endlist+independent_segments",
             "-hls_segment_filename", str(LIVE_DIR / f"{self.cid}_{self.kind}_%05d.ts"), str(self.playlist)]
        if self.record:
            d = REC_DIR / self.cid
            d.mkdir(parents=True, exist_ok=True)
            c += ["-an", "-c:v", "copy", "-f", "segment", "-segment_time", str(SEGMENT_SECONDS),
                  "-segment_atclocktime", "1", "-segment_format", "mp4",
                  "-segment_format_options", "movflags=+frag_keyframe+empty_moov+default_base_moof",
                  "-reset_timestamps", "1", "-strftime", "1", str(d / "%Y-%m-%d_%H-%M-%S.mp4")]
        return c

    def run(self):
        backoff = 2
        log = open(LOG_DIR / f"{self.cid}_{self.kind}.log", "ab")
        while not STOP.is_set():
            self.playlist.unlink(missing_ok=True)
            started = time.time()
            self.proc = subprocess.Popen(self.cmd(), stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            while self.proc.poll() is None and not STOP.wait(5):
                ref = self.playlist.stat().st_mtime if self.playlist.exists() else started
                if time.time() - ref > 25:          # Stream hängt -> Neustart
                    break
            if self.proc.poll() is None:
                self.proc.kill()
            self.proc.wait()
            if STOP.is_set():
                break
            backoff = 2 if time.time() - started > 60 else min(backoff * 2, 30)
            STOP.wait(backoff)


def janitor():
    """Löscht alte Aufnahmen (Alter + freier Speicher)."""
    while not STOP.is_set():
        try:
            files = sorted(REC_DIR.glob("*/*.mp4"), key=lambda f: f.stat().st_mtime)
            cutoff = time.time() - RETENTION_DAYS * 86400
            keep = []
            for f in files:
                if f.stat().st_mtime < cutoff:
                    f.unlink(missing_ok=True)
                else:
                    keep.append(f)
            while keep and shutil.disk_usage(REC_DIR).free < MIN_FREE_GB * 2**30:
                f = keep.pop(0)
                if time.time() - f.stat().st_mtime > 120:
                    f.unlink(missing_ok=True)
        except Exception as e:
            print("janitor:", e, file=sys.stderr)
        STOP.wait(300)


def is_online(cid):
    p = LIVE_DIR / f"{cid}_sub.m3u8"
    return p.exists() and time.time() - p.stat().st_mtime < 15


# ======================= WEB =======================
app = Flask(__name__)
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax",
                  SESSION_COOKIE_SECURE=os.environ.get("CCTV_HTTPS") == "1",
                  PERMANENT_SESSION_LIFETIME=30 * 86400)
FAILS = {}


def need_login(f):
    @wraps(f)
    def w(*a, **k):
        if not session.get("ok"):
            if request.path.startswith(("/api", "/live", "/rec")):
                abort(401)
            return redirect("/login")
        return f(*a, **k)
    return w


LOGIN = """<!doctype html><html lang="de"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Login</title>
<style>:root{color-scheme:dark}body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0e1114;color:#e6e9ec;font:16px system-ui,sans-serif}
form{background:#171b20;padding:28px;border-radius:14px;width:min(320px,90vw);display:grid;gap:12px}
input,button{padding:11px;border-radius:8px;border:1px solid #333b45;background:#222831;color:#e6e9ec;font:inherit}
button{background:#3b9eff;border-color:#3b9eff;color:#fff;cursor:pointer}.err{color:#ff7b7b;font-size:14px}</style></head>
<body><form method="post"><h2 style="margin:0">Kameras</h2>
<input type="password" name="pw" placeholder="Passwort" autofocus autocomplete="current-password">
<button>Anmelden</button><div class="err">__ERR__</div></form></body></html>"""


@app.route("/login", methods=["GET", "POST"])
def login():
    err = ""
    if request.method == "POST":
        ip = request.remote_addr
        n, t = FAILS.get(ip, (0, 0))
        if n >= 5 and time.time() - t < 600:
            err = "Zu viele Versuche. Bitte 10 Minuten warten."
        elif secrets.compare_digest(request.form.get("pw", "").encode(), PASSWORD.encode()):
            FAILS.pop(ip, None)
            session.permanent = True
            session["ok"] = True
            return redirect("/")
        else:
            FAILS[ip] = (n + 1 if time.time() - t < 600 else 1, time.time())
            err = "Falsches Passwort."
    return Response(LOGIN.replace("__ERR__", err), mimetype="text/html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/")
@need_login
def index():
    r = Response(INDEX, mimetype="text/html")
    r.headers["Cache-Control"] = "no-store"
    return r


@app.route("/api/cams")
@need_login
def api_cams():
    return jsonify([dict(id=c, name=v["name"], online=is_online(c)) for c, v in CAMS.items()])


@app.route("/live/<name>")
@need_login
def live(name):
    if not re.fullmatch(r"cam\d+_(main|sub)(\.m3u8|_\d+\.ts)", name):
        abort(404)
    return send_from_directory(LIVE_DIR, name, max_age=0)


@app.route("/api/recs/<cid>")
@need_login
def api_recs(cid):
    day = request.args.get("date", "")
    if cid not in CAMS or not re.fullmatch(r"\d{4}-\d\d-\d\d", day):
        abort(404)
    out = []
    for f in sorted((REC_DIR / cid).glob(f"{day}_*.mp4"), reverse=True):
        out.append(dict(name=f.name, time=f.name[11:19].replace("-", ":"), mb=round(f.stat().st_size / 2**20)))
    return jsonify(out)


@app.route("/rec/<cid>/<name>")
@need_login
def rec(cid, name):
    if cid not in CAMS or not re.fullmatch(r"\d{4}-\d\d-\d\d_\d\d-\d\d-\d\d\.mp4", name):
        abort(404)
    return send_from_directory(REC_DIR / cid, name, conditional=True)


INDEX = r"""<!doctype html><html lang="de"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Kameras</title>
<style>
:root{color-scheme:dark;--bg:#0e1114;--card:#171b20;--fg:#e6e9ec;--acc:#3b9eff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px system-ui,sans-serif}
header{display:flex;gap:8px;align-items:center;padding:10px 14px;background:var(--card);position:sticky;top:0;z-index:5;flex-wrap:wrap}
header h1{font-size:16px;margin:0 auto 0 0}
button,select,input{background:#222831;color:var(--fg);border:1px solid #333b45;border-radius:8px;padding:7px 12px;font:inherit;cursor:pointer}
button.on{background:var(--acc);border-color:var(--acc);color:#fff}
#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:8px;padding:8px}
.tile{position:relative;background:#000;border-radius:10px;overflow:hidden;aspect-ratio:16/9;cursor:pointer}
.tile video{width:100%;height:100%;object-fit:contain;display:block}
.tag{position:absolute;left:8px;top:8px;background:#000a;padding:3px 8px;border-radius:6px;font-size:13px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#e5484d;margin-right:6px}.dot.ok{background:#30a46c}
#big{position:fixed;inset:0;background:#000;z-index:10;display:none}
#big video{width:100%;height:100%;object-fit:contain}
#big button{position:absolute;right:12px;top:12px;z-index:2}
#big .tag{font-size:15px}
#recv{display:none;padding:12px}
#rp{width:100%;max-height:62vh;background:#000;border-radius:10px;margin-top:10px}
#files{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
</style></head><body>
<header><h1>Kameras</h1>
<button id="t1" class="on" onclick="tab('live')">Live</button>
<button id="t2" onclick="tab('rec')">Aufnahmen</button>
<a href="/logout"><button>Abmelden</button></a></header>
<div id="grid"></div>
<div id="big"><button onclick="closeBig()">Schließen ✕</button><span class="tag" id="bigtag"></span><video id="bigv" muted autoplay playsinline controls></video></div>
<div id="recv"><div style="display:flex;gap:8px;flex-wrap:wrap"><select id="rc" onchange="loadRecs()"></select>
<input type="date" id="rd" onchange="loadRecs()"></div><div id="files"></div><video id="rp" controls playsinline></video></div>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.17/dist/hls.min.js"></script>
<script>
const $=s=>document.querySelector(s);let cams=[];
async function api(u){const r=await fetch(u);if(r.status==401){location='/login';return[]}return r.json()}
function kill(v){if(v._h){v._h.destroy();v._h=null}v.removeAttribute('src');v.load()}
function halt(v){v._want=null;kill(v)}
function play(v,url){
  kill(v);v._want=url;
  if(window.Hls&&Hls.isSupported()){
    const h=new Hls({maxBufferLength:8,liveSyncDurationCount:3,liveMaxLatencyDurationCount:8});
    h.on(Hls.Events.ERROR,(_,d)=>{if(d.fatal){kill(v);setTimeout(()=>{if(v.isConnected&&v._want===url)play(v,url)},4000)}});
    h.loadSource(url);h.attachMedia(v);v._h=h;
  }else{v.src=url;v.onerror=()=>setTimeout(()=>{if(v._want===url)play(v,url)},4000)}
  v.play().catch(()=>{});
}
function buildGrid(){
  const g=$('#grid');g.innerHTML='';
  cams.forEach(c=>{
    const d=document.createElement('div');d.className='tile';
    d.innerHTML=`<span class="tag"><span class="dot" id="d_${c.id}"></span>${c.name}</span><video muted autoplay playsinline></video>`;
    d.onclick=()=>openBig(c);g.appendChild(d);c.v=d.querySelector('video');
  });
}
const startGrid=()=>cams.forEach(c=>play(c.v,`/live/${c.id}_sub.m3u8`));
const stopGrid=()=>cams.forEach(c=>halt(c.v));
function openBig(c){$('#bigtag').textContent=c.name;$('#big').style.display='block';play($('#bigv'),`/live/${c.id}_main.m3u8`)}
function closeBig(){$('#big').style.display='none';halt($('#bigv'))}
document.addEventListener('keydown',e=>{if(e.key=='Escape')closeBig()});
function tab(t){
  const live=t=='live';$('#t1').className=live?'on':'';$('#t2').className=live?'':'on';
  $('#grid').style.display=live?'grid':'none';$('#recv').style.display=live?'none':'block';
  if(live)startGrid();else{stopGrid();loadRecs()}
}
async function loadRecs(){
  const c=$('#rc').value,d=$('#rd').value,r=await api(`/api/recs/${c}?date=${d}`),f=$('#files');
  f.innerHTML=r.length?'':'Keine Aufnahmen an diesem Tag.';
  r.forEach(x=>{const b=document.createElement('button');b.textContent=`${x.time} (${x.mb} MB)`;
    b.onclick=()=>{const p=$('#rp');p.src=`/rec/${c}/${x.name}`;p.play()};f.appendChild(b)});
}
async function status(){const s=await api('/api/cams');s.forEach(c=>{const e=$('#d_'+c.id);if(e)e.className='dot'+(c.online?' ok':'')})}
(async()=>{
  cams=await api('/api/cams');buildGrid();startGrid();status();setInterval(status,10000);
  $('#rc').innerHTML=cams.map(c=>`<option value="${c.id}">${c.name}</option>`).join('');
  $('#rd').value=new Date(Date.now()-new Date().getTimezoneOffset()*6e4).toISOString().slice(0,10);
})();
</script></body></html>"""


def shutdown():
    STOP.set()
    for w in WORKERS:
        if w.proc and w.proc.poll() is None:
            w.proc.terminate()


WORKERS = []

if __name__ == "__main__":
    if len(PASSWORD) < 8:
        sys.exit("Bitte starten mit: CCTV_PASSWORD='mindestens-8-zeichen' CAM_PASSWORD='...' python3 cctv.py")
    for d in (REC_DIR, LOG_DIR, LIVE_DIR):
        d.mkdir(parents=True, exist_ok=True)
    for f in LIVE_DIR.glob("cam*"):
        f.unlink(missing_ok=True)
    if not KEY_FILE.exists():
        KEY_FILE.write_text(secrets.token_hex(32))
        KEY_FILE.chmod(0o600)
    app.secret_key = KEY_FILE.read_text().strip()

    for cid in CAMS:
        for kind in ("main", "sub"):
            w = Worker(cid, kind)
            WORKERS.append(w)
            w.start()
            time.sleep(0.3)  # Kameras nicht gleichzeitig überfahren
    threading.Thread(target=janitor, daemon=True).start()
    atexit.register(shutdown)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    print(f"Läuft auf http://{HOST}:{PORT}  |  Aufnahmen: {REC_DIR}")
    try:
        from waitress import serve
        serve(app, host=HOST, port=PORT, threads=32)
    except ImportError:
        app.run(host=HOST, port=PORT, threaded=True)
