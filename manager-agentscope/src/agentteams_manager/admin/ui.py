"""Dependency-free operations console shipped inside the Manager image."""

ADMIN_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AgentTeams Manager</title>
<style>
:root{color-scheme:dark;--bg:#07100f;--panel:#0c1715;--line:#22332f;
--text:#e8f2ef;--muted:#8ca39d;--accent:#5ee6b4;--warn:#f7c86a}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);
font:14px/1.45 ui-monospace,SFMono-Regular,Consolas,monospace}
.shell{display:grid;grid-template-columns:220px minmax(480px,1fr) 330px;
min-height:100vh}.rail{border-right:1px solid var(--line);padding:26px 18px}
.brand{font-size:17px;letter-spacing:.04em;margin-bottom:30px}.dot{color:var(--accent)}
nav button{width:100%;border:0;border-left:2px solid transparent;background:none;
color:var(--muted);padding:10px 12px;text-align:left;font:inherit;cursor:pointer}
nav button.on{border-color:var(--accent);color:var(--text);background:#0f211d}
main{padding:28px 32px}.eyebrow{color:var(--accent);text-transform:uppercase;
letter-spacing:.15em;font-size:11px}h1{font:500 28px/1.2 system-ui;margin:8px 0 24px}
.status{display:flex;gap:22px;border-block:1px solid var(--line);padding:13px 0;
margin-bottom:26px;color:var(--muted)}.status b{color:var(--text);font-weight:500}
table{width:100%;border-collapse:collapse}th,td{padding:11px 9px;border-bottom:
1px solid var(--line);text-align:left;vertical-align:top}th{color:var(--muted);
font-weight:400}tbody tr{cursor:pointer}tbody tr:hover{background:#0e1c19}
.inspector{border-left:1px solid var(--line);padding:28px 22px;background:var(--panel)}
pre{white-space:pre-wrap;word-break:break-word;color:#bcd0cb}.login{position:fixed;
inset:0;background:#06100eea;display:grid;place-items:center}.login form{width:360px;
border:1px solid var(--line);padding:28px;background:var(--panel)}input{width:100%;
margin:16px 0;padding:11px;background:#07100f;border:1px solid var(--line);
color:var(--text)}.login button{width:100%;padding:10px;background:var(--accent);
border:0;color:#052019;font:inherit}.error{color:var(--warn);min-height:1.5em}
@media(max-width:900px){.shell{grid-template-columns:160px 1fr}.inspector{display:none}}
@media(prefers-reduced-motion:no-preference){main{animation:enter .28s ease-out}
@keyframes enter{from{opacity:0;transform:translateY(5px)}}}
</style>
</head>
<body>
<div class="shell">
<aside class="rail"><div class="brand"><span class="dot">●</span> AgentTeams</div>
<nav id="nav"></nav></aside>
<main><div class="eyebrow">Manager control plane</div><h1 id="title">Overview</h1>
<div class="status" id="status"><span>STATUS <b>CONNECTING</b></span></div>
<table><thead id="head"></thead><tbody id="body"></tbody></table></main>
<aside class="inspector"><div class="eyebrow">Inspector</div><pre id="detail">
Select a row to inspect its durable state.</pre></aside>
</div>
<div class="login" id="login"><form id="loginForm"><div class="eyebrow">
Restricted operations surface</div><h1>Admin token</h1><input id="token"
type="password" autocomplete="current-password" placeholder="Bearer token">
<div class="error" id="error"></div><button>Connect</button></form></div>
<script>
const sections=["overview","sessions","confirmations","projects","workers","teams",
"heartbeat","runtime"];let active="overview",token=sessionStorage.getItem("atm")||"";
const nav=document.querySelector("#nav"),body=document.querySelector("#body");
sections.forEach(s=>{const b=document.createElement("button");b.textContent=s;
b.onclick=()=>load(s);b.dataset.s=s;nav.append(b)});
function value(v){if(v===null||v===undefined)return"—";if(typeof v==="object")
return JSON.stringify(v);return String(v)}
async function load(section){active=section;document.querySelector("#title").textContent=
section[0].toUpperCase()+section.slice(1);document.querySelectorAll("nav button").
forEach(b=>b.classList.toggle("on",b.dataset.s===section));
const r=await fetch("api/"+section,{headers:{Authorization:"Bearer "+token}});
if(r.status===401){document.querySelector("#login").style.display="grid";throw Error("unauthorized")}
const data=await r.json(),rows=data.items||[data],keys=[...new Set(rows.flatMap(o=>
Object.keys(o)))].slice(0,6);document.querySelector("#head").innerHTML="<tr>"+
keys.map(k=>"<th>"+k+"</th>").join("")+"</tr>";body.innerHTML="";
rows.forEach(row=>{const tr=document.createElement("tr");keys.forEach(k=>{const td=
document.createElement("td");td.textContent=value(row[k]);tr.append(td)});tr.onclick=()=>
document.querySelector("#detail").textContent=JSON.stringify(row,null,2);body.append(tr)});
document.querySelector("#status").innerHTML="<span>STATUS <b>ONLINE</b></span><span>"+
"RECORDS <b>"+rows.length+"</b></span>";document.querySelector("#login").style.display="none"}
document.querySelector("#loginForm").onsubmit=e=>{e.preventDefault();token=
document.querySelector("#token").value;sessionStorage.setItem("atm",token);load(active).
catch(()=>document.querySelector("#error").textContent="Token rejected")};
if(token)load(active).catch(()=>{});else document.querySelector("#login").style.display="grid";
</script>
</body></html>"""
