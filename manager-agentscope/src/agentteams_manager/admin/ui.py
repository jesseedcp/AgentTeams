"""Dependency-free operations console shipped inside the Manager image."""

ADMIN_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AgentTeams Manager</title>
<style>
:root{color-scheme:dark;--bg:#07100f;--panel:#0c1715;--line:#22332f;
--text:#e8f2ef;--muted:#8ca39d;--accent:#5ee6b4;--warn:#f7c86a;
--danger:#ff7c78}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);
font:14px/1.45 ui-monospace,SFMono-Regular,Consolas,monospace}
button,input,textarea{font:inherit}.shell{display:grid;
grid-template-columns:220px minmax(480px,1fr) 330px;min-height:100vh}
.rail{border-right:1px solid var(--line);padding:26px 18px}.brand{font-size:17px;
letter-spacing:.04em;margin-bottom:30px}.dot{color:var(--accent)}
nav button{width:100%;border:0;border-left:2px solid transparent;background:none;
color:var(--muted);padding:10px 12px;text-align:left;cursor:pointer}
nav button.on{border-color:var(--accent);color:var(--text);background:#0f211d}
main{padding:28px 32px;min-width:0}.eyebrow{color:var(--accent);
text-transform:uppercase;letter-spacing:.15em;font-size:11px}
.heading{display:flex;align-items:flex-end;justify-content:space-between;gap:20px}
h1{font:500 28px/1.2 system-ui;margin:8px 0 24px}
.toolbar{display:flex;gap:8px;margin-bottom:22px}.toolbar[hidden]{display:none}
button.action{border:1px solid var(--line);background:#10231f;color:var(--text);
padding:8px 12px;cursor:pointer}button.action:hover{border-color:var(--accent)}
button.primary{background:var(--accent);color:#052019;border-color:var(--accent)}
button.danger{color:var(--danger)}button:disabled{opacity:.55;cursor:wait}
.status{display:flex;gap:22px;border-block:1px solid var(--line);padding:13px 0;
margin-bottom:26px;color:var(--muted)}.status b{color:var(--text);font-weight:500}
.notice{min-height:1.5em;color:var(--muted);margin:-16px 0 14px}
.notice.bad{color:var(--danger)}.notice.good{color:var(--accent)}
.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse}
th,td{padding:11px 9px;border-bottom:1px solid var(--line);text-align:left;
vertical-align:top;max-width:270px;overflow:hidden;text-overflow:ellipsis}
th{color:var(--muted);font-weight:400;white-space:nowrap}
tbody tr{cursor:pointer}tbody tr:hover{background:#0e1c19}
.row-actions{display:flex;gap:6px;white-space:nowrap}.row-actions button{padding:4px 7px}
.empty{color:var(--muted);padding:30px 9px}.inspector{border-left:1px solid
var(--line);padding:28px 22px;background:var(--panel);overflow:auto}
pre{white-space:pre-wrap;word-break:break-word;color:#bcd0cb}
.login{position:fixed;inset:0;background:#06100ef2;display:grid;place-items:center;
z-index:5}.login form{width:min(380px,calc(100vw - 32px));border:1px solid
var(--line);padding:28px;background:var(--panel)}input,textarea{width:100%;
padding:11px;background:#07100f;border:1px solid var(--line);color:var(--text)}
.login input{margin:16px 0}.login button{width:100%;padding:10px}
.error{color:var(--warn);min-height:1.5em}
dialog{width:min(680px,calc(100vw - 32px));border:1px solid var(--line);
background:var(--panel);color:var(--text);padding:0}
dialog::backdrop{background:#020807d9}.dialog-head{padding:22px 24px 0}
.dialog-body{padding:10px 24px 22px}label{display:block;color:var(--muted);
margin:12px 0 6px}textarea{min-height:270px;resize:vertical;tab-size:2}
.confirm{display:flex;align-items:flex-start;gap:9px;color:var(--warn);
margin:14px 0}.confirm input{width:auto;margin-top:3px}
.dialog-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:18px}
@media(max-width:900px){.shell{grid-template-columns:160px 1fr}
.inspector{display:none}}@media(max-width:620px){.shell{display:block}.rail{border:0;
border-bottom:1px solid var(--line)}nav{display:flex;overflow:auto}
nav button{min-width:max-content}main{padding:22px 16px}}
@media(prefers-reduced-motion:no-preference){main{animation:enter .28s ease-out}
@keyframes enter{from{opacity:0;transform:translateY(5px)}}}
</style>
</head>
<body>
<div class="shell">
<aside class="rail"><div class="brand"><span class="dot">●</span> AgentTeams</div>
<nav id="nav" aria-label="管理区域"></nav></aside>
<main>
<div class="heading"><div><div class="eyebrow">Manager control plane</div>
<h1 id="title">Overview</h1></div>
<div class="toolbar" id="toolbar" hidden>
<button class="action primary" id="create">创建</button>
<button class="action" id="refresh">刷新</button></div></div>
<div class="status" id="status"><span>STATUS <b>CONNECTING</b></span></div>
<div class="notice" id="notice" role="status" aria-live="polite"></div>
<div class="table-wrap"><table><thead id="head"></thead>
<tbody id="body"></tbody></table></div></main>
<aside class="inspector"><div class="eyebrow">Inspector</div>
<pre id="detail">选择一行，查看其完整持久状态。</pre></aside>
</div>
<div class="login" id="login"><form id="loginForm">
<div class="eyebrow">Restricted operations surface</div><h1>管理员令牌</h1>
<p>令牌只保存在本页内存中；刷新或关闭页面后会清除。</p>
<label for="token">Bearer token</label><input id="token" type="password"
autocomplete="current-password" required>
<div class="error" id="loginError" role="alert"></div>
<button class="action primary">连接</button></form></div>
<dialog id="editor" aria-labelledby="editorTitle"><form method="dialog"
id="editorForm"><div class="dialog-head"><div class="eyebrow" id="editorKind">
Resource operation</div><h1 id="editorTitle">编辑资源</h1></div>
<div class="dialog-body"><p id="editorHelp"></p>
<label for="payload">请求内容（JSON）</label>
<textarea id="payload" spellcheck="false" required></textarea>
<label class="confirm"><input id="confirmed" type="checkbox">
<span>我确认执行危险操作（删除、关闭、替换运行时/镜像、移除参与者或重大计划修改）</span>
</label><div class="error" id="editorError" role="alert"></div>
<div class="dialog-actions"><button class="action" value="cancel">取消</button>
<button class="action primary" id="submit" value="default">提交</button>
</div></div></form></dialog>
<script>
const sections=["overview","sessions","confirmations","projects","workers","teams",
"heartbeat","runtime"];const writable=new Set(["projects","workers","teams"]);
let active="overview",token="",rows=[],edit={method:"POST",resource:"",name:""};
const nav=document.querySelector("#nav"),body=document.querySelector("#body");
const head=document.querySelector("#head"),login=document.querySelector("#login");
const editor=document.querySelector("#editor"),notice=document.querySelector("#notice");
const templates={
workers:{name:"worker-name",runtime:"copaw",model:"qwen3.6-plus"},
teams:{name:"team-name",leader_name:"leader-worker",worker_names:[],
description:""},
projects:{title:"项目名称",description:"项目目标",
plan:"1. 计划第一步\\n2. 计划第二步",participants:["worker-name"]}
};
sections.forEach(section=>{const button=document.createElement("button");
button.textContent=section;button.dataset.section=section;
button.onclick=()=>load(section);nav.append(button)});
function text(value){if(value===null||value===undefined)return"—";
if(typeof value==="object")return JSON.stringify(value);return String(value)}
function resourceURL(section,name=""){const base=writable.has(section)?
"api/v1/"+section:"api/"+section;return name?base+"/"+encodeURIComponent(name):base}
function setNotice(message,kind=""){notice.textContent=message;
notice.className="notice "+kind}
async function request(url,options={}){
const{returnResponse=false,...fetchOptions}=options;
const headers={Authorization:"Bearer "+token,...(fetchOptions.headers||{})};
const response=await fetch(url,{...fetchOptions,headers});
let data={};try{data=await response.json()}catch{}
if(response.status===401){login.style.display="grid";throw Error("令牌无效或已过期")}
if(!response.ok){const error=data.error||{};
const detail=error.details?" "+JSON.stringify(error.details):"";
throw Error((error.message||("HTTP "+response.status))+detail)}
return returnResponse?{status:response.status,data}:data}
async function mutate(url,method,payload){
const idempotencyKey=crypto.randomUUID();
for(let attempt=0;attempt<150;attempt++){
const result=await request(url,{method,returnResponse:true,
headers:{"Content-Type":"application/json",
"Idempotency-Key":idempotencyKey},body:JSON.stringify(payload)});
if(result.status!==202)return result.data;
if((result.data.error||{}).code!=="effect_pending")return result.data;
setNotice("控制器正在完成资源配置…");
await new Promise(resolve=>setTimeout(resolve,2000))}
throw Error("操作仍在后台执行，请稍后刷新查看最终状态")}
async function load(section=active){active=section;
document.querySelector("#title").textContent=section[0].toUpperCase()+section.slice(1);
document.querySelectorAll("nav button").forEach(button=>
button.classList.toggle("on",button.dataset.section===section));
document.querySelector("#toolbar").hidden=!writable.has(section);
setNotice("正在读取…");try{const data=await request(resourceURL(section));
rows=data.items||[data];render(rows);login.style.display="none";setNotice("")}
catch(error){setNotice(error.message,"bad");throw error}}
function render(items){head.replaceChildren();body.replaceChildren();
const keys=[...new Set(items.flatMap(item=>Object.keys(item)))].slice(0,6);
const header=document.createElement("tr");keys.forEach(key=>{const th=
document.createElement("th");th.textContent=key;header.append(th)});
if(writable.has(active)){const th=document.createElement("th");
th.textContent="操作";header.append(th)}head.append(header);
if(!items.length){const row=document.createElement("tr"),cell=
document.createElement("td");cell.className="empty";
cell.colSpan=Math.max(1,keys.length+(writable.has(active)?1:0));
cell.textContent="暂无记录";row.append(cell);body.append(row)}
items.forEach(item=>{const row=document.createElement("tr");
keys.forEach(key=>{const cell=document.createElement("td");
cell.textContent=text(item[key]);row.append(cell)});
if(writable.has(active)){const actions=document.createElement("td");
actions.className="row-actions";
const change=document.createElement("button");change.className="action";
change.textContent="编辑";change.onclick=event=>{event.stopPropagation();
openEditor("PATCH",active,item.name||item.project_id,item)};
const remove=document.createElement("button");
remove.className="action danger";remove.textContent=active==="projects"?"关闭":"删除";
remove.onclick=event=>{event.stopPropagation();
openEditor("DELETE",active,item.name||item.project_id,item)};
actions.append(change,remove);row.append(actions)}
row.onclick=()=>document.querySelector("#detail").textContent=
JSON.stringify(item,null,2);body.append(row)});
document.querySelector("#status").innerHTML="<span>STATUS <b>ONLINE</b></span>"+
"<span>RECORDS <b>"+items.length+"</b></span>"}
function patchTemplate(resource,item){if(resource==="workers")return{
model:item.model||""};if(resource==="teams")return{
leader_name:item.leader,worker_names:item.workers||[],
description:(item.spec||{}).description||""};return{
plan:(item.metadata||{}).plan||"",change_kind:"minor",reason:"说明修改原因"}}
function openEditor(method,resource,name="",item=null){edit={method,resource,name};
document.querySelector("#editorKind").textContent=method+" / "+resource;
document.querySelector("#editorTitle").textContent=method==="POST"?"创建资源":
method==="PATCH"?"修改 "+name:(resource==="projects"?"关闭 ":"删除 ")+name;
document.querySelector("#editorHelp").textContent=method==="DELETE"?
"此操作不可由页面自动撤销，必须勾选确认。":
"只提交需要设置或修改的字段。字段名使用下方模板格式。";
const payload=method==="POST"?templates[resource]:
method==="PATCH"?patchTemplate(resource,item):{};
document.querySelector("#payload").value=JSON.stringify(payload,null,2);
document.querySelector("#payload").disabled=method==="DELETE";
document.querySelector("#confirmed").checked=method==="DELETE";
document.querySelector("#editorError").textContent="";editor.showModal()}
document.querySelector("#create").onclick=()=>openEditor("POST",active);
document.querySelector("#refresh").onclick=()=>load();
document.querySelector("#editorForm").onsubmit=async event=>{
if(event.submitter&&event.submitter.value==="cancel")return;
event.preventDefault();const submit=document.querySelector("#submit");
submit.disabled=true;try{let payload=edit.method==="DELETE"?{}:
JSON.parse(document.querySelector("#payload").value);
if(!payload||Array.isArray(payload)||typeof payload!=="object")
throw Error("请求内容必须是 JSON 对象");
if(document.querySelector("#confirmed").checked)payload.confirmed=true;
const name=edit.method==="POST"?"":edit.name;
await mutate(resourceURL(edit.resource,name),edit.method,payload);
editor.close();setNotice("操作已提交并完成。","good");await load()}
catch(error){document.querySelector("#editorError").textContent=error.message}
finally{submit.disabled=false}};
document.querySelector("#loginForm").onsubmit=async event=>{event.preventDefault();
token=document.querySelector("#token").value;
document.querySelector("#loginError").textContent="";
try{await load(active)}catch(error){
document.querySelector("#loginError").textContent=error.message}};
login.style.display="grid";
</script>
</body></html>"""
