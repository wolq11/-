(function(){
if(window._aiChatLoaded)return;
window._aiChatLoaded=true;
var inIframe=(window.self!==window.top);
if(inIframe)return;
var api=window.location.origin;
var chatOpen=false;
var chatSending=false;
var chatInited=false;

var TOOL_ICONS={
    check_approval:'📋',
    search_documents:'🔍',
    check_alerts:'⚠️',
    generate_insight_report:'📊',
    query_club_data:'📈',
    list_clubs:'🏫'
};
var TOOL_NAMES={
    check_approval:'审批查询',
    search_documents:'文件搜索',
    check_alerts:'预警检查',
    generate_insight_report:'数据报告',
    query_club_data:'社团数据',
    list_clubs:'社团列表'
};

var PAGE_SUGGESTIONS={
    'upload.html':[
        {text:'📋 待审批材料',msg:'帮我看看有哪些待审批的材料'},
        {text:'🔍 搜文件',msg:'搜索活动相关文件'}
    ],
    'workload.html':[
        {text:'📋 工作量审核',msg:'查看工作量审核情况'},
        {text:'📊 数据报告',msg:'生成数据分析报告'}
    ],
    'dashboard.html':[
        {text:'🏫 社团列表',msg:'有哪些社团'},
        {text:'⚠️ 系统预警',msg:'系统有什么预警吗'}
    ],
    'club-tools.html':[
        {text:'💰 财务数据',msg:'查看社团财务数据'},
        {text:'📊 数据报告',msg:'生成数据分析报告'}
    ],
    'club-detail.html':[
        {text:'👥 成员数据',msg:'查看社团成员数据'},
        {text:'🎯 活动数据',msg:'查看社团活动数据'}
    ],
    'stats.html':[
        {text:'📊 数据报告',msg:'生成数据分析报告'},
        {text:'🏫 社团列表',msg:'有哪些社团'}
    ]
};

function getCurrentPage(){
    var p=window.location.pathname;
    var parts=p.split('/');
    return parts[parts.length-1]||'index.html';
}

function esc(s){var d=document.createElement('div');d.textContent=s;return d.innerHTML}

function renderMd(text){
    if(!text)return '';
    var h=esc(text);
    h=h.replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>');
    h=h.replace(/\*(.+?)\*/g,'<em>$1</em>');
    h=h.replace(/`(.+?)`/g,'<code style="background:rgba(102,126,234,0.1);color:#667eea;padding:1px 5px;border-radius:4px;font-size:0.9em">$1</code>');
    h=h.replace(/^### (.+)$/gm,'<div style="font-weight:700;font-size:1em;margin:8px 0 4px;color:#1a1a2e">$1</div>');
    h=h.replace(/^## (.+)$/gm,'<div style="font-weight:700;font-size:1.05em;margin:10px 0 4px;color:#1a1a2e">$1</div>');
    h=h.replace(/^# (.+)$/gm,'<div style="font-weight:800;font-size:1.1em;margin:12px 0 6px;color:#1a1a2e">$1</div>');
    h=h.replace(/^- (.+)$/gm,'<div style="padding-left:14px;position:relative;margin:2px 0"><span style="position:absolute;left:0;color:#667eea">•</span>$1</div>');
    h=h.replace(/^(\d+)\. (.+)$/gm,'<div style="padding-left:18px;position:relative;margin:2px 0"><span style="position:absolute;left:0;color:#667eea;font-weight:700">$1.</span>$2</div>');
    h=h.replace(/^---$/gm,'<hr style="border:none;border-top:1px solid #f0f0f8;margin:10px 0">');
    h=h.replace(/\n/g,'<br>');
    return h;
}

function typewriterEffect(element,html,duration,callback){
    var chars=html.split('');
    var total=chars.length;
    var step=Math.max(1,Math.floor(total/(duration/16)));
    var idx=0;
    element.innerHTML='';
    var timer=setInterval(function(){
        idx+=step;
        if(idx>=total){
            idx=total;
            clearInterval(timer);
            element.innerHTML=html;
            if(callback)callback();
            return;
        }
        element.innerHTML=chars.slice(0,idx).join('');
        var body=document.getElementById('fcBody');
        if(body)body.scrollTop=body.scrollHeight;
    },16);
}

function injectStyles(){
    var s=document.createElement('style');
    s.textContent=`
#aiChatFab{position:fixed;bottom:24px;right:24px;width:56px;height:56px;border-radius:50%;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border:none;font-size:1.5em;cursor:pointer;box-shadow:0 4px 20px rgba(102,126,234,0.4);z-index:99999;transition:all .3s;display:flex;align-items:center;justify-content:center}
#aiChatFab:hover{transform:scale(1.1);box-shadow:0 6px 28px rgba(102,126,234,0.5)}
#aiChatFab.has-new::after{content:'';position:absolute;top:2px;right:2px;width:12px;height:12px;border-radius:50%;background:#e74c3c;border:2px solid #fff;animation:fcPulse 1.5s infinite}
@keyframes fcPulse{0%,100%{transform:scale(1)}50%{transform:scale(1.3)}}
#aiChatPanel{position:fixed;bottom:90px;right:24px;width:420px;max-width:calc(100vw - 32px);height:580px;max-height:calc(100vh - 120px);background:#fff;border-radius:20px;box-shadow:0 8px 40px rgba(0,0,0,0.15);z-index:99998;display:none;flex-direction:column;overflow:hidden;border:1px solid #f0f0f8;animation:fcSlideUp .25s ease;position:fixed}
@keyframes fcSlideUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:translateY(0)}}
#aiChatPanel.open{display:flex}
.fc-header{padding:14px 18px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;display:flex;align-items:center;gap:10px;flex-shrink:0}
.fc-header .fc-avatar{width:36px;height:36px;border-radius:12px;background:rgba(255,255,255,0.2);display:flex;align-items:center;justify-content:center;font-size:1.2em}
.fc-header .fc-info{flex:1}
.fc-header .fc-name{font-weight:700;font-size:0.95em}
.fc-header .fc-status{font-size:0.7em;opacity:0.8;display:flex;align-items:center;gap:4px}
.fc-header .fc-status .fc-dot{width:6px;height:6px;border-radius:50%;background:#55efc4;animation:fcDotPulse 2s infinite}
@keyframes fcDotPulse{0%,100%{opacity:1}50%{opacity:0.4}}
.fc-header .fc-actions{display:flex;gap:6px}
.fc-header .fc-actions button{background:rgba(255,255,255,0.15);border:none;color:#fff;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:0.85em;display:flex;align-items:center;justify-content:center;transition:background .2s}
.fc-header .fc-actions button:hover{background:rgba(255,255,255,0.3)}
.fc-body{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:12px;background:#f8f8fc}
.fc-body::-webkit-scrollbar{width:4px}
.fc-body::-webkit-scrollbar-thumb{background:#ddd;border-radius:4px}
.fc-msg{display:flex;gap:8px;animation:fcMsgIn .25s ease}
@keyframes fcMsgIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.fc-msg.user{flex-direction:row-reverse}
.fc-msg .fc-bubble{max-width:82%;padding:10px 14px;border-radius:14px;font-size:0.86em;line-height:1.7;word-break:break-word}
.fc-msg.assistant .fc-bubble{background:#fff;border:1px solid #f0f0f8;color:#1a1a2e;border-top-left-radius:4px}
.fc-msg.user .fc-bubble{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border-top-right-radius:4px}
.fc-msg .fc-avatar-sm{width:28px;height:28px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:0.85em;flex-shrink:0}
.fc-msg.assistant .fc-avatar-sm{background:linear-gradient(135deg,rgba(102,126,234,0.1),rgba(118,75,162,0.1));color:#667eea}
.fc-msg.user .fc-avatar-sm{background:linear-gradient(135deg,rgba(0,184,148,0.1),rgba(85,239,196,0.1));color:#00b894}
.fc-tool-card{background:linear-gradient(135deg,rgba(102,126,234,0.04),rgba(118,75,162,0.04));border:1px solid rgba(102,126,234,0.12);border-radius:12px;padding:10px 14px;margin-top:8px;font-size:0.8em;position:relative;overflow:hidden}
.fc-tool-card::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;background:linear-gradient(180deg,#667eea,#764ba2);border-radius:3px}
.fc-tool-card .fc-tc-header{display:flex;align-items:center;gap:6px;color:#667eea;font-weight:700;margin-bottom:4px;padding-left:6px}
.fc-tool-card .fc-tc-args{color:#a0a3bd;font-size:0.9em;margin-bottom:4px;padding-left:6px}
.fc-tool-card .fc-tc-result{color:#6b6d8a;font-size:0.88em;max-height:100px;overflow:hidden;position:relative;padding-left:6px}
.fc-tool-card .fc-tc-result::after{content:'';position:absolute;bottom:0;left:0;right:0;height:24px;background:linear-gradient(transparent,rgba(102,126,234,0.04))}
.fc-typing{display:flex;gap:4px;padding:8px 14px}
.fc-typing span{width:6px;height:6px;border-radius:50%;background:#667eea;animation:fcType 1.2s infinite}
.fc-typing span:nth-child(2){animation-delay:.2s}
.fc-typing span:nth-child(3){animation-delay:.4s}
@keyframes fcType{0%,60%,100%{transform:translateY(0);opacity:.4}30%{transform:translateY(-6px);opacity:1}}
.fc-suggestions{display:flex;flex-wrap:wrap;gap:6px;margin-top:4px}
.fc-sug-wrapper{margin-top:4px;padding:8px 12px;background:rgba(102,126,234,0.03);border-radius:10px;border:1px dashed rgba(102,126,234,0.15)}
.fc-sug-label{font-size:0.72em;color:#a0a3bd;margin-bottom:4px;font-weight:600}
.fc-sug-btn{padding:5px 12px;border-radius:16px;border:1px solid rgba(102,126,234,0.2);background:rgba(102,126,234,0.06);color:#667eea;font-size:0.76em;cursor:pointer;transition:all .2s;font-weight:600}
.fc-sug-btn:hover{background:rgba(102,126,234,0.15);border-color:rgba(102,126,234,0.4);transform:translateY(-1px)}
.fc-footer{padding:12px 14px;border-top:1px solid #f0f0f8;display:flex;gap:8px;align-items:flex-end;background:#fff;flex-shrink:0}
.fc-footer textarea{flex:1;border:1.5px solid #eee;border-radius:12px;padding:10px 14px;font-size:0.86em;resize:none;outline:none;font-family:inherit;max-height:100px;min-height:40px;line-height:1.5;transition:border-color .2s;background:#fafaff;color:#1a1a2e}
.fc-footer textarea:focus{border-color:#667eea;background:#fff}
.fc-footer .fc-send{width:40px;height:40px;border-radius:12px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border:none;cursor:pointer;font-size:1.1em;display:flex;align-items:center;justify-content:center;transition:all .2s;flex-shrink:0}
.fc-footer .fc-send:hover{transform:scale(1.05)}
.fc-footer .fc-send:disabled{opacity:0.5;cursor:not-allowed;transform:none}
.fc-welcome{text-align:center;padding:30px 20px;color:#a0a3bd}
.fc-welcome .fc-w-icon{font-size:3em;margin-bottom:10px}
.fc-welcome .fc-w-title{font-size:1.05em;font-weight:700;color:#1a1a2e;margin-bottom:6px}
.fc-welcome .fc-w-desc{font-size:0.82em;line-height:1.6;margin-bottom:16px}
.fc-quick-actions{display:flex;flex-wrap:wrap;gap:6px;justify-content:center}
.fc-quick-btn{padding:6px 14px;border-radius:20px;border:1px solid rgba(102,126,234,0.2);background:rgba(102,126,234,0.06);color:#667eea;font-size:0.78em;cursor:pointer;transition:all .2s;font-weight:600}
.fc-quick-btn:hover{background:rgba(102,126,234,0.15);border-color:rgba(102,126,234,0.4);transform:translateY(-1px)}
.fc-page-hint{font-size:0.72em;color:#a0a3bd;margin-top:12px;padding-top:10px;border-top:1px solid #f0f0f8}
@media(max-width:480px){
    #aiChatPanel{bottom:0;right:0;width:100%;height:100%;max-height:100%;border-radius:0}
    #aiChatFab{bottom:16px;right:16px;width:48px;height:48px;font-size:1.2em}
}
`;
    document.head.appendChild(s);
}

function injectHTML(){
    if(document.getElementById('aiChatFab'))return;
    var fab=document.createElement('button');
    fab.id='aiChatFab';
    fab.innerHTML='🤖';
    fab.title='AI 助手小济';
    fab.onclick=toggleChat;
    document.body.appendChild(fab);

    var panel=document.createElement('div');
    panel.id='aiChatPanel';
    panel.innerHTML=`
<div class="fc-header">
    <div class="fc-avatar">🤖</div>
    <div class="fc-info">
        <div class="fc-name">小济</div>
        <div class="fc-status"><span class="fc-dot"></span> AI 智能助手 · 在线</div>
    </div>
    <div class="fc-actions">
        <button onclick="window._aiChatClear()" title="清空对话">🗑</button>
        <button onclick="window._aiChatToggle()" title="关闭">✕</button>
    </div>
</div>
<div class="fc-body" id="fcBody"></div>
<div class="fc-footer">
    <textarea id="fcInput" rows="1" placeholder="问我任何关于社团的问题..." onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();window._aiChatSend()}"></textarea>
    <button class="fc-send" id="fcSendBtn" onclick="window._aiChatSend()">➤</button>
</div>
`;
    document.body.appendChild(panel);

    var input=document.getElementById('fcInput');
    input.addEventListener('input',function(){
        this.style.height='auto';
        this.style.height=Math.min(this.scrollHeight,100)+'px';
    });
}

function toggleChat(){
    var panel=document.getElementById('aiChatPanel');
    var fab=document.getElementById('aiChatFab');
    chatOpen=!chatOpen;
    if(chatOpen){
        panel.classList.add('open');
        fab.innerHTML='✕';
        fab.classList.remove('has-new');
        if(!chatInited){
            loadHistory();
            chatInited=true;
        }else{
            showWelcome();
        }
        setTimeout(function(){document.getElementById('fcInput').focus()},200);
    }else{
        panel.classList.remove('open');
        fab.innerHTML='🤖';
    }
}

window._aiChatToggle=toggleChat;

function getPageQuickActions(){
    var page=getCurrentPage();
    var actions=PAGE_SUGGESTIONS[page];
    if(actions)return actions;
    return [
        {text:'📋 待审批',msg:'帮我看看有哪些待审批的材料'},
        {text:'⚠️ 预警',msg:'系统有什么预警吗'},
        {text:'🏫 社团列表',msg:'有哪些社团'},
        {text:'📊 数据报告',msg:'生成数据分析报告'}
    ];
}

function showWelcome(){
    var body=document.getElementById('fcBody');
    if(body.children.length>0)return;
    var quickActions=getPageQuickActions();
    var quickHtml=quickActions.map(function(a){
        return '<button class="fc-quick-btn" onclick="window._aiChatQuick(\''+esc(a.msg).replace(/'/g,"\\'")+'\')">'+esc(a.text)+'</button>';
    }).join('');
    var pageName=getCurrentPage();
    var pageHint='';
    if(pageName&&pageName!=='index.html'&&pageName!=='login.html'){
        var pageNames={'upload.html':'材料上传','workload.html':'工作量审核','dashboard.html':'管理中心','club-tools.html':'社团工具','club-detail.html':'社团详情','stats.html':'数据统计','feedback.html':'问题反馈','checkin.html':'签到管理','club-teacher.html':'指导老师'};
        var pn=pageNames[pageName]||'';
        if(pn)pageHint='<div class="fc-page-hint">📍 当前页面：'+esc(pn)+' — 试试问我与当前页面相关的问题</div>';
    }
    body.innerHTML=`
<div class="fc-welcome">
    <div class="fc-w-icon">🤖</div>
    <div class="fc-w-title">你好，我是小济！</div>
    <div class="fc-w-desc">济幼社团管理系统AI助手<br>可以帮你查询审批、搜索文件、检查预警、生成报告等</div>
    <div class="fc-quick-actions">${quickHtml}</div>
    ${pageHint}
</div>`;
}

function loadHistory(){
    fetch(api+'/api/chat-history',{credentials:'include'}).then(function(r){return r.json()}).then(function(d){
        if(d.success&&d.history&&d.history.length>0){
            var body=document.getElementById('fcBody');
            body.innerHTML='';
            d.history.forEach(function(m){
                appendMessage(m.role,m.content,null,false);
            });
            scrollBottom();
        }else{
            showWelcome();
        }
    }).catch(function(){
        showWelcome();
    });
}

function appendMessage(role,content,toolCalls,animate){
    if(animate===undefined)animate=true;
    var body=document.getElementById('fcBody');
    var welcome=body.querySelector('.fc-welcome');
    if(welcome)welcome.remove();

    var div=document.createElement('div');
    div.className='fc-msg '+role;

    var avatar=document.createElement('div');
    avatar.className='fc-avatar-sm';
    avatar.textContent=role==='user'?'👤':'🤖';

    var bubble=document.createElement('div');
    bubble.className='fc-bubble';

    if(role==='user'){
        bubble.textContent=content;
    }else{
        var html=renderMd(content);
        if(animate&&html.length<2000){
            typewriterEffect(bubble,html,Math.min(html.length*2,600));
        }else{
            bubble.innerHTML=html;
        }
    }

    div.appendChild(avatar);
    div.appendChild(bubble);

    if(toolCalls&&toolCalls.length>0){
        toolCalls.forEach(function(tc){
            var tcDiv=document.createElement('div');
            tcDiv.className='fc-tool-card';
            var icon=TOOL_ICONS[tc.name]||'🔧';
            var name=TOOL_NAMES[tc.name]||tc.name;
            var argsStr='';
            try{
                argsStr=Object.entries(tc.arguments||{}).map(function(e){return e[0]+': '+e[1]}).join(', ');
            }catch(e){}
            var resultPreview='';
            if(tc.result_preview){
                try{
                    var parsed=JSON.parse(tc.result_preview);
                    resultPreview=typeof parsed==='string'?parsed:JSON.stringify(parsed,null,2).substring(0,150);
                }catch(e){
                    resultPreview=tc.result_preview.substring(0,150);
                }
            }
            tcDiv.innerHTML=`<div class="fc-tc-header">${icon} ${esc(name)}</div>${argsStr?'<div class="fc-tc-args">参数: '+esc(argsStr)+'</div>':''}${resultPreview?'<div class="fc-tc-result">'+esc(resultPreview)+'</div>':''}`;
            bubble.appendChild(tcDiv);
        });
    }

    body.appendChild(div);
}

function appendSuggestions(suggestions){
    if(!suggestions||!suggestions.length)return;
    var body=document.getElementById('fcBody');
    var wrapper=document.createElement('div');
    wrapper.className='fc-sug-wrapper';
    var label=document.createElement('div');
    label.className='fc-sug-label';
    label.textContent='💡 接下来你可以问：';
    wrapper.appendChild(label);
    var btns=document.createElement('div');
    btns.className='fc-suggestions';
    suggestions.forEach(function(s){
        var btn=document.createElement('button');
        btn.className='fc-sug-btn';
        btn.textContent=s;
        btn.onclick=function(){window._aiChatQuick(s)};
        btns.appendChild(btn);
    });
    wrapper.appendChild(btns);
    body.appendChild(wrapper);
    scrollBottom();
}

function extractSuggestions(text){
    var m=text.match(/💡 \*\*你可能还想了解：\*\* (.+)/);
    if(m){
        return m[1].split('|').map(function(s){return s.trim()}).filter(Boolean);
    }
    return [];
}

function cleanReplyForDisplay(text){
    return text.replace(/\n💡 \*\*你可能还想了解：\*\* .+$/,'');
}

function showTyping(){
    var body=document.getElementById('fcBody');
    var div=document.createElement('div');
    div.className='fc-msg assistant';
    div.id='fcTyping';
    div.innerHTML='<div class="fc-avatar-sm">🤖</div><div class="fc-bubble"><div class="fc-typing"><span></span><span></span><span></span></div></div>';
    body.appendChild(div);
    scrollBottom();
}

function hideTyping(){
    var el=document.getElementById('fcTyping');
    if(el)el.remove();
}

function scrollBottom(){
    var body=document.getElementById('fcBody');
    body.scrollTop=body.scrollHeight;
}

function sendMessage(msg){
    if(chatSending||!msg.trim())return;
    chatSending=true;
    var input=document.getElementById('fcInput');
    var sendBtn=document.getElementById('fcSendBtn');
    input.disabled=true;
    sendBtn.disabled=true;

    appendMessage('user',msg.trim(),null,false);
    input.value='';
    input.style.height='auto';
    showTyping();
    scrollBottom();

    var pageContext=getCurrentPage();
    fetch(api+'/api/ai-chat',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        credentials:'include',
        body:JSON.stringify({message:msg.trim(),page:pageContext})
    }).then(function(r){return r.json()}).then(function(d){
        hideTyping();
        if(d.success){
            var suggestions=extractSuggestions(d.reply);
            var cleanReply=cleanReplyForDisplay(d.reply);
            appendMessage('assistant',cleanReply,d.tool_calls,true);
            if(suggestions.length>0){
                appendSuggestions(suggestions);
            }
        }else{
            appendMessage('assistant','抱歉，出了点问题：'+(d.error||'未知错误'),null,false);
        }
        scrollBottom();
    }).catch(function(e){
        hideTyping();
        appendMessage('assistant','网络错误，请稍后再试 😥',null,false);
        scrollBottom();
    }).finally(function(){
        chatSending=false;
        input.disabled=false;
        sendBtn.disabled=false;
        input.focus();
    });
}

window._aiChatSend=function(){
    var input=document.getElementById('fcInput');
    sendMessage(input.value);
};

window._aiChatQuick=function(msg){
    var input=document.getElementById('fcInput');
    input.value=msg;
    sendMessage(msg);
};

window._aiChatClear=function(){
    if(!confirm('确定清空所有对话记录？'))return;
    fetch(api+'/api/clear-chat',{method:'POST',credentials:'include'}).then(function(r){return r.json()}).then(function(d){
        if(d.success){
            var body=document.getElementById('fcBody');
            body.innerHTML='';
            showWelcome();
        }
    });
};

function init(){
    injectStyles();
    injectHTML();
}

if(document.readyState==='loading'){
    document.addEventListener('DOMContentLoaded',init);
}else{
    init();
}
})();
