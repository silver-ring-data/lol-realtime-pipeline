const container = document.getElementById('audience-container');
const allSeats = [];
let shuffledSeats = [];
const MAX_REAL_VIEWERS = 110000;
const seenChats = new Set();
// 🌟 [추가] 영상이 이미 시작됐는지 체크하는 변수
let isVideoStarted = false; 
let player; // 유튜브 플레이어 객체 보관용
// 🌟 [핵심] 중복 방지용 시간 도장
let lastChatTs = "2000-01-01T00:00:00Z";

const avatarImages = [
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Faker',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Keria',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Gumayusi',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Oner',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Zeus',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Teemo'
];

const CONFIG = {
    API_URL: "https://u4xlftet19.execute-api.ap-northeast-2.amazonaws.com/lol" 
};

// --- 유틸리티 함수 ---
function hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    return Math.abs(hash);
}

function shuffle(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

// --- 경기장 건설 ---
function buildStadium(totalRows) {
    let baseCount = 38; 
    container.innerHTML = '';
    allSeats.length = 0; // 초기화
    for (let r = 0; r < totalRows; r++) {
        const rowDiv = document.createElement('div');
        rowDiv.className = 'audience-row';
        const seatsInThisRow = baseCount + (r * 4); 
        for (let s = 0; s < seatsInThisRow; s++) {
            const seat = document.createElement('div');
            seat.className = 'seat'; 
            rowDiv.appendChild(seat);
            allSeats.push(seat);
        }
        container.appendChild(rowDiv);
    }
    shuffledSeats = shuffle([...allSeats]); 
}
function onYouTubeIframeAPIReady() {
    player = new YT.Player('yt-player', {
        height: '100%',
        width: '100%',
        videoId: 'NsWPXB5Wqzs', // T1 vs BLG 결승전 영상 ID
        playerVars: {
            'start': 13830, // 🌟 딱 여기서부터 시작! (3시간 50분 30초)
            'autoplay': 0,  // 0으로 둬서 일단 멈춰놓고 대기! (핵심)
            'controls': 1,
            'mute': 1       // 크롬 브라우저 자동재생 정책을 피하기 위해 음소거
        }
    });
}

function updateViewerCount(realViewerCount) {
    const targetOccupiedCount = Math.floor((realViewerCount / MAX_REAL_VIEWERS) * allSeats.length);
    shuffledSeats.forEach((seat, index) => {
        if (index < targetOccupiedCount) {
            if (!seat.classList.contains('occupied')) {
                seat.classList.add('occupied');
                const absoluteIdx = allSeats.indexOf(seat);
                const imgIdx = absoluteIdx % avatarImages.length;
                seat.style.backgroundImage = `url('${avatarImages[imgIdx]}')`;
                seat.style.backgroundColor = `hsl(${(absoluteIdx * 40) % 360}, 50%, 45%)`;
            }
        } else {
            seat.classList.remove('occupied');
            seat.style.backgroundImage = 'none';
            seat.style.backgroundColor = '#222';
        }
    });
}

// --- 채팅 발생 로직 ---
function onMessageReceived(nickname, message, priority) {
    const seatCount = allSeats.length;
    let targetSeat = allSeats[hashString(nickname) % seatCount];

    if (!targetSeat.classList.contains('occupied') || targetSeat.querySelector('.chat-bubble')) {
        const silentSeats = allSeats.filter(s => s.classList.contains('occupied') && !s.querySelector('.chat-bubble'));
        if (silentSeats.length === 0) return;
        targetSeat = silentSeats[hashString(nickname) % silentSeats.length];
    }

    displayChat(targetSeat, nickname, message, priority);
}

function displayChat(seat, nickname, message, priority) {
    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble';
    bubble.innerHTML = `
        <span class="chat-nickname">${nickname}</span>
        <span class="chat-content">${message}</span>
    `;

    if (priority === 1) bubble.classList.add('high-priority');
    seat.classList.add('jumping');
    seat.appendChild(bubble);

    setTimeout(() => {
        if (seat.contains(bubble)) bubble.remove();
        seat.classList.remove('jumping');
    }, 3000);
}

// 🌟 [핵심] 채팅 중복 방지용 바구니 (함수 밖에 딱 한 번만 선언해야 기억이 유지돼!)

// --- 데이터 페칭 (로그 강화 & 채팅 폭죽 버전) ---
async function fetchRealtimeData(apiUrl) {
    try {
        const response = await fetch(apiUrl);
        if (!response.ok) return;
        
        const data = await response.json();
        // 🚀 [여기 주목!] 데이터가 성공적으로 왔고, 아직 영상이 안 틀어졌다면?
        if (data && !isVideoStarted) {
            if (player && typeof player.playVideo === 'function') {
                console.log("🎬 데이터 수신 확인! 유튜브 재생 시작!");
                player.playVideo(); // 드디어 영상 출발!
                isVideoStarted = true; // 이제 두 번 다시 재생 명령 안 내림
            }
        }
        
        // 🔍 데이터 확인용 로그 (시청자 수 추가!)
        console.log("📥 [데이터 수신]", new Date().toLocaleTimeString());
        console.log("📊 점수:", data.latest_score);
        console.log("👥 시청자:", data.latest_viewer ? data.latest_viewer.viewer_count : 0);
        console.log("💬 채팅:", data.latest_chats?.length || 0);

        // 🌟 1. 하이라이트 점수 & 전광판 업데이트
        if (data.latest_score) {
            const score = data.latest_score;
            
            // 전광판 HUD 업데이트 (match_id 전달 완료!)
            if (typeof updateArenaHUD === 'function') {
                updateArenaHUD(
                    score.event_score || 0,
                    score.chat_score || 0,
                    score.final_score || 0,
                    score.match_id  
                );
            }
            
            // 게이지바 업데이트
            if (typeof updateGauges === 'function') {
                updateGauges(score.event_score || 0, score.chat_score || 0);
            }
        }
            
        // 🌟 2. 시청자 수 실시간 반영
        if (data.latest_viewer && data.latest_viewer.viewer_count !== undefined) {
            const count = data.latest_viewer.viewer_count;
            
            // UI 숫자 업데이트 (id="viewer-count-display" 엘리먼트 텍스트 변경)
            const displayEl = document.getElementById('viewer-count-display');
            if (displayEl) {
                displayEl.innerText = count.toLocaleString(); // 10,000 처럼 예쁘게 콤마 찍기
            }
            
            // 🏟️ 경기장 내 아바타 가시성(밀도) 조절 함수 호출!
            if (typeof updateViewerCount === 'function') {
                updateViewerCount(count); 
            }
        }

        // 🌟 3. 실시간 채팅 고속 처리 (바구니 중복 제거 + 랜덤 폭발!)
        if (data.latest_chats && data.latest_chats.length > 0) {
            // 시간순으로 정렬해서 자연스럽게 순서대로 올라오도록
            const sortedChats = [...data.latest_chats].sort((a, b) => new Date(a.ts) - new Date(b.ts));
            
            sortedChats.forEach((chat) => {
                // 닉네임 + 내용 + 시간으로 고유한 지문(Key) 만들기
                const chatKey = `${chat.nickname}_${chat.content}_${chat.ts}`;
                
                // 이 지문이 우리 바구니에 없는 '새로운' 채팅일 때만 통과!
                if (!seenChats.has(chatKey)) {
                    seenChats.add(chatKey); // 바구니에 도장 쾅!
                    
                    // 바구니가 너무 무거워지면 한번 싹 비워주기 (메모리 보호)
                    if (seenChats.size > 1000) seenChats.clear();
                    
                    // 0 ~ 400ms 사이에서 무작위로 터지게 (폭죽 효과 팡팡팡!)
                    const randomDelay = Math.random() * 400;
                    setTimeout(() => {
                        if (typeof onMessageReceived === 'function') {
                            onMessageReceived(chat.nickname, chat.content, chat.priority);
                        }
                    }, randomDelay); 
                }
            });
        }
    } catch (e) { 
        console.error("🚨 데이터 가져오다 삐끗했어!:", e); 
    }
}
// --- 게이지 업데이트 함수 (새로 추가!) ---
function updateGauges(eventScore, chatScore) {
    const eventGauge = document.getElementById('event-gauge');
    const chatGauge = document.getElementById('chat-gauge');

    if (eventGauge) {
        // 이벤트 점수 최대치를 100으로 잡고 퍼센트로 변환 (필요에 따라 최대치 조절 가능)
        const eventPercent = Math.min(100, Math.max(0, eventScore)); 
        eventGauge.style.height = `${eventPercent}%`;
    }

    if (chatGauge) {
         // 채팅 점수 최대치를 100으로 잡고 퍼센트로 변환
        const chatPercent = Math.min(100, Math.max(0, chatScore));
        chatGauge.style.height = `${chatPercent}%`;
    }
}

// --- HUD 업데이트 (현재 경기 상태 및 전광판 로직) ---
function updateArenaHUD(eventScore, chatScore, finalScore, matchId) {
    const ledEl = document.getElementById('led-text');
    const statusEl = document.getElementById('match-status'); 

    if (!ledEl) return;

    // 🔍 match_id를 보고 한국어 상태로 변환
    let matchLabel = "";
    if (!matchId || matchId === 'live_match') {
        matchLabel = "📡 실시간 데이터 수신 중...";
    } else if (matchId.includes('pre_game')) {
        matchLabel = "🎮 경기 시작 전";
    } else if (matchId.includes('_g4')) {
        matchLabel = "⚔️ 2024 Worlds 결승전 [4세트]";
    } else if (matchId.includes('intermission')) {
        matchLabel = "☕ 쉬는 시간 (Intermission)";
    } else if (matchId.includes('_g5')) {
        matchLabel = "🏆 2024 Worlds 결승전 [5세트]";
    } else if (matchId.includes('post_game')) {
        matchLabel = "🎉 경기 종료";
    } else {
        matchLabel = matchId; // 매칭되는 게 없으면 ID 그대로 출력
    }

    // 상태창 엘리먼트가 있다면 화면에 반영 (없으면 콘솔에만 출력)
    if (statusEl) {
        statusEl.innerText = matchLabel;
    } else {
        console.log("현재 상태:", matchLabel); 
    }

    // --- 기존 하이라이트 LED 로직 ---
    if (finalScore > 40) {
        ledEl.innerText = `🔥 ${matchLabel} - 하이라이트 🔥`;
        ledEl.style.color = "#ff0000"; // 빨간색
    } else if (eventScore > 40) {
        ledEl.innerText = "⚔️ 이벤트 발생 ⚔️";
        ledEl.style.color = "#0f0"; // 초록색
    } else if (chatScore > 40) {
        ledEl.innerText = "💬 지금 채팅이 엄청나게 올라오고 있습니다! 💬";
        ledEl.style.color = "#0f0"; // 초록색
    } else {
        ledEl.innerText = `⏱️ ${matchLabel} ... ANALYZING FLOW 🚀`;
        ledEl.style.color = "#0f0"; // 초록색
    }
}

// --- 앱 시작 ---
async function initApp() {
    console.log("🎬 앱 초기화 시작 (3만 5천명 입장 완료!)");
    buildStadium(8); 
    updateViewerCount(35000); 
    
    // 0.5초마다 API 호출 (은비야, F12 콘솔창 열어두면 로그 쏟아질 거야!)
    setInterval(() => fetchRealtimeData(CONFIG.API_URL), 500);
}

initApp();