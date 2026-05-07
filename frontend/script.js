const container = document.getElementById('audience-container');
const allSeats = [];
let shuffledSeats = [];
const MAX_REAL_VIEWERS = 110000;
const seenChats = new Set();
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

// --- 데이터 페칭 (로그 강화 버전) ---
async function fetchRealtimeData(apiUrl) {
    try {
        const response = await fetch(apiUrl);
        if (!response.ok) return;
        
        const data = await response.json();
        
        // 🔍 데이터 확인용 로그 (시청자 수 추가!)
        console.log("📥 [데이터 수신]", new Date().toLocaleTimeString());
        console.log("📊 점수:", data.latest_score);
        console.log("👥 시청자:", data.latest_viewer ? data.latest_viewer.viewer_count : 0);
        console.log("💬 채팅:", data.latest_chats?.length || 0);

        // 1. 하이라이트 점수 & 전광판 업데이트
        if (data.latest_score) {
            updateArenaHUD(
                data.latest_score.event_score, 
                data.latest_score.chat_score, 
                data.latest_score.final_score
            );
            // 게이지바도 있다면 같이 업데이트!
            if (typeof updateGauges === 'function') {
                updateGauges(data.latest_score.event_score, data.latest_score.chat_score);
            }
        }

        // 🌟 2. 시청자 수 실시간 반영 (추가된 부분!)
        if (data.latest_viewer && data.latest_viewer.viewer_count !== undefined) {
            const count = data.latest_viewer.viewer_count;
            
            // UI 숫자 업데이트 (id="viewer-count-display" 엘리먼트가 있어야 해!)
            const displayEl = document.getElementById('viewer-count-display');
            if (displayEl) {
                displayEl.innerText = count.toLocaleString(); // 10,000 처럼 콤마 찍기
            }
            
            // 🏟️ 경기장 내 아바타 가시성 조절 함수 호출!
            // (우리가 이전에 만든 setViewerCount 혹은 updateViewerCount 이름에 맞춰줘)
            if (typeof updateViewerCount === 'function') {
                updateViewerCount(count); 
            }
        }

        // 🌟 2. fetchRealtimeData 함수 안의 3번 채팅 처리 로직을 이걸로 싹 교체해!
        // 3. 실시간 채팅 고속 처리 (바구니 중복 제거 + 랜덤 폭발!)
        if (data.latest_chats && data.latest_chats.length > 0) {
            const sortedChats = [...data.latest_chats].sort((a, b) => new Date(a.ts) - new Date(b.ts));
            
            sortedChats.forEach((chat) => {
                // 닉네임 + 내용 + 시간으로 고유한 지문(Key) 만들기
                const chatKey = `${chat.nickname}_${chat.content}_${chat.ts}`;
                
                // 이 지문이 우리 바구니에 없는 '새로운' 채팅일 때만 통과!
                if (!seenChats.has(chatKey)) {
                    seenChats.add(chatKey); // 바구니에 저장!
                    
                    // 바구니가 너무 무거워지면 한번 싹 비워주기 (메모리 보호)
                    if (seenChats.size > 1000) seenChats.clear();
                    
                    // 0 ~ 400ms 사이에서 무작위로 터지게 (폭죽 효과 팡팡팡!)
                    const randomDelay = Math.random() * 400;
                    setTimeout(() => {
                        onMessageReceived(chat.nickname, chat.content, chat.priority);
                    }, randomDelay); 
                }
            });
        }
    } catch (e) { 
        console.error("🚨 데이터 가져오다 삐끗했어!:", e); 
    }
}

// --- HUD 업데이트 (은비 커스텀 로직 반영!) ---
function updateArenaHUD(eventScore, chatScore, finalScore) {
    const ledEl = document.getElementById('led-text');
    if (!ledEl) return;

    // 🌟 은비가 요청한 조건부 전광판 로직!
    if (finalScore > 40) {
        ledEl.innerText = "🔥 하이라이트으으으 🔥";
        ledEl.style.color = "#ff0000"; // 빨간색
    } else if (eventScore > 40) {
        ledEl.innerText = "⚔️ 엄청난 피지컬 컨트로로로로롤 ⚔️";
        ledEl.style.color = "#0f0"; // 초록색
    } else if (chatScore > 40) {
        ledEl.innerText = "💬 채팅 핫함 💬";
        ledEl.style.color = "#0f0"; // 초록색
    } else {
        ledEl.innerText = "⏱️ ANALYZING GAME FLOW... T1 vs BLG ... HYPED UP! 🚀";
        ledEl.style.color = "#0f0";
    }
    
    // 게이지 업데이트
    const eg = document.getElementById('event-gauge');
    const cg = document.getElementById('chat-gauge');
    if (eg) eg.style.height = `${eventScore}%`;
    if (cg) cg.style.height = `${chatScore}%`;
}

// --- 앱 시작 ---
async function initApp() {
    console.log("🎬 앱 초기화 시작 (5만 명 입장 완료!)");
    buildStadium(8); 
    updateViewerCount(50000); 
    
    // 0.5초마다 API 호출 (은비야, F12 콘솔창 열어두면 로그 쏟아질 거야!)
    setInterval(() => fetchRealtimeData(CONFIG.API_URL), 500);
}

initApp();