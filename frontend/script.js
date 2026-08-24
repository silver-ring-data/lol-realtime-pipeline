/*
 * 실시간 하이라이트 대시보드.
 *
 * 서빙 API를 주기적으로 폴링하여 하이라이트 점수, 시청자 수, 채팅을 화면에
 * 반영한다. 관중석 아바타 밀도로 시청자 수를 표현하고, 점수 구간에 따라
 * 전광판 문구를 전환한다.
 */

const container = document.getElementById('audience-container');
const allSeats = [];
let shuffledSeats = [];
const MAX_REAL_VIEWERS = 110000;
const seenChats = new Set();
// 데이터 수신 후 영상을 한 번만 재생시키기 위한 플래그
let isVideoStarted = false;
let player;  // YouTube IFrame Player 인스턴스
let lastChatTs = "2000-01-01T00:00:00Z";  // 중복 수신 방지용 최신 채팅 시각

const avatarImages = [
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Faker',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Keria',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Gumayusi',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Oner',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Zeus',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Teemo'
];

// 서빙 API 엔드포인트. 배포 환경마다 다르므로 실제 값은 로컬에서 설정한다.
// (config/config.yaml의 serving_endpoint와 동일한 값을 사용한다)
const CONFIG = {
    API_URL: window.LOL_API_URL || "https://<api-id>.execute-api.ap-northeast-2.amazonaws.com/lol"
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
        videoId: 'NsWPXB5Wqzs',  // 2024 Worlds 결승 다시보기
        playerVars: {
            'start': 13830,  // 4세트 시작 지점(3시간 50분 30초)부터 재생
            'autoplay': 0,   // 데이터 수신 시점에 맞춰 재생하기 위해 자동재생 해제
            'controls': 1,
            'mute': 1        // 브라우저 자동재생 정책 대응
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

// --- 데이터 페칭 ---
async function fetchRealtimeData(apiUrl) {
    try {
        const response = await fetch(apiUrl);
        if (!response.ok) return;
        
        const data = await response.json();
        // 첫 데이터 수신 시점에 영상 재생을 시작해 화면과 데이터의 시작점을 맞춘다.
        if (data && !isVideoStarted) {
            if (player && typeof player.playVideo === 'function') {
                console.log("[dashboard] 데이터 수신 확인, 영상 재생 시작");
                player.playVideo();
                isVideoStarted = true;
            }
        }
        
        console.debug("[dashboard]", new Date().toLocaleTimeString(),
            "score:", data.latest_score,
            "viewers:", data.latest_viewer ? data.latest_viewer.viewer_count : 0,
            "chats:", data.latest_chats?.length || 0);

        // 1. 하이라이트 점수 및 전광판 갱신
        if (data.latest_score) {
            const score = data.latest_score;
            
            // 전광판 HUD 갱신
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
            
        // 2. 시청자 수 반영
        if (data.latest_viewer && data.latest_viewer.viewer_count !== undefined) {
            const count = data.latest_viewer.viewer_count;
            
            // UI 숫자 업데이트 (id="viewer-count-display" 엘리먼트 텍스트 변경)
            const displayEl = document.getElementById('viewer-count-display');
            if (displayEl) {
                displayEl.innerText = count.toLocaleString();
            }
            
            // 관중석 아바타 밀도로 시청자 규모를 표현
            if (typeof updateViewerCount === 'function') {
                updateViewerCount(count); 
            }
        }

        // 3. 채팅 처리 (중복 제거 후 표시)
        if (data.latest_chats && data.latest_chats.length > 0) {
            // 발생 순서대로 표시되도록 시간순 정렬
            const sortedChats = [...data.latest_chats].sort((a, b) => new Date(a.ts) - new Date(b.ts));
            
            sortedChats.forEach((chat) => {
                // 폴링 주기가 겹쳐도 같은 채팅이 중복 표시되지 않도록 키를 생성한다.
                const chatKey = `${chat.nickname}_${chat.content}_${chat.ts}`;
                
                if (!seenChats.has(chatKey)) {
                    seenChats.add(chatKey);

                    // 메모리 증가를 막기 위해 일정 크기를 넘으면 초기화한다.
                    if (seenChats.size > 1000) seenChats.clear();

                    // 동시에 도착한 채팅이 한꺼번에 뜨지 않도록 표시 시점을 분산한다.
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
        console.error("[dashboard] 데이터 조회에 실패했습니다:", e);
    }
}
// --- 게이지 업데이트 ---
function updateGauges(eventScore, chatScore) {
    const eventGauge = document.getElementById('event-gauge');
    const chatGauge = document.getElementById('chat-gauge');

    if (eventGauge) {
        // 점수 상한을 100으로 두고 게이지 높이를 백분율로 환산한다.
        const eventPercent = Math.min(100, Math.max(0, eventScore)); 
        eventGauge.style.height = `${eventPercent}%`;
    }

    if (chatGauge) {
        // 점수 상한을 100으로 두고 게이지 높이를 백분율로 환산한다.
        const chatPercent = Math.min(100, Math.max(0, chatScore));
        chatGauge.style.height = `${chatPercent}%`;
    }
}

// --- HUD 업데이트 (현재 경기 상태 및 전광판 로직) ---
function updateArenaHUD(eventScore, chatScore, finalScore, matchId) {
    const ledEl = document.getElementById('led-text');
    const statusEl = document.getElementById('match-status'); 

    if (!ledEl) return;

    // match_id의 구간 정보를 화면 표시용 문구로 변환한다.
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
        matchLabel = matchId;  // 정의되지 않은 구간은 ID를 그대로 노출
    }

    // 상태 표시 엘리먼트가 없으면 콘솔로만 출력한다.
    if (statusEl) {
        statusEl.innerText = matchLabel;
    } else {
        console.debug("[dashboard] 현재 상태:", matchLabel);
    }

    // 점수 구간에 따라 전광판 문구와 색상을 전환한다.
    if (finalScore > 40) {
        ledEl.innerText = `🔥 ${matchLabel} - 하이라이트 🔥`;
        ledEl.style.color = "#ff0000";
    } else if (eventScore > 40) {
        ledEl.innerText = "⚔️ 이벤트 발생 ⚔️";
        ledEl.style.color = "#0f0";
    } else if (chatScore > 40) {
        ledEl.innerText = "💬 지금 채팅이 엄청나게 올라오고 있습니다! 💬";
        ledEl.style.color = "#0f0";
    } else {
        ledEl.innerText = `⏱️ ${matchLabel} ... ANALYZING FLOW 🚀`;
        ledEl.style.color = "#0f0";
    }
}

// --- 앱 시작 ---
async function initApp() {
    console.log("[dashboard] 초기화 시작");
    buildStadium(8);
    updateViewerCount(35000);  // 초기 관중 밀도

    // 서빙 API를 0.5초 주기로 폴링한다.
    setInterval(() => fetchRealtimeData(CONFIG.API_URL), 500);
}

initApp();