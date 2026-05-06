const container = document.getElementById('audience-container');
const allSeats = [];        // 물리적인 전체 좌석 (DOM)
let shuffledSeats = [];     // 🌟 랜덤한 입장 순서가 기록된 좌석 리스트

const MAX_REAL_VIEWERS = 100000; 

const chatMsgs = ["T1 가자!", "대상혁!", "제우스!", "나이스!!", "ㅋㅋㅋㅋ", "와 미쳤다", "GOAT", "오늘 폼 미쳤다"];
const avatarImages = [
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Faker',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Keria',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Gumayusi',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Oner',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Zeus',
    'https://api.dicebear.com/7.x/pixel-art/svg?seed=Teemo'
];

// 1. 유틸리티: 닉네임 해시 및 배열 섞기
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

// 2. 경기장 건설 (최초 1회 실행)
function buildStadium(totalRows) {
    let baseCount = 34; 
    container.innerHTML = ''; // 초기화

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
    
    // 🌟 핵심: 모든 좌석의 '입장 순서'를 딱 한 번만 섞어둠!
    shuffledSeats = shuffle([...allSeats]); 
}

// 3. 시청자 밀도 업데이트 (랜덤하지만 일관성 있게)
function updateViewerCount(realViewerCount) {
    // 384석 중 몇 개를 채울지 계산
    const targetOccupiedCount = Math.floor((realViewerCount / MAX_REAL_VIEWERS) * allSeats.length);

    // 🌟 미리 섞인 순서(shuffledSeats)대로 채우기 때문에 
    // 숫자가 늘어나면 빈곳에 추가되고, 줄어들면 마지막에 들어온 사람부터 나감!
    shuffledSeats.forEach((seat, index) => {
        if (index < targetOccupiedCount) {
            if (!seat.classList.contains('occupied')) {
                seat.classList.add('occupied');
                
                // 좌석의 절대 위치(allSeats index)를 써서 아바타 외형 고정
                const absoluteIdx = allSeats.indexOf(seat);
                const imgIdx = absoluteIdx % avatarImages.length;
                seat.style.backgroundImage = `url('${avatarImages[imgIdx]}')`;
                seat.style.backgroundColor = `hsl(${(absoluteIdx * 40) % 360}, 50%, 45%)`;
            }
        } else {
            // 퇴장 처리
            seat.classList.remove('occupied');
            seat.style.backgroundImage = 'none';
            seat.style.backgroundColor = '#222';
        }
    });
}

// 4. 채팅 발생 (고정석 + 폴백 대리인 로직)
function onMessageReceived(nickname, message, priority) {
    // 🌟 방법: 전체 좌석 중 내 닉네임의 "진짜 지정석"을 먼저 찾음
    const myFixedSeatIndex = hashString(nickname) % allSeats.length;
    const targetSeat = allSeats[myFixedSeatIndex];

    // 만약 내 지정석이 지금 비어있는(회색) 자리라면?
    if (!targetSeat.classList.contains('occupied')) {
        // 🌟 Fallback: 현재 앉아있는 아바타 중 한 명을 골라 대신 말하게 함 (유실 방지)
        const occupiedSeats = allSeats.filter(s => s.classList.contains('occupied'));
        if (occupiedSeats.length === 0) return;
        
        const proxyIdx = hashString(nickname) % occupiedSeats.length;
        displayChat(occupiedSeats[proxyIdx], message, priority);
    } else {
        // 내 자리가 활성화되어 있다면 내 자리에서 말하기!
        displayChat(targetSeat, message, priority);
    }
}

// 말풍선 시각화 함수
function displayChat(seat, message, priority) {
    if (seat.querySelector('.chat-bubble')) return; // 도배 방지

    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble';
    bubble.innerText = message;
    if (priority === 1) bubble.classList.add('high-priority');

    seat.classList.add('jumping');
    seat.appendChild(bubble);

    setTimeout(() => {
        if (seat.contains(bubble)) bubble.remove();
        seat.classList.remove('jumping');
    }, 1500);
}

// 오픈서치 연동부분

function updateArenaHUD(eventScore, chatScore, finalScore) {
    // 1. LED 문구 업데이트
    const ledEl = document.getElementById('led-text');
    if (finalScore > 80) {
        ledEl.innerText = "🔥 HOLY MOLY! SUPER PLAY DETECTED!! 🔥";
        ledEl.style.color = "#ff0000"; // 하이라이트 땐 빨간색!
    } else if (eventScore > 50) {
        ledEl.innerText = "⚔️ MASSIVE OBJECTIVE FIGHT IN PROGRESS! ⚔️";
        ledEl.style.color = "#0f0";
    } else {
        ledEl.innerText = "⏱️ ANALYZING GAME FLOW... T1 vs GEN.G ... HYPED UP! 🚀";
        ledEl.style.color = "#0f0";
    }

    // 2. 왼쪽 게이지 (Event Intensity)
    const leftGauge = document.getElementById('event-gauge');
    if (leftGauge) leftGauge.style.height = `${eventScore}%`;
    
    // 3. 오른쪽 게이지 (Chat Intensity)
    const rightGauge = document.getElementById('chat-gauge');
    if (rightGauge) rightGauge.style.height = `${chatScore}%`;
}

// 5. 시뮬레이션 시작
buildStadium(8);
updateViewerCount(15000); // 1만 5천명 모인 상태로 시작 (랜덤하게 흩어져 앉음)

setInterval(() => {
    const users = ["대상혁팬", "은비친구", "T1우승", "데이터왕", "구마유시", "케리아"];
    const user = users[Math.floor(Math.random() * users.length)];
    const msg = chatMsgs[Math.floor(Math.random() * chatMsgs.length)];
    const isHigh = Math.random() > 0.9 ? 1 : 0;
    onMessageReceived(user, msg, isHigh);
}, 400);

// 5초 뒤에 관중이 8만 명으로 꽉 차는 연출!
setTimeout(() => updateViewerCount(80000), 5000);