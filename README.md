lol-realtime-pipeline/
├── data_source/
│   ├── api_server/
│   └── producer/
│       ├── chat/
│       │   ├── chat_bot.py
│       │   ├── Dockerfile
│       │   └── data/                 
│       │       └── t1_blg_chat.json
│       └── game/
│           ├── game_bot.py
│           ├── Dockerfile
│           └── data/                
│               ├── t1_blg_g4_exact.json
│               └── t1_blg_g5_exact.json
├── scripts/         
│   ├── youtube_crawler.py
│   ├── gol_gg_scraper.py
│   └── ...
└── docker-compose.yml
