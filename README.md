Data-Engineering-Stocks-and-Crypto-real-time-data/
│
├── README.md                  
├── .gitignore
├── requirements.txt
│
├── images/                    
│   ├── crypto_dashboard.png
│   ├── kafka.png
│   ├── market_data_pipeline.png
│   ├── stocks_dashboard.png
│   └── minio.png
│
├── producer/
│   └── producer.py
│
├── consumer/
│   ├── stocks-consumer.py
│   └── crypto-consumer.py
│
└── infra/
    ├── dags/
    │   └── minio_to_sql.py
    │
    ├── sql/
    │   ├── silver/
    │   └── gold/
    │
    ├── docker-compose.yml
    └── requirements.txt