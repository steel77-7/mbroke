package utils

import (
	"log"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	MaxLen            int64  `env:"MAX_LEN" envDefault:"2000000"`
	IdleTime          int    `env:"IDLE_TIME" envDefault:"60000"`
	StreamName        string `env:"STREAM_NAME" envDefault:"ingest:primary"`
	ResQueue          string `env:"RES_QUEUE" envDefault:"res_queue"`
	DeadLetterName    string `env:"DEAD_LETTER_NAME" envDefault:"dead_letter"`
	ConsumerGroupName string `env:"CONSUMER_GROUP_NAME" envDefault:"primary"`
	RetryCount        int64  `env:"RETRY_COUNT" envDefault:"3"`
	TCPServerPort     int    `env:"TCP_SERVER_PORT" envDefault:"9000"`
	Port              int    `env:"PORT" envDefault:"8080"`
	Hmac              string `env:"HMAC_SECRET" envDefault:"secret"`
	Secret            string `env:"SECRET" envDefault:"secret"`
	SetName           string `env:"SET_NAME" envDefault:"workerset"`
	BatchSize         int64  `env:"BATCH_SIZE" envDefault:"100"`
}

type RedisConfig struct {
	Addr     string `env:"REDIS_ADDR" envDefault:"redis:6379"`
	PoolSize int    `env:"REDIS_POOL_SIZE" envDefault:"10"`
	Password string `env:"REDIS_PASSWORD"`
	DB       int    `env:"REDIS_DB" envDefault:"0"`
	Protocol int    `env:"REDIS_PROTOCOL" envDefault:"2"`
}

func LoadConfig() (*Config, *RedisConfig) {
	var cfg Config
	var redisCfg RedisConfig

	if err := env.Parse(&cfg); err != nil {
		log.Fatal(err)
	}

	if err := env.Parse(&redisCfg); err != nil {
		log.Fatal(err)
	}

	return &cfg, &redisCfg
}

var Conf *Config
var RedConf *RedisConfig
