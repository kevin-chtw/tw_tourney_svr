package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kevin-chtw/tw_proto/sproto"
	"github.com/redis/go-redis/v9"
)

type RedisStorage struct {
	client     *redis.Client
	expireTime time.Duration
}

func NewRedisStorage(client *redis.Client) *RedisStorage {
	return &RedisStorage{
		client:     client,
		expireTime: time.Minute,
	}
}

func (s *RedisStorage) key(tourney *sproto.TourneyInfo) string {
	return tourney.GameType + ":" + string(tourney.Id)
}

func (s *RedisStorage) UpdateTourney(ctx context.Context, tourney *sproto.TourneyInfo) error {
	if tourney == nil {
		return errors.New("tourney info is nil")
	}

	data, err := json.Marshal(tourney)
	if err != nil {
		return fmt.Errorf("marshal tourney failed: %w", err)
	}

	// Redis的Set命令在key存在时会自动覆盖
	err = s.client.Set(ctx, s.key(tourney), data, s.expireTime).Err()
	if err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}
	return nil
}

func (s *RedisStorage) ListTourneys(ctx context.Context, filter func(*sproto.TourneyInfo) bool) ([]*sproto.TourneyInfo, error) {
	// 构建更精确的key模式
	pattern := "*"
	if filter != nil {
		// 获取一个示例tourney来构建更精确的key模式
		sample := &sproto.TourneyInfo{}
		if filter(sample) {
			pattern = sample.GameType + ":*"
		}
	}

	keys, err := s.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, err
	}

	var tourneys []*sproto.TourneyInfo
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		var tourney sproto.TourneyInfo
		if err := json.Unmarshal(data, &tourney); err != nil {
			continue
		}

		if filter == nil || filter(&tourney) {
			tourneys = append(tourneys, &tourney)
		}
	}

	return tourneys, nil
}
