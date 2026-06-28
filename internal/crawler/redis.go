package crawler

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr string) *RedisClient {
	return &RedisClient{
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

// TryClaimHost returns true if this caller is the first to claim the host (SETNX).
// The claim expires after ttl, allowing the host to be re-crawled after that period.
func (r *RedisClient) TryClaimHost(ctx context.Context, host string, ttl time.Duration) bool {
	ok, _ := r.client.SetNX(ctx, "host:"+host, 1, ttl).Result()
	return ok
}
