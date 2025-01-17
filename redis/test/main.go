package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	totalKeys      = 1000000 // 100 万
	keySizeBytes   = 16      // 16B
	valueSizeBytes = 1024    // 1KB
	concurrency    = 100     // goroutine 数量
)

// randomBytes 生成指定大小的随机字节
func randomBytes(size int) []byte {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return buf
}

// generateKey 生成一个 16B 的 key（可见化用 base64 编码）
func generateKey() string {
	kb := randomBytes(keySizeBytes)
	return base64.StdEncoding.EncodeToString(kb)
}

// generateValue 生成 1KB 随机值
func generateValue() []byte {
	return randomBytes(valueSizeBytes)
}

func main() {
	ctx := context.Background()

	// 连接 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6380",
		Password: "",
		DB:       0,
	})

	// 1. 生成数据，不计入耗时
	fmt.Println("Generating keys and values...")
	keys := make([]string, 0, totalKeys)
	values := make([][]byte, 0, totalKeys)

	for i := 0; i < totalKeys; i++ {
		keys = append(keys, generateKey())
		values = append(values, generateValue())
	}
	fmt.Println("Data generation complete.")

	// 确保数据已准备好
	fmt.Println("Data is ready...")

	fmt.Println("start SET test...")
	// 2. 并发 SET 操作
	startSet := time.Now()
	var wg sync.WaitGroup
	batchSize := totalKeys / concurrency

	for i := 0; i < concurrency; i++ {
		startIdx := i * batchSize
		// 对于最后一个 goroutine，确保处理剩余任务
		endIdx := startIdx + batchSize
		if i == concurrency-1 {
			endIdx = totalKeys
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				if err := rdb.Set(ctx, keys[j], values[j], 0).Err(); err != nil {
					log.Fatalf("SET error at key %d: %v", j, err)
				}
			}
		}(startIdx, endIdx)
	}
	wg.Wait()
	elapsedSet := time.Since(startSet).Seconds()
	fmt.Printf("SET %d keys concurrently using %d goroutines, cost: %.2f seconds\n", totalKeys, concurrency, elapsedSet)

	// ，准备 GET 测试
	fmt.Println("start GET test...")

	// 3. 并发 GET 操作
	startGet := time.Now()
	wg = sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		startIdx := i * batchSize
		endIdx := startIdx + batchSize
		if i == concurrency-1 {
			endIdx = totalKeys
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				_, err := rdb.Get(ctx, keys[j]).Bytes()
				// 注意：如果键不存在，err 返回 redis.Nil，这里忽略这种错误情况
				if err != nil && err != redis.Nil {
					log.Fatalf("GET error at key %d: %v", j, err)
				}
			}
		}(startIdx, endIdx)
	}
	wg.Wait()
	elapsedGet := time.Since(startGet).Seconds()
	fmt.Printf("GET %d keys concurrently using %d goroutines, cost: %.2f seconds\n", totalKeys, concurrency, elapsedGet)
}
