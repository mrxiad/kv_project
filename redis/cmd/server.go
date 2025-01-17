package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

// redis-server
type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure //数据库
	server *redcon.Server                            //服务
	mu     sync.RWMutex                              //读写锁
}

func logMemoryUsage() {
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
		time.Sleep(10 * time.Second) // 每10秒记录一次
	}
}
func main() {
	go logMemoryUsage()

	// 打开 Redis 数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure //默认使用第一个数据库

	// 初始化一个 Redis 服务端(IP:端口,handler,accept,close)
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

// server开启监听
func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections")
	_ = svr.server.ListenAndServe() //启动server的监听
}

// 接收一个客户端连接
func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	//log.Println("客户端连接:", conn.RemoteAddr())
	// 创建一个客户端
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

// 客户端连接关闭
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	log.Println("客户端退出")
}
