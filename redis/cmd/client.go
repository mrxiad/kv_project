package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
	"log"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("err Wrong number of arguments for %s command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"quit":  nil,
	"ping":  nil,
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

// redis-service
type BitcaskClient struct {
	server *BitcaskServer                    //存储服务端的指针
	db     *bitcask_redis.RedisDataStructure //存储数据结构的指针
}

// 执行客户端命令
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0])) //将Set转换为set
	cmdFunc, ok := supportedCommands[command]       //判断是否支持该命令
	if !ok {
		conn.WriteError("Err unsupported command: '" + command + "'") //不支持的命令
		return
	}
	log.Println("exec command:", string(cmd.Raw)) //打印命令(RESP协议格式)
	for _, arg := range cmd.Args {
		log.Println("arg:", string(arg)) //打印参数
	}
	//获取客户端
	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit": //实际上用不到这条指令，因为redcon库会自动处理
		_ = conn.Close() //关闭连接
	case "ping":
		conn.WriteString("PONG") //返回PONG
	default:
		//TODO
		res, err := cmdFunc(client, cmd.Args[1:]) //cmd.Args[1:]是除了命令之外的参数
		if err != nil {
			if errors.Is(err, bitcask.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res) //返回结果
	}
}

// 执行set指令
func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	// set a 100
	key, value := args[0], args[1] //获取set的key和value
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

// 执行get指令
func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}
	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

// 执行hset指令
func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

// sadd
func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

// lpush
func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	// res 列表中的数据个数
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(res), nil
}

// zadd
func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
