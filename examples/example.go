package main

import (
	"fmt"
	"kv"
)

func main() {
	opts := kv.DefaultOptions
	db, err := kv.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("key"), []byte("value1"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("key"))

	if err != nil {
		fmt.Println("err =", err)
	}
	fmt.Println("val =", string(val))

	err = db.Put([]byte("key2"), []byte("value2"))

	val, err = db.Get([]byte("key2"))

	if err != nil {
		fmt.Println("err =", err)
	}
	fmt.Println("val =", string(val))

	err = db.Delete([]byte("key"))
	if err != nil {
		panic(err)
	}
}
