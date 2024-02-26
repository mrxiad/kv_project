package main

type KV struct {
	store map[string]string
}

func NewKV() *KV {
	return &KV{
		store: make(map[string]string),
	}
}

func (kv *KV) Set(key, value string) {
	kv.store[key] = value

}

func main() {
	// 1. Create a new Key-Value store
	kv := NewKV()

	// 2. Set a key-value pair
	kv.Set("name", "John")
}
