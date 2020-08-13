package geecache

import (
	"fmt"
	"geecache/geecache/singleflight"
	"log"
	"sync"
)

type Getter interface {
	Get(key string)([]byte, error)
}

type GetterFunc func(key string)([]byte, error)

func(f GetterFunc)Get(key string)([]byte, error) {
	return f(key)
}

type Group struct {
	name string
	getter Getter
	mainCache cache
	peers PeerPicker
	loader *singleflight.Group
}

var (
	mu sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter)*Group {
	if getter == nil {
		panic("nil Getter")
	}

	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name: name,
		getter: getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader: &singleflight.Group{},
	}
	groups[name] = g
	return g
}

func GetGroup(name string)*Group {
	mu.RLock()
	defer mu.RUnlock()
	g := groups[name]
	return g
}

func(g *Group)RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func(g *Group)Get(key string)(ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}

	return g.load(key)
}

func(g *Group)load(key string)(value ByteView, err error) {
	v, err := g.loader.Do(key, func()(interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if v, err := g.getFromPeer(peer, key); err == nil {
					return v, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)
	})
	if err == nil {
		return v.(ByteView), nil
	}
	return ByteView{}, err
}

func(g *Group)getFromPeer(peer PeerGetter, key string)(ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: bytes}, nil
}

func(g *Group)getLocally(key string)(ByteView, error) {
	b, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}

	v := ByteView{b: cloneBytes(b)}
	g.populateCache(key, v)
	return v, nil
}

func(g *Group)populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}
