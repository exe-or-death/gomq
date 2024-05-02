package gomq

import (
	"sync"
	"time"
)

type Config struct {
	sync.RWMutex
	reconnectTimeout time.Duration
	connectTimeout   time.Duration
	queueLen         int
}

func (c *Config) Default() {
	c.Lock()
	defer c.Unlock()

	c.reconnectTimeout = time.Second
	c.connectTimeout = time.Second * 3
	c.queueLen = 1024
}

func (c *Config) ReconnectTimeout() time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.reconnectTimeout
}

func (c *Config) SetReconnectTimeout(d time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.reconnectTimeout = d
}

func (c *Config) ConnectTimeout() time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.connectTimeout
}

func (c *Config) SetConnectTimeout(d time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.connectTimeout = d
}

func (c *Config) QueueLen() int {
	c.RLock()
	defer c.RUnlock()
	return c.queueLen
}

func (c *Config) SetQueueLen(queueLen int) {
	c.Lock()
	defer c.Unlock()
	c.queueLen = queueLen
}
