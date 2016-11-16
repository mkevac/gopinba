package gopinba

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mkevac/gopinba/Pinba"
)

type Client struct {
	initialized bool
	address     string
	conn        net.Conn
}

type Timer struct {
	Tags map[string]string

	// private stuff for simpler api
	stopped  bool
	started  time.Time
	duration time.Duration
}

type Request struct {
	Hostname     string
	ServerName   string
	ScriptName   string
	RequestCount uint32
	RequestTime  time.Duration
	DocumentSize uint32
	MemoryPeak   uint32
	Utime        float32
	Stime        float32
	timers       []Timer
	Status       uint32
	Tags         map[string]string
	lk           sync.Mutex
}

func NewClient(address string) (*Client, error) {
	pc := &Client{address: address}
	conn, err := net.Dial("udp", address)
	if err != nil {
		return nil, err
	}

	pc.conn = conn
	pc.initialized = true

	return pc, nil
}

func iN(haystack []string, needle string) (int, bool) {
	for i, s := range haystack {
		if s == needle {
			return i, true
		}
	}
	return -1, false
}

func useAndUpdateDictionary(dictionary []string, tags map[string]string) (map[uint32]uint32, []string) {
	newTags := make(map[uint32]uint32)
	newDict := dictionary

	for k, v := range tags {

		var kpos int
		var vpos int
		var pos int
		var exist bool

		pos, exist = iN(newDict, k)
		if exist {
			kpos = pos
		} else {
			newDict = append(newDict, k)
			kpos = len(newDict) - 1
		}

		pos, exist = iN(newDict, v)
		if exist {
			vpos = pos
		} else {
			newDict = append(newDict, v)
			vpos = len(newDict) - 1
		}

		newTags[uint32(kpos)] = uint32(vpos)
	}

	return newTags, newDict
}

func (pc *Client) SendRequest(request *Request) error {

	if !pc.initialized {
		return fmt.Errorf("Client not initialized")
	}

	pbreq := Pinba.Request{
		Hostname:      request.Hostname,
		ServerName:    request.ServerName,
		ScriptName:    request.ScriptName,
		RequestCount:  request.RequestCount,
		RequestTime:   float32(request.RequestTime.Seconds()),
		DocumentSize:  request.DocumentSize,
		MemoryPeak:    request.MemoryPeak,
		RuUtime:       request.Utime,
		RuStime:       request.Stime,
		Status:        request.Status,
		TimerHitCount: make([]uint32, 0),
		TimerValue:    make([]float32, 0),
		TagName:       make([]uint32, 0),
		TagValue:      make([]uint32, 0),
		Dictionary:    make([]string, 0),
	}
	
	tagsMap, newDict := useAndUpdateDictionary(pbreq.Dictionary, request.Tags)
	pbreq.Dictionary = newDict
	for k, v := range tagsMap {
		pbreq.TagName = append(pbreq.TagName, k)
		pbreq.TagValue = append(pbreq.TagValue, v)
	}

	for _, timer := range request.timers {
		pbreq.TimerHitCount = append(pbreq.TimerHitCount, 1)
		pbreq.TimerValue = append(pbreq.TimerValue, float32(timer.duration.Seconds()))
		tagsMap, newDict := useAndUpdateDictionary(pbreq.Dictionary, timer.Tags)
		pbreq.Dictionary = newDict
		pbreq.TimerTagCount = append(pbreq.TimerTagCount, uint32(len(tagsMap)))

		for k, v := range tagsMap {
			pbreq.TimerTagName = append(pbreq.TimerTagName, k)
			pbreq.TimerTagValue = append(pbreq.TimerTagValue, v)
		}
	}

	buf, err := proto.Marshal(&pbreq)
	if err != nil {
		return err
	}

	_, err = pc.conn.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (req *Request) AddTimer(timer *Timer) {
	req.lk.Lock()
	defer req.lk.Unlock()

	req.timers = append(req.timers, *timer)
}

// this is exactly the same as AddTimer
//  exists only to have api naming similar to pinba php extension
func (req *Request) TimerAdd(timer *Timer) {
	timer.Stop()
	req.AddTimer(timer)
}

func TimerStart(tags map[string]string) *Timer {
	return &Timer{
		duration: 0,
		Tags:     tags,
		stopped:  false,
		started:  time.Now(),
	}
}

func NewTimer(tags map[string]string, duration time.Duration) *Timer {
	return &Timer{
		duration: duration,
		Tags:     tags,
		stopped:  true,
		started:  time.Now().Add(-duration),
	}
}

func (t *Timer) Stop() {
	if !t.stopped {
		t.stopped = true
		t.duration = time.Now().Sub(t.started)
	}
}

func (t *Timer) GetDuration() time.Duration {
	return t.duration
}
