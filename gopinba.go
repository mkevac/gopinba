package gopinba

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/mkevac/gopinba/Pinba"
	"net"
	"time"
)

type PinbaClient struct {
	initialized bool
	address     string
	conn        net.Conn
}

type PinbaTimer struct {
	Name     string
	Duration float32
	Tags     map[string]string
}

type PinbaRequest struct {
	Hostname     string
	ServerName   string
	ScriptName   string
	RequestCount uint32
	RequestTime  time.Duration
	DocumentSize uint32
	MemoryPeak   uint32
	Utime        float32
	Stime        float32
	timers       []PinbaTimer
}

func NewPinbaClient(address string) (*PinbaClient, error) {
	pc := &PinbaClient{address: address}
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

func (pc *PinbaClient) SendRequest(request *PinbaRequest) error {

	if !pc.initialized {
		return fmt.Errorf("PinbaClient not initialized")
	}

	pbreq := Pinba.Request{}
	pbreq.Hostname = proto.String(request.Hostname)
	pbreq.ServerName = proto.String(request.ServerName)
	pbreq.ScriptName = proto.String(request.ScriptName)
	pbreq.RequestCount = proto.Uint32(request.RequestCount)
	pbreq.RequestTime = proto.Float32(float32(request.RequestTime.Seconds()))
	pbreq.DocumentSize = proto.Uint32(request.DocumentSize)
	pbreq.MemoryPeak = proto.Uint32(request.MemoryPeak)
	pbreq.RuUtime = proto.Float32(request.Utime)
	pbreq.RuStime = proto.Float32(request.Stime)
	pbreq.TimerHitCount = make([]uint32, 0)
	pbreq.TimerValue = make([]float32, 0)
	pbreq.Dictionary = make([]string, 0)

	for _, timer := range request.timers {
		pbreq.TimerHitCount = append(pbreq.TimerHitCount, 1)
		pbreq.TimerValue = append(pbreq.TimerValue, timer.Duration)
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

func (req *PinbaRequest) AddTimer(timer PinbaTimer) {
	req.timers = append(req.timers, timer)
}
