package gopinba

import (
	"testing"
	"time"

	"github.com/mkevac/gopinba/Pinba"
)

func inStringSlice(haystack []string, needle string) int {
	for i, s := range haystack {
		if s == needle {
			return i
		}
	}
	return -1
}

func inUint32Slice(haystack []uint32, needle uint32) int {
	for i, s := range haystack {
		if s == needle {
			return i
		}
	}
	return -1
}

func TestMergeTags(t *testing.T) {
	var (
		pbreq Pinba.Request
		tags  = map[string]string{
			"marko": "kevac",
			"margo": "kevac",
			"lazo":  "kevac",
		}
	)

	mergeTags(&pbreq, tags)

	if len(pbreq.Dictionary) != 4 {
		t.Fatalf("dictionary length unexpected (expected 4, actual %v (%v))", len(pbreq.Dictionary), pbreq.Dictionary)
	}

	if len(pbreq.TagName) != 3 {
		t.Fatalf("TagName length unexpected (expected 3, actual %v (%v))", len(pbreq.TagName), pbreq.TagName)
	}

	if len(pbreq.TagValue) != 3 {
		t.Fatalf("TagValue length unexpected (expected 3, actual %v (%v))", len(pbreq.TagValue), pbreq.TagValue)
	}

	for k, v := range tags {
		ki := inStringSlice(pbreq.Dictionary, k)
		if ki == -1 {
			t.Fatalf("%v not found in %v", k, pbreq.Dictionary)
		}

		if -1 == inUint32Slice(pbreq.TagName, uint32(ki)) {
			t.Fatalf("%v not found in %v", ki, pbreq.TagName)
		}

		vi := inStringSlice(pbreq.Dictionary, v)
		if vi == -1 {
			t.Fatalf("%v not found in %v", v, pbreq.Dictionary)
		}

		if -1 == inUint32Slice(pbreq.TagValue, uint32(vi)) {
			t.Fatalf("%v not found in %v", vi, pbreq.TagValue)
		}
	}
}

func TestMergeTimerTags(t *testing.T) {
	var (
		pbreq Pinba.Request
		tags1 = map[string]string{
			"marko1": "kevac1",
			"margo1": "kevac1",
			"lazo1":  "kevac1",
		}
		tags2 = map[string]string{
			"marko2": "kevac2",
			"margo2": "kevac2",
			"lazo2":  "kevac2",
		}
	)

	mergeTimerTags(&pbreq, tags1)
	mergeTimerTags(&pbreq, tags2)

	if len(pbreq.Dictionary) != 8 {
		t.Fatalf("dictionary length unexpected (expected 8, actual %v)", len(pbreq.Dictionary))
	}

	if len(pbreq.TimerTagName) != 6 {
		t.Fatalf("TimerTagName length unexpected (expected 6, actual %v)", len(pbreq.TimerTagName))
	}

	if len(pbreq.TimerTagValue) != 6 {
		t.Fatalf("TimerTagValue length unexpected (expected 6, actual %v)", len(pbreq.TimerTagValue))
	}

	if len(pbreq.TimerTagCount) != 2 {
		t.Fatalf("TimerTagCount length unexpected (expected 2, actual %v)", len(pbreq.TimerTagCount))
	}

	for _, tags := range []map[string]string{tags1, tags2} {
		for k, v := range tags {
			ki := inStringSlice(pbreq.Dictionary, k)
			if ki == -1 {
				t.Fatalf("%v not found in %v", k, pbreq.Dictionary)
			}

			if -1 == inUint32Slice(pbreq.TimerTagName, uint32(ki)) {
				t.Fatalf("%v not found in %v", ki, pbreq.TimerTagName)
			}

			vi := inStringSlice(pbreq.Dictionary, v)
			if vi == -1 {
				t.Fatalf("%v not found in %v", v, pbreq.Dictionary)
			}

			if -1 == inUint32Slice(pbreq.TimerTagValue, uint32(vi)) {
				t.Fatalf("%v not found in %v", vi, pbreq.TimerTagValue)
			}
		}
	}

}

func TestRequest(t *testing.T) {
	pc, err := NewClient("10.0.0.1:30002")
	if err != nil {
		t.Errorf("NewClient() returned error: %v", err)
	}

	req := Request{}

	for i := 0; i < 5; i++ {

		req.Hostname = "hostname"
		req.ServerName = "servername"
		req.ScriptName = "scriptname"
		req.RequestCount = 1
		req.RequestTime = 145987 * time.Microsecond
		req.DocumentSize = 1024

		err = pc.SendRequest(&req)
		if err != nil {
			t.Errorf("SendRequest() returned error: %v", err)
		}
	}
}

func BenchmarkSimple(b *testing.B) {
	pc, err := NewClient(":6666")
	if err != nil {
		b.Fatalf("NewClient() returned error: %v", err)
	}

	for i := 0; i < b.N; i++ {
		req := Request{}

		req.Hostname = "hostname"
		req.ServerName = "servername"
		req.ScriptName = "scriptname"
		req.RequestCount = 1
		req.RequestTime = 145987 * time.Microsecond
		req.DocumentSize = 1024

		err = pc.SendRequest(&req)
		if err != nil {
			b.Errorf("SendRequest() returned error: %v", err)
		}
	}
}
