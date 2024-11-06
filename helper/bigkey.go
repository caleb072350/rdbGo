package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/caleb072350/rdbGo/bytefmt"
	"github.com/caleb072350/rdbGo/core"
	"github.com/caleb072350/rdbGo/model"
	"github.com/emirpasic/gods/sets/treeset"
)

type redisTreeSet struct {
	set      *treeset.Set
	capacity int
}

func (h *redisTreeSet) GetMin() model.RedisObject {
	iter := h.set.Iterator()
	iter.End()
	if iter.Prev() {
		raw := iter.Value().(model.RedisObject)
		return raw
	}
	return nil
}

func (h *redisTreeSet) Append(x model.RedisObject) {
	// if heap is full && x.Size > minSize, then pop min
	if h.set.Size() == h.capacity {
		min := h.GetMin()
		if x.GetSize() > min.GetSize() {
			h.set.Remove(min)
		}
	}
	h.set.Add(x)
}

func (h *redisTreeSet) Dump() []model.RedisObject {
	result := make([]model.RedisObject, 0, h.set.Size())
	iter := h.set.Iterator()
	for iter.Next() {
		raw := iter.Value().(model.RedisObject)
		result = append(result, raw)
	}
	return result
}

func newRedisHeap(cap int) *redisTreeSet {
	s := treeset.NewWith(func(a, b interface{}) int {
		o1 := a.(model.RedisObject)
		o2 := b.(model.RedisObject)
		return o2.GetSize() - o1.GetSize()
	})
	return &redisTreeSet{
		set:      s,
		capacity: cap,
	}
}

// FindBiggestKeys read rdb file and find the largest N keys
// The invoker owns output, FindBiggestKeys won't close it
func FindBiggestKeys(rdbFilename string, topN int, output *os.File) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if topN <= 0 {
		return errors.New("n must greater than 0")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	p := core.NewDecoder(rdbFile)
	h := newRedisHeap(topN)
	err = p.Parse(func(object model.RedisObject) bool {
		h.Append(object)
		return true
	})
	if err != nil {
		return fmt.Errorf("parse rdb %s failed, %v", rdbFilename, err)
	}
	result := h.Dump()
	csvWriter := csv.NewWriter(output)
	defer csvWriter.Flush()
	for _, object := range result {
		err = csvWriter.Write([]string{
			strconv.Itoa(object.GetDBIndex()),
			object.GetKey(),
			object.GetType(),
			strconv.Itoa(object.GetSize()),
			bytefmt.FormatSize(uint64(object.GetSize())),
			strconv.Itoa(object.GetElemCount()),
		})
		if err != nil {
			return fmt.Errorf("write csv failed, %v", err)
		}
	}
	return nil
}
