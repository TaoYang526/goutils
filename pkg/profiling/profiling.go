/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package profiling

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

var cache *ProfilingCache
var once sync.Once

func getCache() *ProfilingCache {
	once.Do(func() {
		cache = &ProfilingCache{
			profMap: make(map[string]Profiling),
		}
	})
	return cache
}

type ProfilingCache struct {
	profMap map[string]Profiling
	sync.RWMutex
}

type Profiling interface {
	StartExecution()
	StartExecutionWithTime(time time.Time)
	FinishExecution(save bool) error
	FinishExecutionWithTime(save bool, finishTime time.Time) error
	AddCheckpoint(name string) error
	AddCheckpointWithTime(name string, time time.Time) error
	GetCount() int
	Clear()
	GetTimeStatistics() *TimeStatistics
	GetQPSStatistics() (*QPSStatistics, error)
}

type DefaultProfiling struct {
	curExec    *execution
	stageInfos map[string]*stageInfo
	count      int
	startTime  time.Time
	finishTime time.Time

	sync.RWMutex
}

type stageInfo struct {
	from      string
	to        string
	samples   []stageSample
	totalTime time.Duration
}

type stageSample struct {
	fromTime time.Time
	toTime   time.Time
	duration time.Duration
}

/*
 * runtime
 */
type execution struct {
	startTime   time.Time
	checkpoints []*checkpoint
}

type checkpoint struct {
	name      string
	timestamp time.Time
}

/*
 * statistics
 */
type TimeStatistics struct {
	StartTime  time.Time
	FinishTime time.Time
	Count      int
	TotalTime  time.Duration
	AvgTime    time.Duration
	StagesTime []*StageTime
}

type StageTime struct {
	From       string
	To         string
	Count      int
	TotalTime  time.Duration
	AvgTime    time.Duration
	Percentage float64
}

type QPSStatistics struct {
	FromTime  time.Time
	ToTime    time.Time
	Count     int
	StagesQPS map[string]*StageQPS
}

type StageQPS struct {
	From string
	To   string
	// qps per second between from-time and to-time
	Samples []int
	AvgQPS  float64
	MaxQPS  int
}

func NewProfiling() Profiling {
	return NewProfilingWithTime(time.Now())
}

func NewProfilingWithTime(startTime time.Time) Profiling {
	return &DefaultProfiling{
		stageInfos: make(map[string]*stageInfo),
		//lock:       sync.RWMutex{},
		count:     0,
		startTime: startTime,
	}
}

func (prof *DefaultProfiling) GetCount() int {
	prof.RLock()
	defer prof.RUnlock()
	return prof.count
}

func (prof *DefaultProfiling) Clear() {
	prof.Lock()
	defer prof.Unlock()
	prof.stageInfos = make(map[string]*stageInfo)
	prof.count = 0
	prof.startTime = time.Now()
	prof.finishTime = time.Time{}
}

func (prof *DefaultProfiling) StartExecution() {
	prof.StartExecutionWithTime(time.Now())
}

func (prof *DefaultProfiling) StartExecutionWithTime(time time.Time) {
	prof.Lock()
	defer prof.Unlock()
	prof.curExec = &execution{
		startTime:   time,
		checkpoints: make([]*checkpoint, 0),
	}
}

func (prof *DefaultProfiling) FinishExecution(save bool) error {
	return prof.FinishExecutionWithTime(save, time.Now())
}

func (prof *DefaultProfiling) FinishExecutionWithTime(save bool, finishTime time.Time) error {
	prof.Lock()
	defer prof.Unlock()
	if prof.curExec == nil {
		return fmt.Errorf("[unexpected profiling]: not start yet")
	}
	if !save {
		prof.curExec = nil
		return nil
	}
	var lastCp, cp *checkpoint
	var lastTs time.Time
	var fromName string
	for _, cp = range prof.curExec.checkpoints {
		if lastCp == nil {
			fromName = "|"
			lastTs = prof.curExec.startTime
		} else {
			fromName = lastCp.name
			lastTs = lastCp.timestamp
		}
		prof.addStageInfo(fromName, cp.name, lastTs, cp.timestamp)
		lastCp = cp
	}
	if lastCp != nil {
		prof.addStageInfo(lastCp.name, "|", cp.timestamp, finishTime)
	}
	prof.curExec = nil
	prof.count += 1
	prof.finishTime = finishTime
	return nil
}

func (prof *DefaultProfiling) addStageInfo(from, to string, fromTime, toTime time.Time) {
	stageInfoKey := from + " -> " + to
	si := prof.stageInfos[stageInfoKey]
	if si == nil {
		si = &stageInfo{
			from:    from,
			to:      to,
			samples: make([]stageSample, 0, 10),
		}
		prof.stageInfos[stageInfoKey] = si
	}
	sample := stageSample{
		fromTime: fromTime,
		toTime:   toTime,
		duration: toTime.Sub(fromTime),
	}
	si.samples = append(si.samples, sample)
	si.totalTime += sample.duration
}

func (prof *DefaultProfiling) AddCheckpoint(name string) error {
	return prof.AddCheckpointWithTime(name, time.Now())
}

func (prof *DefaultProfiling) AddCheckpointWithTime(name string, time time.Time) error {
	prof.Lock()
	defer prof.Unlock()
	if prof.curExec == nil {
		return fmt.Errorf("[unexpected profiling]: not start yet")
	}
	newCp := &checkpoint{
		name:      name,
		timestamp: time,
	}
	prof.curExec.checkpoints = append(prof.curExec.checkpoints, newCp)
	return nil
}

func (prof *DefaultProfiling) GetTimeStatistics() *TimeStatistics {
	prof.RLock()
	defer prof.RUnlock()
	var totalTime time.Duration
	for _, si := range prof.stageInfos {
		totalTime += si.totalTime
	}
	stagesTime := make([]*StageTime, 0, len(prof.stageInfos))
	for _, si := range prof.stageInfos {
		stagesTime = append(stagesTime, &StageTime{
			From:       si.from,
			To:         si.to,
			TotalTime:  si.totalTime,
			AvgTime:    time.Duration(int64(si.totalTime) / int64(len(si.samples))),
			Count:      len(si.samples),
			Percentage: float64(si.totalTime.Nanoseconds()) / float64(totalTime.Nanoseconds()),
		})
	}
	sort.SliceStable(stagesTime, func(i, j int) bool {
		l := stagesTime[i]
		r := stagesTime[j]
		return r.Percentage < l.Percentage
	})
	return &TimeStatistics{
		StartTime:  prof.startTime,
		FinishTime: prof.finishTime,
		Count:      prof.count,
		TotalTime:  totalTime,
		AvgTime:    0,
		StagesTime: stagesTime,
	}
}

func (prof *DefaultProfiling) GetQPSStatistics() (*QPSStatistics, error) {
	prof.RLock()
	defer prof.RUnlock()
	qpsStat := &QPSStatistics{
		FromTime:  prof.startTime,
		ToTime:    prof.finishTime,
		Count:     prof.count,
		StagesQPS: make(map[string]*StageQPS),
	}
	samplesLen := qpsStat.ToTime.Unix() - qpsStat.FromTime.Unix() + 1
	for _, si := range prof.stageInfos {
		name := "(" + si.from + " -> )" + si.to
		qpsSamples := make([]int, samplesLen)
		for _, sp := range si.samples {
			index := sp.toTime.Unix() - qpsStat.FromTime.Unix()
			if index >= samplesLen {
				// the toTime of this stage is after the
				return nil, fmt.Errorf("the finish time (%s) of stage (%s -> %s) is after the finish time (%s) "+
					"of overall profiling process", sp.toTime.String(), si.from, si.to, prof.finishTime.String())
			}
			qpsSamples[index]++
		}
		maxQPS := 0
		totalQPS := 0
		nonZeroQPSCount := 0
		for _, v := range qpsSamples {
			if v > 0 {
				totalQPS += v
				nonZeroQPSCount++
				if v > maxQPS {
					maxQPS = v
				}
			}
		}
		qpsStat.StagesQPS[name] = &StageQPS{
			From:    si.from,
			To:      si.to,
			Samples: qpsSamples,
			AvgQPS:  float64(totalQPS*10/nonZeroQPSCount) / 10,
			MaxQPS:  maxQPS,
		}
	}
	return qpsStat, nil
}


func (pc *ProfilingCache) initProfiling(name string) Profiling {
	pc.Lock()
	defer pc.Unlock()
	profiling := NewProfiling()
	pc.profMap[name] = profiling
	return profiling
}

func (pc *ProfilingCache) getProfiling(name string) Profiling {
	pc.Lock()
	defer pc.Unlock()
	if prof, ok := pc.profMap[name]; ok {
		return prof
	}
	return nil
}

func InitProfilingInCache(name string) Profiling {
	return getCache().initProfiling(name)
}

func GetProfilingFromCache(name string) Profiling {
	return getCache().getProfiling(name)
}

func GetProfilingFromCacheOrEmpty(name string) Profiling {
	if prof := getCache().getProfiling(name); prof != nil {
		return prof
	}
	return NewEmptyProfiling()
}

type EmptyProfiling struct {
	Profiling
}

func NewEmptyProfiling() Profiling {
	return &EmptyProfiling{}
}

func (ep *EmptyProfiling) StartExecution() {}

func (ep *EmptyProfiling) StartExecutionWithTime(time time.Time) {}

func (ep *EmptyProfiling) FinishExecution(save bool) error {
	return nil
}

func (ep *EmptyProfiling) FinishExecutionWithTime(save bool, finishTime time.Time) error {
	return nil
}

func (ep *EmptyProfiling) AddCheckpoint(name string) error {
	return nil
}

func (ep *EmptyProfiling) AddCheckpointWithTime(name string, time time.Time) error {
	return nil
}

func (ep *EmptyProfiling) GetCount() int {
	return 0
}

func (ep *EmptyProfiling) GetTimeStatistics() *TimeStatistics {
	return nil
}

func (ep *EmptyProfiling) GetQPSStatistics() (*QPSStatistics, error) {
	return nil, nil
}
