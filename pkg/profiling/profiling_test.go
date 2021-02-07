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
	"math/rand"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestProfilingCommon(t *testing.T) {
	prof := NewProfiling()
	numSamples := 10
	for i := 0; i < numSamples; i++ {
		prof.StartExecution()
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		prof.AddCheckpoint("s1")
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		prof.AddCheckpoint("s2")
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		prof.AddCheckpoint("s2")
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		prof.AddCheckpoint("s3")
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		prof.FinishExecution(true)
	}
	assert.Equal(t, prof.GetCount(), numSamples)
	// check time statistics
	stat := prof.GetTimeStatistics()
	assert.Equal(t, len(stat.StagesTime), 5)
	for _, stageTime := range stat.StagesTime {
		fmt.Printf("%+v \n", stageTime)
	}
	// check QPS statistics
	qpsStat, err := prof.GetQPSStatistics()
	assert.NilError(t, err)
	assert.Equal(t, qpsStat.Count, numSamples)
	assert.Equal(t, len(qpsStat.StagesQPS), 5)
	for _, qps := range qpsStat.StagesQPS {
		fmt.Printf("%+v \n", qps)
	}
}

func TestProfilingIfSave(t *testing.T) {
	prof := NewProfiling()
	prof.StartExecution()
	prof.AddCheckpoint("c1")
	prof.FinishExecution(false)
	assert.Equal(t, prof.GetCount(), 0)
	prof.StartExecution()
	prof.AddCheckpoint("c1")
	prof.FinishExecution(true)
	assert.Equal(t, prof.GetCount(), 1)
}

func TestProfilingCache(t *testing.T) {
	testProf := "test"
	prof := InitProfilingInCache(testProf)
	assert.Assert(t, prof != nil)
	profFromCache := GetProfilingFromCache(testProf)
	assert.Equal(t, prof, profFromCache)
	profFromCache.StartExecution()
	profFromCache.AddCheckpoint("c1")
	profFromCache.AddCheckpoint("c2")
	profFromCache.AddCheckpoint("c2")
	profFromCache.FinishExecution(true)
	assert.Equal(t, profFromCache.GetCount(), 1)
	timeStats := profFromCache.GetTimeStatistics()
	assert.Equal(t, len(timeStats.StagesTime), 4)
	for _, st := range timeStats.StagesTime {
		assert.Equal(t, st.Count, 1)
	}
	qpsStats, err := profFromCache.GetQPSStatistics()
	assert.NilError(t, err)
	assert.Equal(t, len(qpsStats.StagesQPS), 4)
	for _, sq := range qpsStats.StagesQPS {
		assert.Equal(t, len(sq.Samples), 1)
	}
	profFromCache.Clear()
	assert.Equal(t, profFromCache.GetCount(), 0)
	// empty
	emptyProf := GetProfilingFromCacheOrEmpty("non-existed")
	emptyProf.StartExecution()
	emptyProf.AddCheckpoint("c1")
	emptyProf.FinishExecution(true)
	assert.Equal(t, profFromCache.GetCount(), 0)
}
