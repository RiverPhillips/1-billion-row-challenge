package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"syscall"
)

type stats struct {
	min   int32
	max   int32
	sum   int32
	count uint64
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	args := flag.Args()
	if len(args) != 1 {
		log.Fatal("Usage: 1brc <File>")
	}

	fileName := args[0]

	err := process(os.Stdout, fileName)
	if err != nil {
		log.Fatal(err)
	}
}

func process(output io.Writer, fileName string) error {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return err
	}
	defer syscall.Munmap(data)

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()
	chunkSize := len(data) / numWorkers

	results := make([]*hashtable, numWorkers)

	blockStart := 0
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		blockEnd := blockStart + chunkSize
		if i == numWorkers-1 {
			blockEnd = len(data)
		} else {
			for blockEnd < len(data)-1 && data[blockEnd] != '\n' {
				blockEnd++
			}
			if blockEnd < len(data) {
				blockEnd++
			}
		}

		go func(i, blockStart, blockEnd int) {
			defer wg.Done()
			results[i] = processData(data, blockStart, blockEnd)
		}(i, blockStart, blockEnd)
		blockStart = blockEnd
	}

	wg.Wait()

	res := mergeHashTables(results)

	// Create slice of just the populated items
	populated := make([]item, 0, res.size)
	for _, item := range res.items {
		if item.value != nil {
			populated = append(populated, item)
		}
	}

	// Sort only the populated items
	sort.Slice(populated, func(i, j int) bool {
		return bytes.Compare(populated[i].key, populated[j].key) < 0
	})

	b := bufio.NewWriter(output)

	const div10 = 0.1
	b.WriteByte('{')
	for i, item := range populated {
		if i > 0 {
			b.WriteString(", ")
		}
		stats := item.value
		mean := float64(stats.sum) / float64(stats.count) * div10

		b.Write(item.key)
		fmt.Fprintf(b, "=%.1f/%.1f/%.1f",
			float64(stats.min)*div10,
			mean,
			float64(stats.max)*div10)
	}
	b.WriteString("}\n")

	b.Flush()
	return nil
}

func mergeHashTables(tables []*hashtable) *hashtable {
	res := NewHashTable(1 << 16)

	for _, table := range tables {
		for _, item := range table.items {
			if item.value == nil {
				continue
			}

			s := res.get(item.hash, item.key)
			if s == nil {
				res.add(item.hash, item.key, &stats{
					max:   item.value.max,
					min:   item.value.min,
					sum:   item.value.sum,
					count: item.value.count,
				})
			} else {
				s.min = min(s.min, item.value.min)
				s.max = max(s.max, item.value.max)
				s.sum += item.value.sum
				s.count += item.value.count
			}

		}
	}

	return res
}

func processData(data []byte, start int, endPos int) *hashtable {
	res := NewHashTable(1 << 16)

	hash := newFnvHash()
	for i := start; i < endPos; i++ {
		b := data[i]
		if b == ';' {
			station := data[start:i]

			// Find the line end
			lineEnd := i + 1
			for ; lineEnd < len(data) && data[lineEnd] != '\n'; lineEnd++ {
			}

			temp := bytesToFixedPointInt(data[i+1 : lineEnd])

			s := res.get(hash, station)
			if s == nil {
				res.add(hash, station, &stats{temp, temp, temp, 1})
			} else {
				s.min = min(s.min, temp)
				s.max = max(s.max, temp)
				s.sum += temp
				s.count++
			}

			// Reset for next line
			i = lineEnd
			start = lineEnd + 1
			hash = newFnvHash()
		} else if b == '\n' {
			// Skip newlines
			start = i + 1
			hash = newFnvHash()
		} else {
			// Build hash incrementally for station name
			hash = hashByte(hash, b)
		}

	}
	return res

}

func bytesToFixedPointInt(bytes []byte) int32 {
	negative := false
	idx := 0
	if bytes[0] == '-' {
		idx++
		negative = true
	}

	// Parse integer part
	val := int32(bytes[idx] - '0')
	idx++
	if bytes[idx] != '.' {
		val = val*10 + int32(bytes[idx]-'0')
		idx++
	}
	idx++ // skip decimal
	val = val*10 + int32(bytes[idx]-'0')

	if negative {
		val = -val
	}
	return val
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

type fnvHash = uint64

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

func newFnvHash() fnvHash {
	return fnvOffset
}

func hashByte(h fnvHash, b byte) fnvHash {
	h *= fnvPrime
	h = h ^ uint64(b)
	return h
}

type item struct {
	hash  fnvHash
	key   []byte
	value *stats
}

type hashtable struct {
	items []item
	size  uint64
}

func NewHashTable(numBuckets uint64) *hashtable {
	return &hashtable{
		items: make([]item, numBuckets),
		size:  0,
	}
}

func (ht *hashtable) add(hash fnvHash, key []byte, v *stats) {
	index := hash % uint64(len(ht.items))
	originalIndex := index

	// Keep probing until we find an empty slot
	for {
		if ht.items[index].value == nil {
			ht.items[index] = item{key: key, value: v, hash: hash}
			ht.size++
			return
		}

		if bytes.Equal(ht.items[index].key, key) {
			ht.items[index].value = v
			return
		}

		index = (index + 1) % uint64(len(ht.items))

		if index == originalIndex {
			panic("Hashtable is full")
		}
	}
}

func (ht *hashtable) get(hash fnvHash, key []byte) *stats {
	index := hash % uint64(len(ht.items))
	originalIndex := index

	// Keep probing until we find the key or an empty slot
	for {
		if ht.items[index].value == nil {
			return nil
		}

		if bytes.Equal(ht.items[index].key, key) {
			return ht.items[index].value
		}

		index = (index + 1) % uint64(len(ht.items))

		if index == originalIndex {
			return nil
		}
	}
}
