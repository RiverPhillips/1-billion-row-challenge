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
	"unsafe"
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

	data, err := syscall.Mmap(int(file.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE|syscall.MAP_POPULATE)
	if err != nil {
		return err
	}
	defer syscall.Munmap(data)

	// Advise the kernel about our sequential access pattern
	syscall.Madvise(data, syscall.MADV_SEQUENTIAL)

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()
	chunkSize := len(data) / numWorkers

	results := make([]*hashtable, numWorkers)

	blockStart := 0
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {

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
	// Size chosen to keep load factor <2 for ~413k unique stations
	// 2^18 = 262,144 buckets → load factor ~1.6
	res := NewHashTable(1 << 18)

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
	// Per-worker hash table sized for ~34k stations (413k total / 12 CPUs)
	// 2^14 = 16,384 buckets → load factor ~2.0
	res := NewHashTable(1 << 14)

	i := start
	for i < endPos {
		semicolonPos := i
		for ; semicolonPos < endPos && data[semicolonPos] != ';'; semicolonPos++ {
		}
		if semicolonPos == endPos {
			break
		}

		hash := hashBytes(data, i, semicolonPos)

		stationKey := data[i:semicolonPos]
		lineEnd := semicolonPos + 1
		for ; lineEnd < endPos; lineEnd++ {
			if data[lineEnd] == '\n' {
				break
			}
		}

		tempStart := semicolonPos + 1
		tempBytes := data[tempStart:lineEnd]
		temp := bytesToFixedPointInt(tempBytes)

		s := res.get(hash, stationKey)
		if s == nil {
			// Create new stats entry
			s = &stats{temp, temp, temp, 1}
			res.add(hash, data[i:semicolonPos], s)
		} else {
			// Update existing stats
			if temp < s.min {
				s.min = temp
			}
			if temp > s.max {
				s.max = temp
			}
			s.sum += temp
			s.count++
		}

		// Move to next line
		i = lineEnd + 1

	}
	return res
}

func bytesToFixedPointInt(bytes []byte) int32 {
	// Unsafe optimization: read multiple bytes at once and decode using bit manipulation
	// Temperature formats: [-]D.D (3-4 bytes) or [-]DD.D (4-5 bytes)
	// This eliminates branches and bounds checking in the hot path (1B calls)

	// Read first 8 bytes as uint64 (safe because mmap buffer has extra space)
	word := *(*uint64)(unsafe.Pointer(&bytes[0]))

	// Extract individual bytes using bit shifting
	b0 := byte(word)
	b1 := byte(word >> 8)
	b2 := byte(word >> 16)
	b3 := byte(word >> 24)
	b4 := byte(word >> 32)

	var val int32
	var neg int32

	if b0 == '-' {
		neg = -1
		// Format: -D.D or -DD.D
		if b2 == '.' {
			// -D.D: b1=digit, b2='.', b3=digit
			val = int32(b1-'0')*10 + int32(b3-'0')
		} else {
			// -DD.D: b1=digit, b2=digit, b3='.', b4=digit
			val = int32(b1-'0')*100 + int32(b2-'0')*10 + int32(b4-'0')
		}
	} else {
		neg = 1
		// Format: D.D or DD.D
		if b1 == '.' {
			// D.D: b0=digit, b1='.', b2=digit
			val = int32(b0-'0')*10 + int32(b2-'0')
		} else {
			// DD.D: b0=digit, b1=digit, b2='.', b3=digit
			val = int32(b0-'0')*100 + int32(b1-'0')*10 + int32(b3-'0')
		}
	}

	return val * neg
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

func hashBytes(data []byte, start, end int) fnvHash {
	h := newFnvHash()
	for i := start; i < end; i++ {
		h *= fnvPrime
		h ^= fnvHash(data[i])
	}
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

		if ht.items[index].hash == hash && bytes.Equal(ht.items[index].key, key) {
			return ht.items[index].value
		}

		index = (index + 1) % uint64(len(ht.items))

		if index == originalIndex {
			return nil
		}
	}
}
