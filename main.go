package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"os"
	"runtime/pprof"
)

type result struct {
	min   float64
	max   float64
	sum   float64
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
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	res := make(map[string]*result)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		// Up to the first ';' is the city, we'll hash that then mod shard it
		station, tempBytes, hasSemi := bytes.Cut(line, []byte(";"))
		if !hasSemi {
			continue
		}

		temp := bytesToFloat(tempBytes)

		stationStr := string(station)
		s, ok := res[stationStr]
		if !ok {
			res[stationStr] = &result{temp, temp, temp, 1}
		} else {
			s.min = math.Min(s.min, temp)
			s.max = math.Max(s.max, temp)
			s.sum += temp
			s.count++
		}

	}

	stations := make([]string, 0, len(res))
	for k := range maps.Keys(res) {
		stations = append(stations, k)
	}

	fmt.Fprint(output, "{")
	for i, station := range stations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := res[station]
		mean := s.sum / float64(s.count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, s.min, mean, s.max)
	}
	fmt.Print("}\n")
	return nil
}

func bytesToFloat(bytes []byte) float64 {
	negative := false
	idx := 0
	if bytes[0] == '-' {
		idx++
		negative = true
	}

	// Parse integer part
	val := 0.0
	for idx < len(bytes) && bytes[idx] != '.' {
		val = val*10 + float64(bytes[idx]-'0')
		idx++
	}

	// Skip decimal point
	idx++

	// Parse decimal part
	scale := 0.1
	for idx < len(bytes) {
		val += float64(bytes[idx]-'0') * scale
		scale *= 0.1
		idx++
	}

	if negative {
		val = -val
	}
	return val
}
