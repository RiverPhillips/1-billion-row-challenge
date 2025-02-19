package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
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
		line := scanner.Text()
		// Up to the first ';' is the city, we'll hash that then mod shard it
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		tempDouble, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			return err
		}

		s, ok := res[station]
		if !ok {
			res[station] = &result{tempDouble, tempDouble, tempDouble, 1}
		} else {
			s.min = math.Min(s.min, tempDouble)
			s.max = math.Max(s.max, tempDouble)
			s.sum += tempDouble
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
