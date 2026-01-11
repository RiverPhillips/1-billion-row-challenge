//go:build !amd64 || nosimd

package main

// findByte fallback implementation for non-AVX2 systems
func findByte(data []byte, start int, end int, target byte) int {
	for i := start; i < end; i++ {
		if data[i] == target {
			return i
		}
	}
	return end
}

func hasAVX2() bool {
	return false
}
