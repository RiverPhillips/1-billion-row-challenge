//go:build amd64 && !nosimd

package main

// findByte searches for target byte in data[start:end] using AVX2 SIMD
// Returns the position of the first occurrence, or end if not found
// Processes 32 bytes at a time using AVX2 instructions
func findByte(data []byte, start int, end int, target byte) int

// hasAVX2 returns true if the CPU supports AVX2 instructions
func hasAVX2() bool
