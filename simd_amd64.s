#include "textflag.h"

// func findByte(data []byte, start int, end int, target byte) int
TEXT ·findByte(SB), NOSPLIT, $8-49
    // Load arguments
    MOVQ    data_base+0(FP), SI     // SI = data pointer
    MOVQ    start+24(FP), AX        // AX = start
    MOVQ    end+32(FP), DX          // DX = end
    MOVB    target+40(FP), CX       // CX = target byte

    // Early exit if start >= end
    CMPQ    AX, DX
    JGE     not_found

    // Calculate actual start pointer
    ADDQ    AX, SI                  // SI = &data[start]
    SUBQ    AX, DX                  // DX = end - start (remaining length)

    // Broadcast target byte to all 32 bytes of YMM0
    // Store byte to stack, then broadcast from memory
    MOVB    CX, 0(SP)
    VPBROADCASTB    0(SP), Y0       // Y0 = [target, target, ..., target]

    // Process 32 bytes at a time
    MOVQ    DX, R8                  // R8 = remaining length
    SHRQ    $5, R8                  // R8 = remaining length / 32 (number of chunks)
    JZ      check_remainder         // If less than 32 bytes, go to remainder

simd_loop:
    // Load 32 bytes
    VMOVDQU (SI), Y1                // Y1 = 32 bytes from data

    // Compare all 32 bytes with target
    VPCMPEQB    Y0, Y1, Y2          // Y2 = comparison result (0xFF where equal)

    // Convert to bitmask
    VPMOVMSKB   Y2, R9              // R9 = bitmask of matches

    // Check if any byte matched
    TESTL   R9, R9
    JNZ     found_in_chunk

    // Move to next chunk
    ADDQ    $32, SI
    DECQ    R8
    JNZ     simd_loop

check_remainder:
    // Process remaining bytes (less than 32) with scalar code
    ANDQ    $31, DX                 // DX = remaining length % 32
    JZ      not_found               // If no remainder, not found

scalar_loop:
    MOVB    (SI), BX
    CMPB    BX, CX
    JE      found_scalar
    INCQ    SI
    DECQ    DX
    JNZ     scalar_loop
    JMP     not_found

found_in_chunk:
    // Find position of first match using trailing zero count
    TZCNTL  R9, R9                  // R9 = position of first set bit
    ADDQ    R9, SI                  // SI = pointer to found byte
    JMP     calculate_result

found_scalar:
    // Already pointing to the found byte

calculate_result:
    // Calculate result = (current SI - original data pointer) + original start
    MOVQ    data_base+0(FP), BX     // BX = original data pointer
    SUBQ    BX, SI                  // SI = offset from start of data
    MOVQ    SI, ret+48(FP)          // Return offset
    VZEROUPPER                       // Clean up AVX state
    RET

not_found:
    MOVQ    end+32(FP), AX          // Return end
    MOVQ    AX, ret+48(FP)
    VZEROUPPER                       // Clean up AVX state
    RET

// func hasAVX2() bool
TEXT ·hasAVX2(SB), NOSPLIT, $0-1
    // Check CPUID for AVX2 support
    // EAX=7, ECX=0: Extended Features
    MOVL    $7, AX
    MOVL    $0, CX
    CPUID

    // AVX2 is bit 5 of EBX
    ANDL    $0x20, BX
    SHRL    $5, BX
    MOVB    BX, ret+0(FP)
    RET
