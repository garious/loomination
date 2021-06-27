#pragma once
/**
 * @brief Solana C-based BPF program utility functions and types
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Pick up static_assert if C11 or greater
 *
 * Inlined here until <assert.h> is available
 */
#if (defined _ISOC11_SOURCE || (defined __STDC_VERSION__ && __STDC_VERSION__ >= 201112L)) && !defined (__cplusplus)
#undef static_assert
#define static_assert _Static_assert
#endif

/**
 * Numeric types
 */
#ifndef __LP64__
#error LP64 data model required
#endif

typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef signed short int16_t;
typedef unsigned short uint16_t;
typedef signed int int32_t;
typedef unsigned int uint32_t;
typedef signed long int int64_t;
typedef unsigned long int uint64_t;
typedef int64_t ssize_t;
typedef uint64_t size_t;

#if defined (__cplusplus) || defined(static_assert)
static_assert(sizeof(int8_t) == 1);
static_assert(sizeof(uint8_t) == 1);
static_assert(sizeof(int16_t) == 2);
static_assert(sizeof(uint16_t) == 2);
static_assert(sizeof(int32_t) == 4);
static_assert(sizeof(uint32_t) == 4);
static_assert(sizeof(int64_t) == 8);
static_assert(sizeof(uint64_t) == 8);
#endif

/**
 * Minimum of signed integral types
 */
#define INT8_MIN   (-128)
#define INT16_MIN  (-32767-1)
#define INT32_MIN  (-2147483647-1)
#define INT64_MIN  (-9223372036854775807L-1)

/**
 * Maximum of signed integral types
 */
#define INT8_MAX   (127)
#define INT16_MAX  (32767)
#define INT32_MAX  (2147483647)
#define INT64_MAX  (9223372036854775807L)

/**
 * Maximum of unsigned integral types
 */
#define UINT8_MAX   (255)
#define UINT16_MAX  (65535)
#define UINT32_MAX  (4294967295U)
#define UINT64_MAX  (18446744073709551615UL)

/**
 * NULL
 */
#define NULL 0

/** Indicates the instruction was processed successfully */
#define SUCCESS 0

/**
 * Builtin program status values occupy the upper 32 bits of the program return
 * value.  Programs may define their own error values but they must be confined
 * to the lower 32 bits.
 */
#define TO_BUILTIN(error) ((uint64_t)(error) << 32)

/** Note: Not applicable to program written in C */
#define ERROR_CUSTOM_ZERO TO_BUILTIN(1)
/** The arguments provided to a program instruction where invalid */
#define ERROR_INVALID_ARGUMENT TO_BUILTIN(2)
/** An instruction's data contents was invalid */
#define ERROR_INVALID_INSTRUCTION_DATA TO_BUILTIN(3)
/** An account's data contents was invalid */
#define ERROR_INVALID_ACCOUNT_DATA TO_BUILTIN(4)
/** An account's data was too small */
#define ERROR_ACCOUNT_DATA_TOO_SMALL TO_BUILTIN(5)
/** An account's balance was too small to complete the instruction */
#define ERROR_INSUFFICIENT_FUNDS TO_BUILTIN(6)
/** The account did not have the expected program id */
#define ERROR_INCORRECT_PROGRAM_ID TO_BUILTIN(7)
/** A signature was required but not found */
#define ERROR_MISSING_REQUIRED_SIGNATURES TO_BUILTIN(8)
/** An initialize instruction was sent to an account that has already been initialized */
#define ERROR_ACCOUNT_ALREADY_INITIALIZED TO_BUILTIN(9)
/** An attempt to operate on an account that hasn't been initialized */
#define ERROR_UNINITIALIZED_ACCOUNT TO_BUILTIN(10)
/** The instruction expected additional account keys */
#define ERROR_NOT_ENOUGH_ACCOUNT_KEYS TO_BUILTIN(11)
/** Note: Not applicable to program written in C */
#define ERROR_ACCOUNT_BORROW_FAILED TO_BUILTIN(12)
/** The length of the seed is too long for address generation */
#define MAX_SEED_LENGTH_EXCEEDED TO_BUILTIN(13)
/** Provided seeds do not result in a valid address */
#define INVALID_SEEDS TO_BUILTIN(14)

/**
 * Boolean type
 */
#ifndef __cplusplus
#include <stdbool.h>
#endif

/**
 * Prints a string to stdout
 */
void sol_log_(const char *, uint64_t);
#define sol_log(message) sol_log_(message, sol_strlen(message))

/**
 * Prints a 64 bit values represented in hexadecimal to stdout
 */
void sol_log_64_(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t);
#define sol_log_64 sol_log_64_

/**
 * Prints the current compute unit consumption to stdout
 */
void sol_log_compute_units_();
#define sol_log_compute_units() sol_log_compute_units_()

/**
 * Size of Public key in bytes
 */
#define SIZE_PUBKEY 32

/**
 * Public key
 */
typedef struct {
  uint8_t x[SIZE_PUBKEY];
} SolPubkey;

/**
 * Compares two public keys
 *
 * @param one First public key
 * @param two Second public key
 * @return true if the same
 */
static bool SolPubkey_same(const SolPubkey *one, const SolPubkey *two) {
  for (int i = 0; i < sizeof(*one); i++) {
    if (one->x[i] != two->x[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Keyed Account
 */
typedef struct {
  SolPubkey *key;      /** Public key of the account */
  uint64_t *lamports;  /** Number of lamports owned by this account */
  uint64_t data_len;   /** Length of data in bytes */
  uint8_t *data;       /** On-chain data within this account */
  SolPubkey *owner;    /** Program that owns this account */
  uint64_t rent_epoch; /** The epoch at which this account will next owe rent */
  bool is_signer;      /** Transaction was signed by this account's key? */
  bool is_writable;    /** Is the account writable? */
  bool executable;     /** This account's data contains a loaded program (and is now read-only) */
} SolAccountInfo;

/**
 * Copies memory
 */
static void sol_memcpy(void *dst, const void *src, int len) {
  for (int i = 0; i < len; i++) {
    *((uint8_t *)dst + i) = *((const uint8_t *)src + i);
  }
}

/**
 * Compares memory
 */
static int sol_memcmp(const void *s1, const void *s2, int n) {
  for (int i = 0; i < n; i++) {
    uint8_t diff = *((uint8_t *)s1 + i) - *((const uint8_t *)s2 + i);
    if (diff) {
      return diff;
    }
  }
  return 0;
}

/**
 * Fill a byte string with a byte value
 */
static void *sol_memset(void *b, int c, size_t len) {
  uint8_t *a = (uint8_t *) b;
  while (len > 0) {
    *a = c;
    a++;
    len--;
  }
}

/**
 * Find length of string
 */
static size_t sol_strlen(const char *s) {
  size_t len = 0;
  while (*s) {
    len++;
    s++;
  }
  return len;
}

/**
 * Computes the number of elements in an array
 */
#define SOL_ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))


/**
 * Internal memory alloc/free function
 */
void *sol_alloc_free_(uint64_t size, void *ptr);

/**
 * Alloc zero-initialized memory
 */
static void *sol_calloc(size_t nitems, size_t size) {
  return sol_alloc_free_(nitems * size, 0);
}

/**
 * Deallocates the memory previously allocated by sol_calloc
 */
static void sol_free(void *ptr) {
  (void) sol_alloc_free_(0, ptr);
}

/**
 * The Solana runtime provides a memory region that is available to programs at
 * a fixed virtual address and length. The builtin functions `sol_calloc` and
 * `sol_free` call into the Solana runtime to allocate from this memory region
 * for heap operations.  Because the memory region is directly available to
 * programs another option is a program can implement their own heap directly on
 * top of that region.  If a program chooses to implement their own heap they
 * should not call the builtin heap functions because they will conflict.
 * `HEAP_START_ADDRESS` and `HEAP_LENGTH` specify the memory region's start
 * virtual address and length.
 */
#define HEAP_START_ADDRESS (uint64_t)0x300000000
#define HEAP_LENGTH (uint64_t)(32 * 1024)

/**
 * Panics
 *
 * Prints the line number where the panic occurred and then causes
 * the BPF VM to immediately halt execution. No accounts' data are updated
 */
void sol_panic_(const char *, uint64_t, uint64_t, uint64_t);
#define sol_panic() sol_panic_(__FILE__, sizeof(__FILE__), __LINE__, 0)

/**
 * Asserts
 */
#define sol_assert(expr)  \
if (!(expr)) {          \
  sol_panic(); \
}

/**
 * Structure that the program's entrypoint input data is deserialized into.
 */
typedef struct {
  SolAccountInfo* ka; /** Pointer to an array of SolAccountInfo, must already
                          point to an array of SolAccountInfos */
  uint64_t ka_num; /** Number of SolAccountInfo entries in `ka` */
  const uint8_t *data; /** pointer to the instruction data */
  uint64_t data_len; /** Length in bytes of the instruction data */
  const SolPubkey *program_id; /** program_id of the currently executing program */
} SolParameters;

/**
 * Maximum number of bytes a program may add to an account during a single realloc
 */
#define MAX_PERMITTED_DATA_INCREASE (1024 * 10)

/**
 * De-serializes the input parameters into usable types
 *
 * Use this function to deserialize the buffer passed to the program entrypoint
 * into usable types.  This function does not perform copy deserialization,
 * instead it populates the pointers and lengths in SolAccountInfo and data so
 * that any modification to lamports or account data take place on the original
 * buffer.  Doing so also eliminates the need to serialize back into the buffer
 * at the end of the program.
 *
 * @param input Source buffer containing serialized input parameters
 * @param params Pointer to a SolParameters structure
 * @return Boolean true if successful.
 */
static bool sol_deserialize(
  const uint8_t *input,
  SolParameters *params,
  uint64_t ka_num
) {
  if (NULL == input || NULL == params) {
    return false;
  }
  params->ka_num = *(uint64_t *) input;
  input += sizeof(uint64_t);

  for (int i = 0; i < params->ka_num; i++) {
    uint8_t dup_info = input[0];
    input += sizeof(uint8_t);

    if (i >= ka_num) {
      if (dup_info == UINT8_MAX) {
        input += sizeof(uint8_t);
        input += sizeof(uint8_t);
        input += sizeof(uint8_t);
        input += 4; // padding
        input += sizeof(SolPubkey);
        input += sizeof(SolPubkey);
        input += sizeof(uint64_t);
        uint64_t data_len = *(uint64_t *) input;
        input += sizeof(uint64_t);
        input += data_len;
        input += MAX_PERMITTED_DATA_INCREASE;
        input = (uint8_t*)(((uint64_t)input + 8 - 1) & ~(8 - 1)); // padding
        input += sizeof(uint64_t);
      } else {
        input += 7; // padding
      }
      continue;
    }
    if (dup_info == UINT8_MAX) {
      // is signer?
      params->ka[i].is_signer = *(uint8_t *) input != 0;
      input += sizeof(uint8_t);

      // is writable?
      params->ka[i].is_writable = *(uint8_t *) input != 0;
      input += sizeof(uint8_t);

      // executable?
      params->ka[i].executable = *(uint8_t *) input;
      input += sizeof(uint8_t);

      input += 4; // padding

      // key
      params->ka[i].key = (SolPubkey *) input;
      input += sizeof(SolPubkey);

      // owner
      params->ka[i].owner = (SolPubkey *) input;
      input += sizeof(SolPubkey);

      // lamports
      params->ka[i].lamports = (uint64_t *) input;
      input += sizeof(uint64_t);

      // account data
      params->ka[i].data_len = *(uint64_t *) input;
      input += sizeof(uint64_t);
      params->ka[i].data = (uint8_t *) input;
      input += params->ka[i].data_len;
      input += MAX_PERMITTED_DATA_INCREASE;
      input = (uint8_t*)(((uint64_t)input + 8 - 1) & ~(8 - 1)); // padding

      // rent epoch
      params->ka[i].rent_epoch = *(uint64_t *) input;
      input += sizeof(uint64_t);
    } else {
      params->ka[i].is_signer = params->ka[dup_info].is_signer;
      params->ka[i].is_writable = params->ka[dup_info].is_writable;
      params->ka[i].executable = params->ka[dup_info].executable;
      params->ka[i].key = params->ka[dup_info].key;
      params->ka[i].owner = params->ka[dup_info].owner;
      params->ka[i].lamports = params->ka[dup_info].lamports;
      params->ka[i].data_len = params->ka[dup_info].data_len;
      params->ka[i].data = params->ka[dup_info].data;
      params->ka[i].rent_epoch = params->ka[dup_info].rent_epoch;
      input += 7; // padding
    }
  }

  params->data_len = *(uint64_t *) input;
  input += sizeof(uint64_t);
  params->data = input;
  input += params->data_len;

  params->program_id = (SolPubkey *) input;
  input += sizeof(SolPubkey);

  return true;
}

/**
 * Byte array pointer and string
 */
typedef struct {
  const uint8_t *addr; /** bytes */
  uint64_t len; /** number of bytes*/
} SolBytes;

/**
 * Length of a sha256 hash result
 */
#define SHA256_RESULT_LENGTH 32

/**
 * Sha256
 *
 * @param bytes Array of byte arrays
 * @param bytes_len Number of byte arrays
 * @param result 32 byte array to hold the result
 */
uint64_t sol_sha256(
    const SolBytes *bytes,
    int bytes_len,
    uint8_t *result
);

/**
 * Length of a Keccak hash result
 */
#define KECCAK_RESULT_LENGTH 32

/**
 * Keccak
 *
 * @param bytes Array of byte arrays
 * @param bytes_len Number of byte arrays
 * @param result 32 byte array to hold the result
 */
uint64_t sol_keccak256(
    const SolBytes *bytes,
    int bytes_len,
    uint8_t *result
);

/** The ed25519 public key is not a valid public key */
#define ED25519_SIG_CHECK_INVALID_PUBLIC_KEY 1
/** The ed25519 signature is not a valid signature */
#define ED25519_SIG_CHCKEC_INVALID_SIGNATURE 2
/** THe ed25519 signature could not be verified */
#define ED25519_SIG_CHECK_VERIFIY_FAILED 3

/** ed25519 signature length */
#define ED25519_SIGNATURE_LENGTH 64

/** ed25519 public key length */
#define ED25519_PUBLIC_KEY_LENGTH 32

/**
 * Check ed25519 signature using public key
 *
 * @param message Message to be verified
 * @param message_len Length of message
 * @param signature Signature to be verified
 * @param publickey Public key to use for verification
 */
uint64_t sol_ed25519_sig_check(
    uint8_t *message,
    uint64_t message_len,
    uint8_t signature[ED25519_SIGNATURE_LENGTH],
    uint8_t publickey[ED25519_PUBLIC_KEY_LENGTH]
);

/**
 * Length of a Blake3 hash result
 */
#define BLAKE3_RESULT_LENGTH 32

/**
 * Blake3
 *
 * @param bytes Array of byte arrays
 * @param bytes_len Number of byte arrays
 * @param result 32 byte array to hold the result
 */
uint64_t sol_blake3(
    const SolBytes *bytes,
    int bytes_len,
    const uint8_t *result
);

/**
 * Account Meta
 */
typedef struct {
  SolPubkey *pubkey; /** An account's public key */
  bool is_writable; /** True if the `pubkey` can be loaded as a read-write account */
  bool is_signer; /** True if an Instruction requires a Transaction signature matching `pubkey` */
} SolAccountMeta;

/**
 * Instruction
 */
typedef struct {
  SolPubkey *program_id; /** Pubkey of the instruction processor that executes this instruction */
  SolAccountMeta *accounts; /** Metadata for what accounts should be passed to the instruction processor */
  uint64_t account_len; /** Number of SolAccountMetas */
  uint8_t *data; /** Opaque data passed to the instruction processor */
  uint64_t data_len; /** Length of the data in bytes */
} SolInstruction;

/**
 * Seed used to create a program address or passed to sol_invoke_signed
 */
typedef struct {
  const uint8_t *addr; /** Seed bytes */
  uint64_t len; /** Length of the seed bytes */
} SolSignerSeed;

/**
 * Seeds used by a signer to create a program address or passed to
 * sol_invoke_signed
 */
typedef struct {
  const SolSignerSeed *addr; /** An arry of a signer's seeds */
  uint64_t len; /** Number of seeds */
} SolSignerSeeds;

/**
 * Create a program address
 *
 * @param seeds Seed bytes used to sign program accounts
 * @param seeds_len Length of the seeds array
 * @param program_id Program id of the signer
 * @param program_address Program address created, filled on return
 */
uint64_t sol_create_program_address(
    const SolSignerSeed *seeds,
    int seeds_len,
    const SolPubkey *program_id,
    SolPubkey *program_address
);

/**
 * Try to find a program address and return corresponding bump seed
 *
 * @param seeds Seed bytes used to sign program accounts
 * @param seeds_len Length of the seeds array
 * @param program_id Program id of the signer
 * @param program_address Program address created, filled on return
 * @param bump_seed Bump seed required to create a valid program address
 */
uint64_t sol_try_find_program_address(
    const SolSignerSeed *seeds,
    int seeds_len,
    const SolPubkey *program_id,
    SolPubkey *program_address,
    uint8_t *bump_seed
);

/**
 * Cross-program invocation
 *  * @{
 */

/**
 * Internal cross-program invocation function
 */
uint64_t sol_invoke_signed_c(
  const SolInstruction *instruction,
  const SolAccountInfo *account_infos,
  int account_infos_len,
  const SolSignerSeeds *signers_seeds,
  int signers_seeds_len
);

/**
 * Invoke another program and sign for some of the keys
 *
 * @param instruction Instruction to process
 * @param account_infos Accounts used by instruction
 * @param account_infos_len Length of account_infos array
 * @param seeds Seed bytes used to sign program accounts
 * @param seeds_len Length of the seeds array
 */
static uint64_t sol_invoke_signed(
    const SolInstruction *instruction,
    const SolAccountInfo *account_infos,
    int account_infos_len,
    const SolSignerSeeds *signers_seeds,
    int signers_seeds_len
) {
  return sol_invoke_signed_c(
    instruction,
    account_infos,
    account_infos_len,
    signers_seeds,
    signers_seeds_len
  );
}
/**
 * Invoke another program
 *
 * @param instruction Instruction to process
 * @param account_infos Accounts used by instruction
 * @param account_infos_len Length of account_infos array
*/
static uint64_t sol_invoke(
    const SolInstruction *instruction,
    const SolAccountInfo *account_infos,
    int account_infos_len
) {
  const SolSignerSeeds signers_seeds[] = {{}};
  return sol_invoke_signed(
    instruction,
    account_infos,
    account_infos_len,
    signers_seeds,
    0
  );
}

/**@}*/

/**
 * Debugging utilities
 * @{
 */

/**
 * Prints the hexadecimal representation of a public key
 *
 * @param key The public key to print
 */
void sol_log_pubkey(
    const SolPubkey *pubkey
);

/**
 * Prints the hexadecimal representation of an array
 *
 * @param array The array to print
 */
static void sol_log_array(const uint8_t *array, int len) {
  for (int j = 0; j < len; j++) {
    sol_log_64(0, 0, 0, j, array[j]);
  }
}

/**
 * Prints the program's input parameters
 *
 * @param params Pointer to a SolParameters structure
 */
static void sol_log_params(const SolParameters *params) {
  sol_log("- Program identifier:");
  sol_log_pubkey(params->program_id);

  sol_log("- Number of KeyedAccounts");
  sol_log_64(0, 0, 0, 0, params->ka_num);
  for (int i = 0; i < params->ka_num; i++) {
    sol_log("  - Is signer");
    sol_log_64(0, 0, 0, 0, params->ka[i].is_signer);
    sol_log("  - Is writable");
    sol_log_64(0, 0, 0, 0, params->ka[i].is_writable);
    sol_log("  - Key");
    sol_log_pubkey(params->ka[i].key);
    sol_log("  - Lamports");
    sol_log_64(0, 0, 0, 0, *params->ka[i].lamports);
    sol_log("  - data");
    sol_log_array(params->ka[i].data, params->ka[i].data_len);
    sol_log("  - Owner");
    sol_log_pubkey(params->ka[i].owner);
    sol_log("  - Executable");
    sol_log_64(0, 0, 0, 0, params->ka[i].executable);
    sol_log("  - Rent Epoch");
    sol_log_64(0, 0, 0, 0, params->ka[i].rent_epoch);
  }
  sol_log("- Instruction data\0");
  sol_log_array(params->data, params->data_len);
}

/**@}*/

/**
 * Program instruction entrypoint
 *
 * @param input Buffer of serialized input parameters.  Use sol_deserialize() to decode
 * @return 0 if the instruction executed successfully
 */
uint64_t entrypoint(const uint8_t *input);

#ifdef SOL_TEST
/**
 * Stub functions when building tests
 */
#include <stdio.h>
void sol_log_(const char *s, uint64_t len) {
  printf("Program log: %s\n", s);
}
void sol_log_64(uint64_t arg1, uint64_t arg2, uint64_t arg3, uint64_t arg4, uint64_t arg5) {
  printf("Program log: %llu, %llu, %llu, %llu, %llu\n", arg1, arg2, arg3, arg4, arg5);
}
void sol_log_pubkey(const SolPubkey *pubkey) {
  printf("Program log: ");
  for (int i = 0; i < SIZE_PUBKEY; i++) {
    printf("%02 ", pubkey->x[i]);
  }
  printf("\n");
}
void sol_log_compute_units_() {
  printf("Program consumption: __ units remaining\n");
}
void sol_panic_(const char *file, uint64_t len, uint64_t line, uint64_t column) {
  printf("Panic in %s at %d:%d\n", file, line, column);
  abort();
}
#endif

#ifdef __cplusplus
}
#endif

/**@}*/
