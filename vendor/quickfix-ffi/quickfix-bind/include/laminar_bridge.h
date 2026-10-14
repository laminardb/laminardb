#ifndef _LAMINAR_QUICKFIX_BRIDGE_H_
#define _LAMINAR_QUICKFIX_BRIDGE_H_

// LaminarDB extensions to the quickfix C binding.
//
// Every function catches all C++ exceptions. Borrowed pointers (`ptr` outputs and field-map
// handles) stay valid only while the owning message is alive and unmodified.

#include "quickfix_bind.h"

#ifdef __cplusplus
extern "C" {
namespace FIX {
#endif

typedef struct FieldMap LmFixFieldMap_t;

typedef void (*LmFixBytesSink)(void *ctx, const char *ptr, size_t len);

const LmFixFieldMap_t *LmFix_messageBody(const FixMessage_t *msg);
const LmFixFieldMap_t *LmFix_messageHeader(const FixMessage_t *msg);
const LmFixFieldMap_t *LmFix_messageTrailer(const FixMessage_t *msg);

// 1 when present, 0 when absent, negative on error. `ptr`/`len` include embedded NUL bytes.
int8_t LmFix_fieldBytes(const LmFixFieldMap_t *map, int32_t tag, const char **ptr, size_t *len);

// Number of repeating-group entries, 0 when absent, negative on error.
int64_t LmFix_groupCount(const LmFixFieldMap_t *map, int32_t tag);

// One-based group entry, or NULL when out of range or on error.
const LmFixFieldMap_t *LmFix_groupEntry(const LmFixFieldMap_t *map, int32_t tag, int64_t index);

// Sets a body (section 0), header (1), or trailer (2) field from `len` bytes, which may contain
// NUL bytes. 0 on success.
int8_t LmFix_messageSetFieldBytes(FixMessage_t *msg, int32_t section, int32_t tag, const char *ptr, size_t len);

// Engine serialization (`Message::toString`) with its exact length. 0 on success.
int8_t LmFix_messageSerialize(const FixMessage_t *msg, void *ctx, LmFixBytesSink sink);

// Engine serialization of a copy without the given header tags. 0 on success.
int8_t LmFix_messageSerializeWithoutHeaderTags(const FixMessage_t *msg, const int32_t *tags, size_t count,
                                               void *ctx, LmFixBytesSink sink);

// Length-aware parse with a session dictionary for header and trailer and an application
// dictionary for body and group structure (the same dictionary below FIXT). Checks body length
// and checksum. NULL on error.
FixMessage_t *LmFix_messageParse(const char *ptr, size_t len, const FixDataDictionary_t *sessionDictionary,
                                 const FixDataDictionary_t *applicationDictionary);

// Dictionary introspection for validating column mappings before activation.

// The dictionary's version, `type.major.minor` of its root element (the service pack is not part
// of it). 0 on success.
int8_t LmFix_dictionaryVersion(const FixDataDictionary_t *dictionary, void *ctx, LmFixBytesSink sink);

// FIX::TYPE::Type of a defined field, or -1 when the dictionary does not define it.
int32_t LmFix_dictionaryFieldType(const FixDataDictionary_t *dictionary, int32_t tag);

// 1 when the dictionary defines `msgType`, 0 when not, negative on error.
int8_t LmFix_dictionaryHasMessage(const FixDataDictionary_t *dictionary, const char *msgType);

// Where `tag` may appear in `msgType`: 1 header, 2 body, 3 trailer, 0 nowhere; negative on error.
int8_t LmFix_dictionaryFieldPlacement(const FixDataDictionary_t *dictionary, const char *msgType, int32_t tag);

// Member dictionary of repeating group `countTag` in `msgType` (pass a group dictionary for a
// nested group), or NULL when it is not such a group. Borrowed from `dictionary`.
const FixDataDictionary_t *LmFix_dictionaryGroup(const FixDataDictionary_t *dictionary, const char *msgType,
                                                 int32_t countTag);

// 1 when `tag` is a member of a group dictionary, 0 when not, negative on error.
int8_t LmFix_dictionaryGroupHasField(const FixDataDictionary_t *groupDictionary, int32_t tag);

// Persisted engine store identity and in-memory sequence state of a live session. 0 on success.
int8_t LmFix_sessionState(const FixSessionID_t *sessionId, int64_t *creationUnixSeconds, uint64_t *nextSender,
                          uint64_t *nextTarget);

// `Session::setNextSenderMsgSeqNum` on a live session. 0 on success.
int8_t LmFix_sessionSetNextSender(const FixSessionID_t *sessionId, uint64_t nextSender);

// Persistence of an externally owned session store. Each callback returns 0 once the change is
// persisted; any other value makes the engine see an IOException and leaves the store unchanged.
typedef struct LmFixStoreCallbacks {
  int8_t (*setNextSender)(void *ctx, uint64_t nextSender);
  int8_t (*setNextTarget)(void *ctx, uint64_t nextTarget);
  // Resets both sequence numbers to 1 and reports the new store creation time, which may differ
  // from `nowUnixMicros`.
  int8_t (*reset)(void *ctx, int64_t nowUnixMicros, int64_t *creationUnixMicros);
  int8_t (*load)(void *ctx, int64_t *creationUnixMicros, uint64_t *nextSender, uint64_t *nextTarget);
} LmFixStoreCallbacks;

// Store factory for one session whose state starts at the given values and changes only through
// `callbacks`. Stores never keep outbound messages, so sessions must use PersistMessages=N.
// `ctx` must outlive the factory. NULL on error.
FixMessageStoreFactory_t *LmFix_callbackStoreFactoryNew(const LmFixStoreCallbacks *callbacks, void *ctx,
                                                        int64_t creationUnixMicros, uint64_t nextSender,
                                                        uint64_t nextTarget);

// `Initiator::stop(force)`. 0 on success.
int8_t LmFix_initiatorStop(FixInitiator_t *initiator, int8_t force);

// `Acceptor::stop(force)`. 0 on success.
int8_t LmFix_acceptorStop(FixAcceptor_t *acceptor, int8_t force);

// Copies the calling thread's last bridge error into `buffer` (NUL-terminated when capacity > 0)
// and returns its full length.
size_t LmFix_lastError(char *buffer, size_t capacity);

#ifdef __cplusplus
}
}
#endif

#endif // _LAMINAR_QUICKFIX_BRIDGE_H_
