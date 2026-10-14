#include "laminar_bridge.h"

#include <cstring>
#include <exception>
#include <string>

#include <quickfix/Acceptor.h>
#include <quickfix/DataDictionary.h>
#include <quickfix/FieldMap.h>
#include <quickfix/Initiator.h>
#include <quickfix/Message.h>
#include <quickfix/Session.h>
#include <quickfix/SessionID.h>

namespace {

thread_local std::string lastError;

void setError(const char *what) { lastError.assign(what ? what : "unknown C++ exception"); }

} // namespace

#define LMFIX_GUARD(_FAIL_, _BODY_)                                                                                    \
  try {                                                                                                                \
    _BODY_                                                                                                             \
  } catch (std::exception & ex) {                                                                                      \
    setError(ex.what());                                                                                               \
    return (_FAIL_);                                                                                                   \
  } catch (...) {                                                                                                      \
    setError(nullptr);                                                                                                 \
    return (_FAIL_);                                                                                                   \
  }

extern "C" {
namespace FIX {

const LmFixFieldMap_t *LmFix_messageBody(const Message *msg) {
  if (!msg) {
    return nullptr;
  }
  return static_cast<const FieldMap *>(msg);
}

const LmFixFieldMap_t *LmFix_messageHeader(const Message *msg) {
  if (!msg) {
    return nullptr;
  }
  return static_cast<const FieldMap *>(&msg->getHeader());
}

const LmFixFieldMap_t *LmFix_messageTrailer(const Message *msg) {
  if (!msg) {
    return nullptr;
  }
  return static_cast<const FieldMap *>(&msg->getTrailer());
}

int8_t LmFix_fieldBytes(const FieldMap *map, int32_t tag, const char **ptr, size_t *len) {
  if (!map || !ptr || !len) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    if (!map->isSetField(tag)) {
      return 0;
    }
    const std::string &value = map->getFieldRef(tag).getString();
    *ptr = value.data();
    *len = value.size();
    return 1;
  })
}

int64_t LmFix_groupCount(const FieldMap *map, int32_t tag) {
  if (!map) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, { return static_cast<int64_t>(map->groupCount(tag)); })
}

const LmFixFieldMap_t *LmFix_groupEntry(const FieldMap *map, int32_t tag, int64_t index) {
  if (!map || index < 1) {
    setError("invalid group entry request");
    return nullptr;
  }
  LMFIX_GUARD(nullptr, {
    if (static_cast<uint64_t>(index) > map->groupCount(tag)) {
      setError("group entry out of range");
      return nullptr;
    }
    return map->getGroupPtr(static_cast<int>(index), tag);
  })
}

int8_t LmFix_messageSetFieldBytes(Message *msg, int32_t section, int32_t tag, const char *ptr, size_t len) {
  if (!msg || (!ptr && len > 0)) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    FieldMap *map = nullptr;
    switch (section) {
    case 0:
      map = msg;
      break;
    case 1:
      map = &msg->getHeader();
      break;
    case 2:
      map = &msg->getTrailer();
      break;
    default:
      setError("unknown message section");
      return ERRNO_INVAL;
    }
    map->setField(FieldBase(tag, std::string(ptr ? ptr : "", len)));
    return 0;
  })
}

int8_t LmFix_messageSerialize(const Message *msg, void *ctx, LmFixBytesSink sink) {
  if (!msg || !sink) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    std::string value;
    msg->toString(value);
    sink(ctx, value.data(), value.size());
    return 0;
  })
}

int8_t LmFix_messageSerializeWithoutHeaderTags(const Message *msg, const int32_t *tags, size_t count, void *ctx,
                                               LmFixBytesSink sink) {
  if (!msg || !sink || (count > 0 && !tags)) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    Message copy(*msg);
    for (size_t i = 0; i < count; ++i) {
      copy.getHeader().removeField(tags[i]);
    }
    std::string value;
    copy.toString(value);
    sink(ctx, value.data(), value.size());
    return 0;
  })
}

Message *LmFix_messageParse(const char *ptr, size_t len, const DataDictionary *dictionary) {
  if (!ptr || !dictionary) {
    setError("null argument");
    return nullptr;
  }
  LMFIX_GUARD(nullptr, { return new Message(std::string(ptr, len), *dictionary, true); })
}

int32_t LmFix_dictionaryFieldType(const DataDictionary *dictionary, int32_t tag) {
  if (!dictionary) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    TYPE::Type type = TYPE::Unknown;
    if (!dictionary->isField(tag)) {
      return -1;
    }
    dictionary->getFieldType(tag, type);
    return static_cast<int32_t>(type);
  })
}

int8_t LmFix_dictionaryHasMessage(const DataDictionary *dictionary, const char *msgType) {
  if (!dictionary || !msgType) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, { return dictionary->isMsgType(msgType) ? 1 : 0; })
}

int8_t LmFix_dictionaryFieldPlacement(const DataDictionary *dictionary, const char *msgType, int32_t tag) {
  if (!dictionary || !msgType) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    if (dictionary->isHeaderField(tag)) {
      return 1;
    }
    if (dictionary->isMsgField(msgType, tag)) {
      return 2;
    }
    if (dictionary->isTrailerField(tag)) {
      return 3;
    }
    return 0;
  })
}

const DataDictionary *LmFix_dictionaryGroup(const DataDictionary *dictionary, const char *msgType, int32_t countTag) {
  if (!dictionary || !msgType) {
    setError("null argument");
    return nullptr;
  }
  LMFIX_GUARD(nullptr, {
    int delim = 0;
    const DataDictionary *group = nullptr;
    if (!dictionary->getGroup(msgType, countTag, delim, group)) {
      return nullptr;
    }
    return group;
  })
}

int8_t LmFix_dictionaryGroupHasField(const DataDictionary *groupDictionary, int32_t tag) {
  if (!groupDictionary) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, { return groupDictionary->isField(tag) ? 1 : 0; })
}

int8_t LmFix_sessionState(const SessionID *sessionId, int64_t *creationUnixSeconds, uint64_t *nextSender,
                          uint64_t *nextTarget) {
  if (!sessionId || !creationUnixSeconds || !nextSender || !nextTarget) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    Session *session = Session::lookupSession(*sessionId);
    if (!session) {
      setError("session not found");
      return ERROR_SESSION_NOT_FOUND;
    }
    *creationUnixSeconds = static_cast<int64_t>(session->getStore()->getCreationTime().getTimeT());
    *nextSender = static_cast<uint64_t>(session->getExpectedSenderNum());
    *nextTarget = static_cast<uint64_t>(session->getExpectedTargetNum());
    return 0;
  })
}

int8_t LmFix_sessionSetNextSender(const SessionID *sessionId, uint64_t nextSender) {
  if (!sessionId) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    Session *session = Session::lookupSession(*sessionId);
    if (!session) {
      setError("session not found");
      return ERROR_SESSION_NOT_FOUND;
    }
    session->setNextSenderMsgSeqNum(static_cast<SEQNUM>(nextSender));
    return 0;
  })
}

int8_t LmFix_initiatorStop(Initiator *initiator, int8_t force) {
  if (!initiator) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    initiator->stop(force != 0);
    return 0;
  })
}

int8_t LmFix_acceptorStop(Acceptor *acceptor, int8_t force) {
  if (!acceptor) {
    setError("null argument");
    return ERRNO_INVAL;
  }
  LMFIX_GUARD(ERRNO_EXCEPTION, {
    acceptor->stop(force != 0);
    return 0;
  })
}

size_t LmFix_lastError(char *buffer, size_t capacity) {
  if (buffer && capacity > 0) {
    size_t copied = lastError.size() < capacity - 1 ? lastError.size() : capacity - 1;
    std::memcpy(buffer, lastError.data(), copied);
    buffer[copied] = '\0';
  }
  return lastError.size();
}

} // namespace FIX
} // extern "C"
