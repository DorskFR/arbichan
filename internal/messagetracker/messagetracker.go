package messagetracker

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type MessageTracker struct {
	lastMessageTime atomic.Value
	exchangeName    string
	staleThreshold  time.Duration
}

func NewMessageTracker(exchangeName string, staleThreshold time.Duration) *MessageTracker {
	mt := &MessageTracker{
		exchangeName:   exchangeName,
		staleThreshold: staleThreshold,
	}
	mt.lastMessageTime.Store(time.Now())
	return mt
}

func (mt *MessageTracker) RecordMessage() {
	mt.lastMessageTime.Store(time.Now())
}

func (mt *MessageTracker) CheckStaleConnection() {
	lastTime := mt.lastMessageTime.Load().(time.Time)
	if time.Since(lastTime) > mt.staleThreshold {
		log.Warn().
			Str("exchange", mt.exchangeName).
			Dur("timeSinceLastMessage", time.Since(lastTime)).
			Msg("Connection may be stale")
	} else {
		log.Debug().
			Str("exchange", mt.exchangeName).
			Dur("timeSinceLastMessage", time.Since(lastTime)).
			Msg("Connection does not appear stale")
	}
}
