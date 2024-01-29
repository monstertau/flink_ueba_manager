package util

import "sync"

type (
	SyncCtrlMsg struct {
		doneChan chan struct{}
	}
	// control message for closing a running resource
	CloseCtrMsg struct {
		*SyncCtrlMsg
	}
	// control message for stopping a running resource
	StopCtrMsg struct {
	}

	ChannelClosedWaiter struct {
		channelClosed bool
		cond          sync.Cond
	}
)

func NewSyncCtrlMsg() *SyncCtrlMsg {
	return &SyncCtrlMsg{
		doneChan: make(chan struct{}),
	}
}

func (ctrlMsg *SyncCtrlMsg) Done() {
	ctrlMsg.doneChan <- struct{}{}
}

func (ctrlMsg *SyncCtrlMsg) WaitForDone() {
	<-ctrlMsg.doneChan
}

func NewCloseCtrlMsg() *CloseCtrMsg {
	return &CloseCtrMsg{
		SyncCtrlMsg: NewSyncCtrlMsg(),
	}
}

func NewStopCtrlMsg() *StopCtrMsg {
	return &StopCtrMsg{}
}

func NewChannelClosedWaiter() *ChannelClosedWaiter {
	return &ChannelClosedWaiter{
		cond: sync.Cond{L: &sync.RWMutex{}},
	}
}

// Notify wakes all goroutines waiting on Wait previously
func (w *ChannelClosedWaiter) Notify() {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	w.channelClosed = true
	w.cond.Broadcast()
}

// Wait waits for the event that there no more messages to consume (the input channel is closed)
func (w *ChannelClosedWaiter) Wait() {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	for !w.channelClosed {
		w.cond.Wait()
	}
}
