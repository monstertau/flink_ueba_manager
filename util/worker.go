package util

import (
	"github.com/sirupsen/logrus"
	"sync"
)

type CommonWorker struct {
	ManageChan chan interface{}
	Logger     *logrus.Entry
	sync.Mutex
	*ChannelClosedWaiter
}

func NewCommonWorker(key string, value interface{}) *CommonWorker {
	return &CommonWorker{
		ManageChan:          make(chan interface{}, 5),
		Logger:              logrus.WithField(key, value),
		ChannelClosedWaiter: NewChannelClosedWaiter(),
	}
}

func (cm *CommonWorker) Close() {
	close(cm.ManageChan)
}
