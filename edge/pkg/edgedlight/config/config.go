package config

import (
	"sync"

	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
)

var Config Configure
var once sync.Once

type Configure struct {
	v1alpha1.EdgedLight
}

func InitConfigure(e *v1alpha1.EdgedLight) {
	once.Do(func() {
		Config = Configure{
			EdgedLight: *e,
		}
	})
}
