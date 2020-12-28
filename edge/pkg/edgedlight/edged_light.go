package edgedlight

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-chassis/go-chassis/third_party/forked/k8s.io/apimachinery/pkg/util/sets"
	"github.com/kubeedge/beehive/pkg/common/util"
	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/common/constants"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/edged/apis"
	fakekube "github.com/kubeedge/kubeedge/edge/pkg/edged/fake"
	"github.com/kubeedge/kubeedge/edge/pkg/edged/podmanager"
	"github.com/kubeedge/kubeedge/edge/pkg/edged/status"
	edgedlightconfig "github.com/kubeedge/kubeedge/edge/pkg/edgedlight/config"
	"github.com/kubeedge/kubeedge/edge/pkg/edgedlight/podman"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/client"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
	"github.com/kubeedge/kubeedge/pkg/version"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/pleg"
	kubestatus "k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/kubelet/util/queue"
)

const (
	backOffPeriod = 10 * time.Second
	// MaxContainerBackOff is the max backoff period, exported for the e2e test
	MaxContainerBackOff = 300 * time.Second
	//EdgeController gives controller name
	EdgeController                   = "edgecontroller"
	enqueueDuration                  = 10 * time.Second
	workerResyncIntervalJitterFactor = 0.5
	syncMsgRespTimeout   = 1 * time.Minute
)

// podReady holds the initPodReady flag and its lock
type podReady struct {
	// initPodReady is flag to check Pod ready status
	initPodReady bool
	// podReadyLock is used to guard initPodReady flag
	podReadyLock sync.RWMutex
}

type edgedLight struct {
	enable              bool
	hostname            string
	namespace           string
	nodeName            string
	uid                       types.UID
	nodeStatusUpdateFrequency time.Duration
	podManager          podmanager.Manager
	concurrentConsumers int
	metaClient          client.CoreInterface
	kubeClient          clientset.Interface
	podmanClient        podman.Interface
	statusManager       kubestatus.Manager
	podAdditionQueue    *workqueue.Type
	podAdditionBackoff  *flowcontrol.Backoff
	podDeletionQueue    *workqueue.Type
	podDeletionBackoff  *flowcontrol.Backoff
	workQueue           queue.WorkQueue
	registrationCompleted     bool
	version            string
	// podReady is structure with initPodReady flag and its lock
	podReady
	podLastSyncTime sync.Map

	// edge node IP
	nodeIP net.IP
}

// Register register edged
func Register(e *v1alpha1.EdgedLight) {
	edgedlightconfig.InitConfigure(e)
	edgedLight, err := newEdgedLight(e.Enable)
	if err != nil {
		klog.Errorf("init new edged error, %v", err)
		os.Exit(1)
		return
	}
	core.Register(edgedLight)
}

func newEdgedLight(enable bool) (*edgedLight, error) {
	backoff := flowcontrol.NewBackOff(backOffPeriod, MaxContainerBackOff)
	podManager := podmanager.NewPodManager()
	metaClient := client.New()
	podmanClient := podman.NewClient()
	ed := &edgedLight{
		enable:              enable,
		nodeName:            edgedlightconfig.Config.HostnameOverride,
		namespace:           edgedlightconfig.Config.RegisterNodeNamespace,
		concurrentConsumers: edgedlightconfig.Config.ConcurrentConsumers,
		nodeStatusUpdateFrequency: time.Duration(edgedlightconfig.Config.NodeStatusUpdateFrequency) * time.Second,
		podManager:          podManager,
		podAdditionQueue:    workqueue.New(),
		podAdditionBackoff:  backoff,
		podDeletionQueue:    workqueue.New(),
		podDeletionBackoff:  backoff,
		workQueue:           queue.NewBasicWorkQueue(clock.RealClock{}),
		metaClient:          metaClient,
		kubeClient:          fakekube.NewSimpleClientset(metaClient),
		podmanClient:        podmanClient,
		version:                   fmt.Sprintf("%s-kubeedge-%s", constants.CurrentSupportK8sVersion, version.Get()),
		nodeIP:                    net.ParseIP(edgedlightconfig.Config.NodeIP),
		uid:                       types.UID("38796d14-1df3-11e8-8e5a-286ed488f209"),
	}
	ed.statusManager = status.NewManager(ed.kubeClient, ed.podManager, ed, ed.metaClient)
	return ed, nil
}

func (e *edgedLight) Start() {
	go utilwait.Until(e.syncNodeStatus, e.nodeStatusUpdateFrequency, utilwait.NeverStop)
	e.statusManager.Start()
	e.podAddWorkerRun(e.concurrentConsumers)
	e.podRemoveWorkerRun(e.concurrentConsumers)
	e.syncPod()
}

func (e *edgedLight) syncPod() {
	time.Sleep(10 * time.Second)

	//when starting, send msg to metamanager once to get existing pods
	info := model.NewMessage("").BuildRouter(e.Name(), e.Group(), e.namespace+"/"+model.ResourceTypePod,
		model.QueryOperation)
	beehiveContext.Send(metamanager.MetaManagerModuleName, *info)
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("Sync pod stop")
			return
		default:
		}
		result, err := beehiveContext.Receive(e.Name())
		if err != nil {
			klog.Errorf("failed to get pod: %v", err)
			continue
		}

		_, resType, resID, err := util.ParseResourceEdge(result.GetResource(), result.GetOperation())
		if err != nil {
			klog.Errorf("failed to parse the Resource: %v", err)
			continue
		}
		op := result.GetOperation()

		var content []byte

		switch result.Content.(type) {
		case []byte:
			content = result.GetContent().([]byte)
		default:
			content, err = json.Marshal(result.Content)
			if err != nil {
				klog.Errorf("marshal message content failed: %v", err)
				continue
			}
		}
		klog.Infof("result content is %s", result.Content)
		switch resType {
		case model.ResourceTypePod:
			if op == model.ResponseOperation && resID == "" && result.GetSource() == metamanager.MetaManagerModuleName {
				err := e.handlePodListFromMetaManager(content)
				if err != nil {
					klog.Errorf("handle podList failed: %v", err)
					continue
				}
				e.setInitPodReady(true)
			} else if op == model.ResponseOperation && resID == "" && result.GetSource() == EdgeController {
				err := e.handlePodListFromEdgeController(content)
				if err != nil {
					klog.Errorf("handle controllerPodList failed: %v", err)
					continue
				}
				e.setInitPodReady(true)
			} else {
				err := e.handlePod(op, content)
				if err != nil {
					klog.Errorf("handle pod failed: %v", err)
					continue
				}
			}
		case model.ResourceTypeConfigmap:
			if op != model.ResponseOperation {
				err := e.handleConfigMap(op, content)
				if err != nil {
					klog.Errorf("handle configMap failed: %v", err)
				}
			} else {
				klog.Infof("skip to handle configMap with type response")
				continue
			}
		case model.ResourceTypeSecret:
			if op != model.ResponseOperation {
				err := e.handleSecret(op, content)
				if err != nil {
					klog.Errorf("handle secret failed: %v", err)
				}
			} else {
				klog.Infof("skip to handle secret with type response")
				continue
			}
		case constants.CSIResourceTypeVolume:
			klog.Infof("volume operation type: %s", op)
			res, err := e.handleVolume(op, content)
			if err != nil {
				klog.Errorf("handle volume failed: %v", err)
			} else {
				resp := result.NewRespByMessage(&result, res)
				beehiveContext.SendResp(*resp)
			}
		default:
			klog.Errorf("resType is not pod or configmap or secret or volume: esType is %s", resType)
			continue
		}
	}
}

func (e *edgedLight) syncLoopIteration(plegCh <-chan *pleg.PodLifecycleEvent, housekeepingCh <-chan time.Time, syncWorkQueueCh <-chan time.Time) {
	for {
		select {
		case <-housekeepingCh:
			err := e.HandlePodCleanups()
			if err != nil {
				klog.Errorf("Handle Pod Cleanup Failed: %v", err)
			}
		case <-syncWorkQueueCh:
			podsToSync := e.getPodsToSync()
			if len(podsToSync) == 0 {
				break
			}
			for _, pod := range podsToSync {
				if !e.podIsTerminated(pod) {
					key := types.NamespacedName{
						Namespace: pod.Namespace,
						Name:      pod.Name,
					}
					e.podAdditionQueue.Add(key.String())
				}
			}
		}
	}
}

// Get pods which should be resynchronized. Currently, the following pod should be resynchronized:
//   * pod whose work is ready.
//   * internal modules that request sync of a pod.
func (e *edgedLight) getPodsToSync() []*v1.Pod {
	allPods := e.podManager.GetPods()
	podUIDs := e.workQueue.GetWork()
	podUIDSet := sets.NewString()
	for _, podUID := range podUIDs {
		podUIDSet.Insert(string(podUID))
	}
	var podsToSync []*v1.Pod
	for _, pod := range allPods {
		if podUIDSet.Has(string(pod.UID)) {
			// The work of the pod is ready
			podsToSync = append(podsToSync, pod)
		}
	}
	return podsToSync
}

// podIsTerminated returns true if pod is in the terminated state ("Failed" or "Succeeded").
func (e *edgedLight) podIsTerminated(pod *v1.Pod) bool {
	// Check the cached pod status which was set after the last sync.
	status, ok := e.statusManager.GetPodStatus(pod.UID)
	if !ok {
		// If there is no cached status, use the status from the
		// apiserver. This is useful if kubelet has recently been
		// restarted.
		status = pod.Status
	}

	return status.Phase == v1.PodFailed || status.Phase == v1.PodSucceeded || (pod.DeletionTimestamp != nil && notRunning(status.ContainerStatuses))
}

// notRunning returns true if every status is terminated or waiting, or the status list
// is empty.
func notRunning(statuses []v1.ContainerStatus) bool {
	for _, status := range statuses {
		if status.State.Terminated == nil && status.State.Waiting == nil {
			return false
		}
	}
	return true
}

func (e *edgedLight) HandlePodCleanups() error {
	if !e.isInitPodReady() {
		return nil
	}
	pods := e.podManager.GetPods()
	e.removeOrphanedPodStatuses(pods)

	return nil
}

// removeOrphanedPodStatuses removes obsolete entries in podStatus where
// the pod is no longer considered bound to this node.
func (e *edgedLight) removeOrphanedPodStatuses(pods []*v1.Pod) {
	podUIDs := make(map[types.UID]bool, len(pods))
	for _, pod := range pods {
		podUIDs[pod.UID] = true
	}

	e.statusManager.RemoveOrphanedStatuses(podUIDs)
}

// NewNamespacedNameFromString parses the provided string and returns a NamespacedName
func NewNamespacedNameFromString(s string) types.NamespacedName {
	Separator := '/'
	nn := types.NamespacedName{}
	result := strings.Split(s, string(Separator))
	if len(result) == 2 {
		nn.Namespace = result[0]
		nn.Name = result[1]
	}
	return nn
}

func (e *edgedLight) podAddWorkerRun(consumers int) {
	for i := 0; i < consumers; i++ {
		klog.Infof("start pod addition queue work %d", i)
		go func(i int) {
			for {
				item, quit := e.podAdditionQueue.Get()
				if quit {
					klog.Errorf("consumer: [%d], worker addition queue is shutting down!", i)
					return
				}
				namespacedName := NewNamespacedNameFromString(item.(string))
				podName := namespacedName.Name
				klog.Infof("worker [%d] get pod addition item [%s]", i, podName)
				backOffKey := fmt.Sprintf("pod_addition_worker_%s", podName)
				if e.podAdditionBackoff.IsInBackOffSinceUpdate(backOffKey, e.podAdditionBackoff.Clock.Now()) {
					klog.Errorf("consume pod addition backoff: Back-off consume pod [%s] addition  error, backoff: [%v]", podName, e.podAdditionBackoff.Get(backOffKey))
					go func() {
						klog.Infof("worker [%d] backoff pod addition item [%s] failed, re-add to queue", i, podName)
						time.Sleep(e.podAdditionBackoff.Get(backOffKey))
						e.podAdditionQueue.Add(item)
					}()
					e.podAdditionQueue.Done(item)
					continue
				}
				err := e.consumePodAddition(&namespacedName)
				if err != nil {
					if err == apis.ErrPodNotFound {
						klog.Infof("worker [%d] handle pod addition item [%s] failed with not found error.", i, podName)
						e.podAdditionBackoff.Reset(backOffKey)
					} else {
						go func() {
							klog.Errorf("worker [%d] handle pod addition item [%s] failed: %v, re-add to queue", i, podName, err)
							e.podAdditionBackoff.Next(backOffKey, e.podAdditionBackoff.Clock.Now())
							time.Sleep(enqueueDuration)
							e.podAdditionQueue.Add(item)
						}()
					}
				} else {
					e.podAdditionBackoff.Reset(backOffKey)
				}
				e.podAdditionQueue.Done(item)
			}
		}(i)
	}
}

func (e *edgedLight) podRemoveWorkerRun(consumers int) {
	for i := 0; i < consumers; i++ {
		go func(i int) {
			for {
				item, quit := e.podDeletionQueue.Get()
				if quit {
					klog.Errorf("consumer: [%d], worker addition queue is shutting down!", i)
					return
				}
				namespacedName := NewNamespacedNameFromString(item.(string))
				podName := namespacedName.Name
				klog.Infof("consumer: [%d], worker get removed pod [%s]\n", i, podName)
				err := e.consumePodDeletion(&namespacedName)
				if err != nil {
					if err == apis.ErrContainerNotFound {
						klog.Infof("pod [%s] is not exist, with container not found error", podName)
					} else if err == apis.ErrPodNotFound {
						klog.Infof("pod [%s] is not found", podName)
					} else {
						go func(item interface{}) {
							klog.Errorf("worker remove pod [%s] failed: %v", podName, err)
							time.Sleep(2 * time.Second)
							e.podDeletionQueue.Add(item)
						}(item)
					}
				}
				e.podDeletionQueue.Done(item)
			}
		}(i)
	}
}

func (e *edgedLight) consumePodAddition(namespacedName *types.NamespacedName) error {
	podName := namespacedName.Name
	klog.Infof("start to consume added pod [%s]", podName)
	pod, ok := e.podManager.GetPodByName(namespacedName.Namespace, podName)
	if !ok || pod.DeletionTimestamp != nil {
		return apis.ErrPodNotFound
	}
	err := e.podmanClient.CreatePod(pod)
	if err != nil {
		klog.Infof("consume added pod [%s] failed, %v", podName, err)
	}
	e.workQueue.Enqueue(pod.UID, utilwait.Jitter(time.Minute, workerResyncIntervalJitterFactor))
	klog.Infof("consume added pod [%s] successfully\n", podName)
	return nil
}

func (e *edgedLight) consumePodDeletion(namespacedName *types.NamespacedName) error {
	podName := namespacedName.Name
	klog.Infof("start to consume removed pod [%s]", podName)
	pod, ok := e.podManager.GetPodByName(namespacedName.Namespace, podName)
	if !ok {
		return apis.ErrPodNotFound
	}

	err := e.podmanClient.DeletePod(pod.Name)
	if err != nil {
		if err == apis.ErrContainerNotFound {
			return err
		}
		return fmt.Errorf("consume removed pod [%s] failed, %v", podName, err)
	}
	klog.Infof("consume removed pod [%s] successfully\n", podName)
	return nil
}

func (e *edgedLight) handlePod(op string, content []byte) (err error) {
	var pod v1.Pod
	err = json.Unmarshal(content, &pod)
	if err != nil {
		return err
	}

	switch op {
	case model.InsertOperation:
		e.addPod(&pod)
	case model.UpdateOperation:
		e.updatePod(&pod)
	case model.DeleteOperation:
		if delPod, ok := e.podManager.GetPodByName(pod.Namespace, pod.Name); ok {
			e.deletePod(delPod)
		}
	}
	return nil
}

func (e *edgedLight) handlePodListFromMetaManager(content []byte) (err error) {
	var lists []string
	err = json.Unmarshal([]byte(content), &lists)
	if err != nil {
		return err
	}

	for _, list := range lists {
		var pod v1.Pod
		err = json.Unmarshal([]byte(list), &pod)
		if err != nil {
			return err
		}
		e.addPod(&pod)
		if err = e.updatePodStatus(&pod); err != nil {
			klog.Errorf("handlePodListFromMetaManager: update pod %s status error", pod.Name)
			return err
		}
	}

	return nil
}

func (e *edgedLight) handlePodListFromEdgeController(content []byte) (err error) {
	var lists []v1.Pod
	if err := json.Unmarshal(content, &lists); err != nil {
		return err
	}

	for _, list := range lists {
		e.addPod(&list)
	}

	return nil
}

func (e *edgedLight) addPod(obj interface{}) {
	pod := obj.(*v1.Pod)
	klog.Infof("start sync addition for pod [%s]", pod.Name)

	key := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}
	e.podManager.AddPod(pod)
	e.podAdditionQueue.Add(key.String())
	klog.Infof("success sync addition for pod [%s]", pod.Name)
}

func (e *edgedLight) updatePod(obj interface{}) {
	newPod := obj.(*v1.Pod)
	klog.Infof("start update pod [%s]", newPod.Name)
	key := types.NamespacedName{
		Namespace: newPod.Namespace,
		Name:      newPod.Name,
	}
	e.podManager.UpdatePod(newPod)
	if newPod.DeletionTimestamp == nil {
		e.podAdditionQueue.Add(key.String())
	} else {
		e.podDeletionQueue.Add(key.String())
	}
	klog.Infof("success update pod is %+v\n", newPod)
}

func (e *edgedLight) deletePod(obj interface{}) {
	pod := obj.(*v1.Pod)
	klog.Infof("start remove pod [%s]", pod.Name)
	e.podManager.DeletePod(pod)
	e.statusManager.TerminatePod(pod)
	klog.Infof("success remove pod [%s]", pod.Name)
}

// isInitPodReady is used to safely return initPodReady flag
func (e *edgedLight) isInitPodReady() bool {
	e.podReadyLock.RLock()
	defer e.podReadyLock.RUnlock()
	return e.initPodReady
}

// setInitPodReady is used to safely set initPodReady flag
func (e *edgedLight) setInitPodReady(readyStatus bool) {
	e.podReadyLock.Lock()
	defer e.podReadyLock.Unlock()
	e.initPodReady = readyStatus
}

func (e *edgedLight) handleVolume(op string, content []byte) (interface{}, error) {
	return nil, nil
}

func (e *edgedLight) handleConfigMap(op string, content []byte) (err error) {
	return nil
}

func (e *edgedLight) handleSecret(op string, content []byte) (err error) {
	return nil
}

// Module interface 代替Edged
func (e *edgedLight) Name() string {
	return modules.EdgedModuleName
}

func (e *edgedLight) Group() string {
	return modules.EdgedGroup
}

//Enable indicates whether this module is enabled
func (e *edgedLight) Enable() bool {
	return e.enable
}
