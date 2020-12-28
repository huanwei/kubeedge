package edgedlight

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/common/constants"
	edgeapi "github.com/kubeedge/kubeedge/common/types"
	"github.com/kubeedge/kubeedge/edge/pkg/common/message"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/edgedlight/config"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub"
	"github.com/kubeedge/kubeedge/pkg/util"
)

//GPUInfoQueryTool sets information monitoring tool location for GPU
var GPUInfoQueryTool = "/var/IEF/nvidia/bin/nvidia-smi"
var initNode v1.Node
var reservationMemory = resource.MustParse(fmt.Sprintf("%dMi", 100))

func (e *edgedLight) initialNode() (*v1.Node, error) {
	var node = &v1.Node{}

	if runtime.GOOS == "windows" {
		return node, nil
	}

	nodeInfo, err := e.getNodeInfo()
	if err != nil {
		return nil, err
	}
	node.Status.NodeInfo = nodeInfo

	hostname, err := os.Hostname()
	if err != nil {
		klog.Errorf("couldn't determine hostname: %v", err)
		return nil, err
	}
	if len(e.nodeName) != 0 {
		hostname = e.nodeName
	}

	node.Labels = map[string]string{
		// Kubernetes built-in labels
		v1.LabelHostname:   hostname,
		v1.LabelOSStable:   runtime.GOOS,
		v1.LabelArchStable: runtime.GOARCH,

		// KubeEdge specific labels
		"node-role.kubernetes.io/edge":  "",
		"node-role.kubernetes.io/agent": "",
	}

	node.Status.Addresses = []v1.NodeAddress{
		{Type: v1.NodeInternalIP, Address: e.nodeIP.String()},
		{Type: v1.NodeHostName, Address: hostname},
	}

	node.Status.Capacity = make(v1.ResourceList)
	node.Status.Allocatable = make(v1.ResourceList)
	err = e.setMemInfo(node.Status.Capacity, node.Status.Allocatable)
	if err != nil {
		return nil, err
	}

	err = e.setCPUInfo(node.Status.Capacity, node.Status.Allocatable)
	if err != nil {
		return nil, err
	}

	node.Status.Capacity[v1.ResourcePods] = *resource.NewQuantity(110, resource.DecimalSI)
	node.Status.Allocatable[v1.ResourcePods] = *resource.NewQuantity(110, resource.DecimalSI)

	return node, nil
}

func (e *edgedLight) setInitNode(node *v1.Node) {
	initNode.Status = *node.Status.DeepCopy()
}

func (e *edgedLight) getNodeStatusRequest(node *v1.Node) (*edgeapi.NodeStatusRequest, error) {
	var nodeStatus = &edgeapi.NodeStatusRequest{}
	nodeStatus.UID = e.uid
	nodeStatus.Status = *node.Status.DeepCopy()
	nodeStatus.Status.Phase = e.getNodePhase()

	e.setNodeStatusDaemonEndpoints(nodeStatus)
	e.setNodeStatusConditions(nodeStatus)

	nodeStatus.Status.VolumesInUse = []v1.UniqueVolumeName{"edgedLight do not support volume status update!"}

	return nodeStatus, nil
}

func (e *edgedLight) setNodeStatusDaemonEndpoints(node *edgeapi.NodeStatusRequest) {
	node.Status.DaemonEndpoints = v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: constants.ServerPort,
		},
	}
}

func (e *edgedLight) setNodeStatusConditions(node *edgeapi.NodeStatusRequest) {
	e.setNodeReadyCondition(node)
}

// setNodeReadyCondition is partially come from "k8s.io/kubernetes/pkg/kubelet.setNodeReadyCondition"
func (e *edgedLight) setNodeReadyCondition(node *edgeapi.NodeStatusRequest) {
	currentTime := metav1.NewTime(time.Now())
	var newNodeReadyCondition v1.NodeCondition

	newNodeReadyCondition = v1.NodeCondition{
		Type:              v1.NodeReady,
		Status:            v1.ConditionTrue,
		Reason:            "EdgeReady",
		Message:           "edge is posting ready status",
		LastHeartbeatTime: currentTime,
	}

	readyConditionUpdated := false
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == v1.NodeReady {
			if node.Status.Conditions[i].Status == newNodeReadyCondition.Status {
				newNodeReadyCondition.LastTransitionTime = node.Status.Conditions[i].LastTransitionTime
			} else {
				newNodeReadyCondition.LastTransitionTime = currentTime
			}
			node.Status.Conditions[i] = newNodeReadyCondition
			readyConditionUpdated = true
			break
		}
	}
	if !readyConditionUpdated {
		newNodeReadyCondition.LastTransitionTime = currentTime
		node.Status.Conditions = append(node.Status.Conditions, newNodeReadyCondition)
	}
}

func (e *edgedLight) getNodeInfo() (v1.NodeSystemInfo, error) {
	nodeInfo := v1.NodeSystemInfo{}
	kernel, err := util.Command("uname", []string{"-r"})
	if err != nil {
		return nodeInfo, err
	}

	prettyName, err := util.Command("sh", []string{"-c", `cat /etc/os-release | grep PRETTY_NAME| awk -F '"' '{print$2}'`})
	if err != nil {
		return nodeInfo, err
	}

	nodeInfo.ContainerRuntimeVersion = fmt.Sprintf("use podman as container runtime")

	nodeInfo.KernelVersion = kernel
	nodeInfo.OperatingSystem = runtime.GOOS
	nodeInfo.Architecture = runtime.GOARCH
	nodeInfo.KubeletVersion = e.version
	nodeInfo.OSImage = prettyName

	return nodeInfo, nil
}

func (e *edgedLight) setMemInfo(total, allocated v1.ResourceList) error {
	out, err := ioutil.ReadFile("/proc/meminfo")
	if err != nil {
		return err
	}
	matches := regexp.MustCompile(`MemTotal:\s*([0-9]+) kB`).FindSubmatch(out)
	if len(matches) != 2 {
		return fmt.Errorf("failed to match regexp in output: %q", string(out))
	}
	m, err := strconv.ParseInt(string(matches[1]), 10, 64)
	if err != nil {
		return err
	}
	totalMem := m / 1024
	mem := resource.MustParse(strconv.FormatInt(totalMem, 10) + "Mi")
	total[v1.ResourceMemory] = mem.DeepCopy()

	if mem.Cmp(reservationMemory) > 0 {
		mem.Sub(reservationMemory)
	}
	allocated[v1.ResourceMemory] = mem.DeepCopy()

	return nil
}

func (e *edgedLight) setCPUInfo(total, allocated v1.ResourceList) error {
	total[v1.ResourceCPU] = resource.MustParse(fmt.Sprintf("%d", runtime.NumCPU()))
	allocated[v1.ResourceCPU] = total[v1.ResourceCPU].DeepCopy()

	return nil
}

func (e *edgedLight) getNodePhase() v1.NodePhase {
	return v1.NodeRunning
}

func (e *edgedLight) registerNode() error {
	node, err := e.initialNode()
	if err != nil {
		klog.Errorf("Unable to construct v1.Node object for edge: %v", err)
		return err
	}

	e.setInitNode(node)

	if !config.Config.RegisterNode {
		//when register-node set to false, do not auto register node
		klog.Infof("register-node is set to false")
		e.registrationCompleted = true
		return nil
	}

	klog.Infof("Attempting to register node %s", e.nodeName)

	resource := fmt.Sprintf("%s/%s/%s", e.namespace, model.ResourceTypeNodeStatus, e.nodeName)
	nodeInfoMsg := message.BuildMsg(modules.MetaGroup, "", modules.EdgedModuleName, resource, model.InsertOperation, node)
	var res model.Message
	if _, ok := core.GetModules()[edgehub.ModuleNameEdgeHub]; ok {
		res, err = beehiveContext.SendSync(edgehub.ModuleNameEdgeHub, *nodeInfoMsg, syncMsgRespTimeout)
	} else {
		res, err = beehiveContext.SendSync(EdgeController, *nodeInfoMsg, syncMsgRespTimeout)
	}

	if err != nil || res.Content != "OK" {
		klog.Errorf("register node failed, error: %v", err)
		if res.Content != "OK" {
			klog.Errorf("response from cloud core: %v", res.Content)
		}
		return err
	}

	klog.Infof("Successfully registered node %s", e.nodeName)
	e.registrationCompleted = true

	return nil
}

func (e *edgedLight) updateNodeStatus() error {
	nodeStatus, err := e.getNodeStatusRequest(&initNode)
	if err != nil {
		klog.Errorf("Unable to construct api.NodeStatusRequest object for edge: %v", err)
		return err
	}

	err = e.metaClient.NodeStatus(e.namespace).Update(e.nodeName, *nodeStatus)
	if err != nil {
		klog.Errorf("update node failed, error: %v", err)
	}
	return nil
}

func (e *edgedLight) syncNodeStatus() {
	if !e.registrationCompleted {
		if err := e.registerNode(); err != nil {
			klog.Errorf("Register node failed: %v", err)
			return
		}
	}

	if err := e.updateNodeStatus(); err != nil {
		klog.Errorf("Unable to update node status: %v", err)
	}
}

