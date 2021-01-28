package podman

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/containers/podman/v2/libpod/define"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/container"
)

const (
	YamlPath = "/temp/podman_client"
)

type Interface interface {
	CreatePod(pod *v1.Pod) error
	DeletePod(nameOrId string) error
	GetPodStatus(uid types.UID, name, namespace string) (*container.PodStatus, error)
	ContainersExist(names []string) bool
	PodVolumesExist(pod *v1.Pod) bool
}

// podmanClient实现Interface
type podmanClient struct {
	podmanStateToK8sState map[string]container.State
}

func NewClient() Interface {
	err := os.MkdirAll(YamlPath, os.ModeDir)
	if err != nil {
		klog.Fatalf("can't create directory %s, err: %v", YamlPath, err)
		return nil
	}
	pc := &podmanClient{
		podmanStateToK8sState: make(map[string]container.State),
	}
	pc.podmanStateToK8sState["unknown"] = container.ContainerStateUnknown
	pc.podmanStateToK8sState["configured"] = container.ContainerStateCreated
	pc.podmanStateToK8sState["created"] = container.ContainerStateCreated
	pc.podmanStateToK8sState["running"] = container.ContainerStateRunning
	pc.podmanStateToK8sState["stopped"] = container.ContainerStateExited
	pc.podmanStateToK8sState["exited"] = container.ContainerStateExited
	pc.podmanStateToK8sState["removing"] = container.ContainerStateExited
	return pc
}

func (pc *podmanClient) CreatePod(pod *v1.Pod) error {
	//result, err := ExecCommand("podman volume create ")
	var volumeresult string
	var podmanresult string
	var HostPort string
	var ContainerPort string
	var err error
	result, _ := ExecCommand("podman pod list | grep " + pod.Name)
	if len(result) != 0 {
		return nil
	}

	strings.Contains()
	containercmd := "podman container run --pod " + pod.Name + " --name " + pod.Spec.Containers[0].Name
	podcmd := "podman pod create --name " + pod.Name
	if pod.Spec.Volumes != nil && strings.Contains(pod.Spec.Volumes[0].Name, "default-token") {
		klog.Info(pod.Spec.Volumes[0].Name)
		volumeresult, err = ExecCommand("podman volume create " + pod.Spec.Volumes[0].Name)
		if err != nil {
			klog.Error(err)
			return err
		}
		containercmd += " -v " + pod.Spec.Volumes[0].Name
	}

	//result, err := ExecCommand("podman pod create --name " + name)
	//根据v1.pod启动podman pod，满足pod.Name与podman的podname相同即可

	if pod.Spec.Containers[0].Ports != nil {
		HostPort = strconv.Itoa(int(pod.Spec.Containers[0].Ports[0].HostPort))
		ContainerPort = strconv.Itoa(int(pod.Spec.Containers[0].Ports[0].ContainerPort))
		podcmd += " -p " + HostPort + ":" + ContainerPort
	}
	podmanresult, err = ExecCommand(podcmd)
	if err != nil {
		klog.Error(err)
		return err
	}

	//根据v1.pod.Spec.Containers[0].Image创建容器
	containercmd += " " + pod.Spec.Containers[0].Image
	klog.Infof(containercmd)
	containerresult, err := ExecCommand(containercmd)
	if err != nil {
		klog.Error(err)
		return err
	}
	//d, err := yaml.Marshal(pod)
	//if err != nil {
	//	return err
	//}
	//filename := fmt.Sprintf("%s/temp.yaml", YamlPath)
	//err = ioutil.WriteFile(filename, d, os.ModeTemporary)
	//if err != nil {
	//	return err
	//}
	//result, err := ExecCommand("podman play kube ./temp.yaml")

	klog.Infof("successfully created!!")

	if strings.Contains(volumeresult, "Error") || len(podmanresult) == 0 {
		return fmt.Errorf(volumeresult)
	}
	if strings.Contains(podmanresult, "Error") || len(podmanresult) == 0 {
		return fmt.Errorf(podmanresult)
	}
	if strings.Contains(containerresult, "Error") || len(containerresult) == 0 {
		return fmt.Errorf(containerresult)
	}
	klog.Infof("successfully created!!")
	return nil
}

func (pc *podmanClient) DeletePod(nameOrId string) error {
	result, err := ExecCommand("podman pod rm -f " + nameOrId)
	if err != nil {
		return err
	}
	if strings.Contains(result, "Error") {
		return fmt.Errorf("%s", result)
	}
	return nil
}

func (pc *podmanClient) GetPodStatus(uid types.UID, name, namespace string) (*container.PodStatus, error) {
	var result string
	var err error
	//
	//if uid != "" {
	//	result, err = ExecCommand(fmt.Sprintf("podman pod inspect %s --namespace %s", uid, namespace))
	//} else if name != "" {
	result, err = ExecCommand(fmt.Sprintf("podman pod inspect %s ", name))
	//}

	if strings.Contains(result, "Error") {
		return nil, fmt.Errorf("%s", result)
	}
	if err != nil {
		return nil, err
	}

	var podmanObj define.InspectPodData

	err = json.Unmarshal([]byte(result), &podmanObj)
	klog.Infof("podmanobj : %v", podmanObj)
	if err != nil {
		return nil, err
	}
	//var podmanStatus []*container.Status
	var ContainerStatuses []*container.Status
	for _, c := range podmanObj.Containers {

		var k8sStatus = &container.Status{
			ID:        container.ContainerID{"podman", c.ID},
			Name:      c.Name,
			State:     pc.podmanStateToK8sState[c.State],
			CreatedAt: podmanObj.Created,
		}

		ContainerStatuses = append(ContainerStatuses, k8sStatus)
	}

	for _, i := range ContainerStatuses {
		klog.Infof("containerstatus:%s", i)
	}

	return &container.PodStatus{
		ID:                types.UID(podmanObj.ID),
		Name:              podmanObj.Name,
		Namespace:         podmanObj.Namespace,
		IPs:               nil,
		ContainerStatuses: ContainerStatuses,
		SandboxStatuses:   nil,
	}, nil
}

func (pc *podmanClient) ContainersExist(names []string) bool {
	for _, name := range names {
		result, _ := ExecCommand("podman container list | grep " + name)
		if len(result) != 0 {
			return true
		}
	}
	return false
}

func (pc *podmanClient) PodVolumesExist(pod *v1.Pod) bool {
	return false
}

func ExecCommand(s string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", s)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	return out.String(), err
}

func WriteToFile(fileName string, content string) error {
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("file create failed. err: " + err.Error())
	} else {
		// offset
		//os.Truncate(filename, 0) //clear
		n, _ := f.Seek(0, os.SEEK_END)
		_, err = f.WriteAt([]byte(content), n)
		fmt.Println("write succeed!")
		defer f.Close()
	}
	return err
}
