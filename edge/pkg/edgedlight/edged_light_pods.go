package edgedlight

import (
	"sort"

	pkgutil "github.com/kubeedge/kubeedge/pkg/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/status"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
)

// 从podman获知pod是否可以删除，参考edged同名函数
func (e *edgedLight) PodResourcesAreReclaimed(pod *v1.Pod, status v1.PodStatus) bool {
	if !notRunning(status.ContainerStatuses) {
		// We shouldn't delete pods that still have running containers
		klog.V(3).Infof("Pod %q is terminated, but some containers are still running", format.Pod(pod))
		return false
	}
	contNames := make([]string, 0)
	// pod's containers should be deleted
	for _, containerStatus := range status.InitContainerStatuses {
		contNames = append(contNames, containerStatus.Name)
	}
	for _, containerStatus := range status.ContainerStatuses {
		contNames = append(contNames, containerStatus.Name)
	}
	if e.podmanClient.ContainersExist(contNames) {
		klog.V(3).Infof("Pod %q is terminated, but some containers have not been cleaned up")
		return false
	}

	if e.podmanClient.PodVolumesExist(pod) {
		// We shouldn't delete pods whose volumes have not been cleaned up if we are not keeping terminated pod volumes
		klog.V(3).Infof("Pod %q is terminated, but some volumes have not been cleaned up", format.Pod(pod))
		return false
	}

	return true
}

// convertStatusToAPIStatus creates an api PodStatus for the given pod from
// the given internal pod status.  It is purely transformative and does not
// alter the kubelet state at all.
func (e *edgedLight) convertStatusToAPIStatus(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *v1.PodStatus {
	var apiPodStatus v1.PodStatus

	hostIP, err := pkgutil.GetLocalIP(e.nodeName)
	if err != nil {
		klog.Errorf("Failed to get host IP: %v", err)
	} else {
		apiPodStatus.HostIP = hostIP

		apiPodStatus.PodIPs = make([]v1.PodIP, 0, len(podStatus.IPs))
		for _, ip := range podStatus.IPs {
			apiPodStatus.PodIPs = append(apiPodStatus.PodIPs, v1.PodIP{
				IP: ip,
			})
		}

		if len(apiPodStatus.PodIPs) > 0 {
			apiPodStatus.PodIP = apiPodStatus.PodIPs[0].IP
		}

		if pod.Spec.HostNetwork && apiPodStatus.PodIP == "" {
			apiPodStatus.PodIP = hostIP
		}
	}
	// set status for Pods created on versions of kube older than 1.6
	apiPodStatus.QOSClass = v1qos.GetPodQOS(pod)

	oldPodStatus, found := e.statusManager.GetPodStatus(pod.UID)
	if !found {
		oldPodStatus = pod.Status
	}

	apiPodStatus.ContainerStatuses = e.convertToAPIContainerStatuses(
		pod, podStatus,
		oldPodStatus.ContainerStatuses,
		pod.Spec.Containers,
		len(pod.Spec.InitContainers) > 0,
		false,
	)
	apiPodStatus.InitContainerStatuses = e.convertToAPIContainerStatuses(
		pod, podStatus,
		oldPodStatus.InitContainerStatuses,
		pod.Spec.InitContainers,
		len(pod.Spec.InitContainers) > 0,
		true,
	)

	return &apiPodStatus
}

// convertToAPIContainerStatuses converts the given internal container
// statuses into API container statuses.
func (e *edgedLight) convertToAPIContainerStatuses(pod *v1.Pod, podStatus *kubecontainer.PodStatus, previousStatus []v1.ContainerStatus, containers []v1.Container, hasInitContainers, isInitContainer bool) []v1.ContainerStatus {
	convertContainerStatus := func(cs *kubecontainer.Status) *v1.ContainerStatus {
		cid := cs.ID.String()
		cstatus := &v1.ContainerStatus{
			Name:         cs.Name,
			RestartCount: int32(cs.RestartCount),
			Image:        cs.Image,
			ImageID:      cs.ImageID,
			ContainerID:  cid,
		}
		switch cs.State {
		case kubecontainer.ContainerStateRunning:
			cstatus.State.Running = &v1.ContainerStateRunning{StartedAt: metav1.NewTime(cs.StartedAt)}
			cstatus.Ready = true
		case kubecontainer.ContainerStateCreated:
			// Treat containers in the "created" state as if they are exited.
			// The pod workers are supposed start all containers it creates in
			// one sync (syncPod) iteration. There should not be any normal
			// "created" containers when the pod worker generates the status at
			// the beginning of a sync iteration.
			fallthrough
		case kubecontainer.ContainerStateExited:
			cstatus.State.Terminated = &v1.ContainerStateTerminated{
				ExitCode:    int32(cs.ExitCode),
				Reason:      cs.Reason,
				Message:     cs.Message,
				StartedAt:   metav1.NewTime(cs.StartedAt),
				FinishedAt:  metav1.NewTime(cs.FinishedAt),
				ContainerID: cid,
			}
		default:
			cstatus.State.Waiting = &v1.ContainerStateWaiting{}
		}
		return cstatus
	}

	// Fetch old containers statuses from old pod status.
	oldStatuses := make(map[string]v1.ContainerStatus, len(containers))
	for _, cstatus := range previousStatus {
		oldStatuses[cstatus.Name] = cstatus
	}

	// Set all container statuses to default waiting state
	statuses := make(map[string]*v1.ContainerStatus, len(containers))
	defaultWaitingState := v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "ContainerCreating"}}
	if hasInitContainers {
		defaultWaitingState = v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "PodInitializing"}}
	}

	for _, container := range containers {
		cstatus := &v1.ContainerStatus{
			Name:  container.Name,
			Image: container.Image,
			State: defaultWaitingState,
		}
		oldStatus, found := oldStatuses[container.Name]
		if found {
			if oldStatus.State.Terminated != nil {
				// Do not update status on terminated init containers as
				// they be removed at any time.
				cstatus = &oldStatus
			} else {
				// Apply some values from the old statuses as the default values.
				cstatus.RestartCount = oldStatus.RestartCount
				cstatus.LastTerminationState = oldStatus.LastTerminationState
			}
		}
		statuses[container.Name] = cstatus
	}

	// Make the latest container status comes first.
	sort.Sort(sort.Reverse(kubecontainer.SortContainerStatusesByCreationTime(podStatus.ContainerStatuses)))
	// Set container statuses according to the statuses seen in pod status
	containerSeen := map[string]int{}
	for _, cStatus := range podStatus.ContainerStatuses {
		cName := cStatus.Name
		if _, ok := statuses[cName]; !ok {
			// This would also ignore the infra container.
			continue
		}
		if containerSeen[cName] >= 2 {
			continue
		}
		cstatus := convertContainerStatus(cStatus)
		if containerSeen[cName] == 0 {
			statuses[cName] = cstatus
		} else {
			statuses[cName].LastTerminationState = cstatus.State
		}
		containerSeen[cName] = containerSeen[cName] + 1
	}

	// Handle the containers failed to be started, which should be in Waiting state.
	for _, container := range containers {
		if isInitContainer {
			// If the init container is terminated with exit code 0, it won't be restarted.
			// TODO(random-liu): Handle this in a cleaner way.
			s := podStatus.FindContainerStatusByName(container.Name)
			if s != nil && s.State == kubecontainer.ContainerStateExited && s.ExitCode == 0 {
				continue
			}
		}
		// If a container should be restarted in next syncpod, it is *Waiting*.
		if !kubecontainer.ShouldContainerBeRestarted(&container, pod, podStatus) {
			continue
		}
		cstatus := statuses[container.Name]
		if cstatus.State.Terminated != nil {
			cstatus.LastTerminationState = cstatus.State
		}
		statuses[container.Name] = cstatus
	}

	var containerStatuses []v1.ContainerStatus
	for _, cstatus := range statuses {
		containerStatuses = append(containerStatuses, *cstatus)
	}

	// Sort the container statuses since clients of this interface expect the list
	// of containers in a pod has a deterministic order.
	if isInitContainer {
		kubetypes.SortInitContainerStatuses(pod, containerStatuses)
	} else {
		sort.Sort(kubetypes.SortedContainerStatuses(containerStatuses))
	}
	return containerStatuses
}

func toKubeContainerStatus(phase v1.PodPhase, status *kubecontainer.Status) v1.ContainerStatus {
	kubeStatus := v1.ContainerStatus{
		Name:         status.Name,
		RestartCount: int32(status.RestartCount),
		ImageID:      status.ImageID,
		Image:        status.Image,
		ContainerID:  status.ID.ID,
	}
	switch phase {
	case v1.PodRunning:
		kubeStatus.State.Running = &v1.ContainerStateRunning{StartedAt: metav1.NewTime(status.StartedAt)}
		kubeStatus.Ready = true
	case v1.PodFailed, v1.PodSucceeded:
		kubeStatus.State.Terminated = &v1.ContainerStateTerminated{
			ExitCode:    int32(status.ExitCode),
			Reason:      status.Reason,
			Message:     status.Message,
			StartedAt:   metav1.NewTime(status.StartedAt),
			FinishedAt:  metav1.NewTime(status.FinishedAt),
			ContainerID: status.ID.ID,
		}
	default:
		kubeStatus.State.Waiting = &v1.ContainerStateWaiting{
			Reason:  status.Reason,
			Message: status.Message,
		}
	}
	return kubeStatus
}

// getPhase returns the phase of a pod given its container info.
func getPhase(spec *v1.PodSpec, info []v1.ContainerStatus) v1.PodPhase {
	initialized := 0
	pendingInitialization := 0
	failedInitialization := 0
	for _, container := range spec.InitContainers {
		containerStatus, ok := podutil.GetContainerStatus(info, container.Name)
		if !ok {
			pendingInitialization++
			continue
		}

		switch {
		case containerStatus.State.Running != nil:
			pendingInitialization++
		case containerStatus.State.Terminated != nil:
			if containerStatus.State.Terminated.ExitCode == 0 {
				initialized++
			} else {
				failedInitialization++
			}
		case containerStatus.State.Waiting != nil:
			if containerStatus.LastTerminationState.Terminated != nil {
				if containerStatus.LastTerminationState.Terminated.ExitCode == 0 {
					initialized++
				} else {
					failedInitialization++
				}
			} else {
				pendingInitialization++
			}
		default:
			pendingInitialization++
		}
	}

	unknown := 0
	running := 0
	waiting := 0
	stopped := 0
	failed := 0
	succeeded := 0
	for _, container := range spec.Containers {
		containerStatus, ok := podutil.GetContainerStatus(info, container.Name)
		if !ok {
			unknown++
			continue
		}

		switch {
		case containerStatus.State.Running != nil:
			running++
		case containerStatus.State.Terminated != nil:
			stopped++
			if containerStatus.State.Terminated.ExitCode == 0 {
				succeeded++
			} else {
				failed++
			}
		case containerStatus.State.Waiting != nil:
			if containerStatus.LastTerminationState.Terminated != nil {
				stopped++
			} else {
				waiting++
			}
		default:
			unknown++
		}
	}

	if failedInitialization > 0 && spec.RestartPolicy == v1.RestartPolicyNever {
		return v1.PodFailed
	}

	switch {
	case pendingInitialization > 0:
		fallthrough
	case waiting > 0:
		klog.Info("pod waiting > 0, pending")
		// One or more containers has not been started
		return v1.PodPending
	case running > 0 && unknown == 0:
		// All containers have been started, and at least
		// one container is running
		return v1.PodRunning
	case running == 0 && stopped > 0 && unknown == 0:
		// All containers are terminated
		if spec.RestartPolicy == v1.RestartPolicyAlways {
			// All containers are in the process of restarting
			return v1.PodRunning
		}
		if stopped == succeeded {
			// RestartPolicy is not Always, and all
			// containers are terminated in success
			return v1.PodSucceeded
		}
		if spec.RestartPolicy == v1.RestartPolicyNever {
			// RestartPolicy is Never, and all containers are
			// terminated with at least one in failure
			return v1.PodFailed
		}
		// RestartPolicy is OnFailure, and at least one in failure
		// and in the process of restarting
		return v1.PodRunning
	default:
		klog.Info("pod default case, pending")
		return v1.PodPending
	}
}

func (e *edgedLight) updatePodStatus(pod *v1.Pod) error {
	klog.Infof("start pod status")
	var podStatus *v1.PodStatus
	var newStatus v1.PodStatus
	var podStatusRemote *kubecontainer.PodStatus
	var err error
	if e.podmanClient != nil {
		podStatusRemote, err = e.podmanClient.GetPodStatus(pod.UID, pod.Name, pod.Namespace)
		if err != nil {
			klog.Errorf("%v", err)
			containerStatus := &kubecontainer.Status{}
			kubeStatus := toKubeContainerStatus(v1.PodUnknown, containerStatus)
			podStatus = &v1.PodStatus{Phase: v1.PodUnknown, ContainerStatuses: []v1.ContainerStatus{kubeStatus}}
		} else {
			if pod.DeletionTimestamp != nil {
				containerStatus := &kubecontainer.Status{State: kubecontainer.ContainerStateExited,
					Reason: "Completed"}
				kubeStatus := toKubeContainerStatus(v1.PodSucceeded, containerStatus)
				podStatus = &v1.PodStatus{Phase: v1.PodSucceeded, ContainerStatuses: []v1.ContainerStatus{kubeStatus}}
			} else {
				podStatus = e.convertStatusToAPIStatus(pod, podStatusRemote)
				// Assume info is ready to process
				spec := &pod.Spec
				allStatus := append(append([]v1.ContainerStatus{}, podStatus.ContainerStatuses...), podStatus.InitContainerStatuses...)
				klog.Infof("allstatus : %v", allStatus)
				podStatus.Phase = getPhase(spec, allStatus)
				// Check for illegal phase transition
				if pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded {
					// API server shows terminal phase; transitions are not allowed
					if podStatus.Phase != pod.Status.Phase {
						klog.Errorf("Pod attempted illegal phase transition from %s to %s: %v", pod.Status.Phase, podStatus.Phase, podStatus)
						// Force back to phase from the API server
						podStatus.Phase = pod.Status.Phase
					}
				}
			}
		}
	}
	klog.Infof("podstatus : %s", podStatus.Phase)
	newStatus = *podStatus.DeepCopy()

	//e.probeManager.UpdatePodStatus(pod.UID, &newStatus)
	if e.podmanClient != nil {
		spec := &pod.Spec
		newStatus.Conditions = append(newStatus.Conditions, status.GeneratePodInitializedCondition(spec, newStatus.InitContainerStatuses, newStatus.Phase))
		newStatus.Conditions = append(newStatus.Conditions, status.GeneratePodReadyCondition(spec, newStatus.Conditions, newStatus.ContainerStatuses, newStatus.Phase))
		//newStatus.Conditions = append(newStatus.Conditions, status.GenerateContainersReadyCondition(spec, newStatus.ContainerStatuses, newStatus.Phase))
		newStatus.Conditions = append(newStatus.Conditions, v1.PodCondition{
			Type:   v1.PodScheduled,
			Status: v1.ConditionTrue,
		})
	}
	pod.Status = newStatus
	e.statusManager.SetPodStatus(pod, newStatus)
	return nil
}
