package victimsselection

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"math"
	"sort"
)

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = "VictimsSelection"

// VictimsSelection is a plugin that implements selecting victim pods in preemption.
type VictimsSelection struct{
	frameworkHandle framework.FrameworkHandle
}

var _ framework.VictimsSelectionPlugin = &VictimsSelection{}

// New initializes a new plugin and returns it.
func New(plArgs *runtime.Unknown, handle framework.FrameworkHandle) (framework.Plugin, error) {
	return &VictimsSelection{
		frameworkHandle: handle,
	}, nil
}

func (v *VictimsSelection) Name() string {
	return Name
}

func (v *VictimsSelection) SelectVictimCandidatesOnNode(
	ctx context.Context,
	state *framework.CycleState,
	preemptor *v1.Pod,
	nodeName string,
	victimsEligibleToPreempt []*v1.Pod,
	canPotentialVictimsMakeEnoughRoom func(potentialVictims []*v1.Pod) (bool, error),
	filterPDBViolationPods func(pods []*v1.Pod) (violatingPods, nonViolatingPods []*v1.Pod),
) ([]*v1.Pod, *framework.Status) {
	// Try to reprieve as many pods as possible. We first try to reprieve the PDB
	// violating victims and then other non-violating ones. In both cases, we start
	// from the highest priority victims.
	sort.Slice(
		victimsEligibleToPreempt,
		func(i, j int) bool { return util.MoreImportantPod(victimsEligibleToPreempt[i], victimsEligibleToPreempt[j]) },
	)
	violatingVictims, nonViolatingVictims := filterPDBViolationPods(victimsEligibleToPreempt)

	selectedVictimsSet := map[*v1.Pod]struct{}{}
	for _, v := range victimsEligibleToPreempt {
		selectedVictimsSet[v] = struct{}{}
	}
	reprievePod := func(p *v1.Pod) error {
		victimsCandidate := []*v1.Pod{}
		for v := range selectedVictimsSet {
			if v != p {
				victimsCandidate = append(victimsCandidate, v)
			}
		}
		fits, err := canPotentialVictimsMakeEnoughRoom(victimsCandidate)
		if err != nil {
			return err
		}
		if fits {
			delete(selectedVictimsSet, p)
		} else {
			klog.V(5).Infof("Pod %v/%v is a potential preemption victim on node %v.", p.Namespace, p.Name, nodeName)
		}
		return nil
	}

	// reprieve pdb violating victims first
	for _, p := range violatingVictims {
		if err := reprievePod(p); err != nil {
			klog.Warningf("Failed to reprieve pod %q: %v", p.Name, err)
			return nil, framework.NewStatus(framework.Error, err.Error())
		}
	}

	// Now we try to reprieve non-violating victims.
	for _, p := range nonViolatingVictims {
		if err := reprievePod(p); err != nil {
			klog.Warningf("Failed to reprieve pod %q: %v", p.Name, err)
			return nil, framework.NewStatus(framework.Error, err.Error())
		}
	}

	selectedVictimsList := []*v1.Pod{}
	for v := range selectedVictimsSet {
		selectedVictimsList = append(selectedVictimsList, v)
	}

	return selectedVictimsList, nil
}

// PickOneNodeForPreemption chooses one node among the given nodes. It assumes
// pods in each map entry are ordered by decreasing priority.
// It picks a node based on the following criteria:
// 1. A node with minimum number of PDB violations.
// 2. A node with minimum highest priority victim is picked.
// 3. Ties are broken by sum of priorities of all victims.
// 4. If there are still ties, node with the minimum number of victims is picked.
// 5. If there are still ties, node with the latest start time of all highest priority victims is picked.
// 6. If there are still ties, the first such node is picked (sort of randomly).
// The 'minNodes1' and 'minNodes2' are being reused here to save the memory
// allocation and garbage collection time.
func (v *VictimsSelection) PickOneNodeForPreemption(
	ctx context.Context,
	state *framework.CycleState,
	nodeNameToVictims map[string]*extenderv1.Victims,
) (string, *framework.Status) {
	if len(nodeNameToVictims) == 0 {
		return "", nil
	}
	minNumPDBViolatingPods := int64(math.MaxInt32)
	var minNodes1 []string
	lenNodes1 := 0
	for nodeName, victims := range nodeNameToVictims {
		if len(victims.Pods) == 0 {
			// We found a node that doesn't need any preemption. Return it!
			// This should happen rarely when one or more pods are terminated between
			// the time that scheduler tries to schedule the pod and the time that
			// preemption logic tries to find nodes for preemption.
			return nodeName, nil
		}
		numPDBViolatingPods := victims.NumPDBViolations
		if numPDBViolatingPods < minNumPDBViolatingPods {
			minNumPDBViolatingPods = numPDBViolatingPods
			minNodes1 = nil
			lenNodes1 = 0
		}
		if numPDBViolatingPods == minNumPDBViolatingPods {
			minNodes1 = append(minNodes1, nodeName)
			lenNodes1++
		}
	}
	if lenNodes1 == 1 {
		return minNodes1[0], nil
	}

	// There are more than one node with minimum number PDB violating pods. Find
	// the one with minimum highest priority victim.
	minHighestPriority := int32(math.MaxInt32)
	var minNodes2 = make([]string, lenNodes1)
	lenNodes2 := 0
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		victims := nodeNameToVictims[node]
		// highestPodPriority is the highest priority among the victims on this node.
		highestPodPriority := podutil.GetPodPriority(victims.Pods[0])
		if highestPodPriority < minHighestPriority {
			minHighestPriority = highestPodPriority
			lenNodes2 = 0
		}
		if highestPodPriority == minHighestPriority {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	if lenNodes2 == 1 {
		return minNodes2[0], nil
	}

	// There are a few nodes with minimum highest priority victim. Find the
	// smallest sum of priorities.
	minSumPriorities := int64(math.MaxInt64)
	lenNodes1 = 0
	for i := 0; i < lenNodes2; i++ {
		var sumPriorities int64
		node := minNodes2[i]
		for _, pod := range nodeNameToVictims[node].Pods {
			// We add MaxInt32+1 to all priorities to make all of them >= 0. This is
			// needed so that a node with a few pods with negative priority is not
			// picked over a node with a smaller number of pods with the same negative
			// priority (and similar scenarios).
			sumPriorities += int64(podutil.GetPodPriority(pod)) + int64(math.MaxInt32+1)
		}
		if sumPriorities < minSumPriorities {
			minSumPriorities = sumPriorities
			lenNodes1 = 0
		}
		if sumPriorities == minSumPriorities {
			minNodes1[lenNodes1] = node
			lenNodes1++
		}
	}
	if lenNodes1 == 1 {
		return minNodes1[0], nil
	}

	// There are a few nodes with minimum highest priority victim and sum of priorities.
	// Find one with the minimum number of pods.
	minNumPods := math.MaxInt32
	lenNodes2 = 0
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		numPods := len(nodeNameToVictims[node].Pods)
		if numPods < minNumPods {
			minNumPods = numPods
			lenNodes2 = 0
		}
		if numPods == minNumPods {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	if lenNodes2 == 1 {
		return minNodes2[0], nil
	}

	// There are a few nodes with same number of pods.
	// Find the node that satisfies latest(earliestStartTime(all highest-priority pods on node))
	latestStartTime := util.GetEarliestPodStartTime(nodeNameToVictims[minNodes2[0]])
	if latestStartTime == nil {
		// If the earliest start time of all pods on the 1st node is nil, just return it,
		// which is not expected to happen.
		klog.Errorf("earliestStartTime is nil for node %s. Should not reach here.", minNodes2[0])
		return minNodes2[0], nil
	}
	nodeToReturn := minNodes2[0]
	for i := 1; i < lenNodes2; i++ {
		node := minNodes2[i]
		// Get earliest start time of all pods on the current node.
		earliestStartTimeOnNode := util.GetEarliestPodStartTime(nodeNameToVictims[node])
		if earliestStartTimeOnNode == nil {
			klog.Errorf("earliestStartTime is nil for node %s. Should not reach here.", node)
			continue
		}
		if earliestStartTimeOnNode.After(latestStartTime.Time) {
			latestStartTime = earliestStartTimeOnNode
			nodeToReturn = node
		}
	}

	return nodeToReturn, nil
}
