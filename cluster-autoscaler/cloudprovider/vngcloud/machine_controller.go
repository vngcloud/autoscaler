package vngcloud

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog/v2"
	"strings"
	lsync "sync"

	lschema "k8s.io/apimachinery/pkg/runtime/schema"
	ldynamic "k8s.io/client-go/dynamic"
	ldynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	linformers "k8s.io/client-go/informers"
	lscale "k8s.io/client-go/scale"
	lcache "k8s.io/client-go/tools/cache"
)

type machineController struct {
	workloadInformerFactory     linformers.SharedInformerFactory
	managementInformerFactory   ldynamicinformer.DynamicSharedInformerFactory
	machineDeploymentInformer   linformers.GenericInformer
	machineInformer             linformers.GenericInformer
	machineSetInformer          linformers.GenericInformer
	machinePoolInformer         linformers.GenericInformer
	nodeInformer                lcache.SharedIndexInformer
	managementClient            ldynamic.Interface
	managementScaleClient       lscale.ScalesGetter
	machineSetResource          lschema.GroupVersionResource
	machineResource             lschema.GroupVersionResource
	machinePoolResource         lschema.GroupVersionResource
	machinePoolsAvailable       bool
	machineDeploymentResource   lschema.GroupVersionResource
	machineDeploymentsAvailable bool
	accessLock                  lsync.Mutex
	autoDiscoverySpecs          []*vngcloudAutoDiscoveryConfig

	// stopChannel is used for running the shared informers, and for starting
	// informers associated with infrastructure machine templates that are
	// discovered during operation.
	stopChannel <-chan struct{}
}

func (c *machineController) run() error {
	c.workloadInformerFactory.Start(c.stopChannel)
	c.managementInformerFactory.Start(c.stopChannel)

	syncFuncs := []lcache.InformerSynced{
		c.nodeInformer.HasSynced,
		c.machineInformer.Informer().HasSynced,
		c.machineSetInformer.Informer().HasSynced,
	}
	if c.machineDeploymentsAvailable {
		syncFuncs = append(syncFuncs, c.machineDeploymentInformer.Informer().HasSynced)
	}
	if c.machinePoolsAvailable {
		syncFuncs = append(syncFuncs, c.machinePoolInformer.Informer().HasSynced)
	}

	klog.V(4).Infof("waiting for caches to sync")
	if !lcache.WaitForCacheSync(c.stopChannel, syncFuncs...) {
		return fmt.Errorf("syncing caches failed")
	}

	return nil
}

func (c *machineController) nodeGroups() ([]cloudprovider.NodeGroup, error) {
	scalableResources, err := c.listScalableResources()
	if err != nil {
		return nil, err
	}

	nodegroups := make([]cloudprovider.NodeGroup, 0, len(scalableResources))

	for _, r := range scalableResources {
		ng, err := newNodeGroupFromScalableResource(c, r)
		if err != nil {
			return nil, err
		}

		if ng != nil {
			nodegroups = append(nodegroups, ng)
			klog.V(4).Infof("discovered node group: %s", ng.Debug())
		}
	}
	return nodegroups, nil
}

// findNodeByProviderID find the Node object keyed by provideID.
// Returns nil if it cannot be found. A DeepCopy() of the object is
// returned on success.
func (c *machineController) findNodeByProviderID(providerID normalizedProviderID) (*corev1.Node, error) {
	objs, err := c.nodeInformer.GetIndexer().ByIndex(nodeProviderIDIndex, string(providerID))
	if err != nil {
		return nil, err
	}

	switch n := len(objs); {
	case n == 0:
		return nil, nil
	case n > 1:
		return nil, fmt.Errorf("internal error; expected len==1, got %v", n)
	}

	node, ok := objs[0].(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("internal error; unexpected type %T", objs[0])
	}

	return node.DeepCopy(), nil
}

func (c *machineController) listScalableResources() ([]*unstructured.Unstructured, error) {
	scalableResources, err := c.listResources(c.machineSetInformer.Lister())
	if err != nil {
		return nil, err
	}

	if c.machineDeploymentsAvailable {
		machineDeployments, err := c.listResources(c.machineDeploymentInformer.Lister())
		if err != nil {
			return nil, err
		}

		scalableResources = append(scalableResources, machineDeployments...)
	}

	if c.machinePoolsAvailable {
		machinePools, err := c.listResources(c.machinePoolInformer.Lister())
		if err != nil {
			return nil, err
		}

		scalableResources = append(scalableResources, machinePools...)
	}

	return scalableResources, nil
}

func (c *machineController) listResources(lister lcache.GenericLister) ([]*unstructured.Unstructured, error) {
	if len(c.autoDiscoverySpecs) == 0 {
		return listResources(lister.ByNamespace(metav1.NamespaceAll), "", labels.Everything())
	}

	var results []*unstructured.Unstructured
	tracker := map[string]bool{}
	for _, spec := range c.autoDiscoverySpecs {
		resources, err := listResources(lister.ByNamespace(spec.namespace), spec.clusterName, spec.labelSelector)
		if err != nil {
			return nil, err
		}
		for i := range resources {
			r := resources[i]
			key := fmt.Sprintf("%s-%s-%s", r.GetKind(), r.GetNamespace(), r.GetName())
			if _, ok := tracker[key]; !ok {
				results = append(results, r)
				tracker[key] = true
			}
		}
	}

	return results, nil
}

func (c *machineController) allowedByAutoDiscoverySpecs(r *unstructured.Unstructured) bool {
	// If no autodiscovery configuration fall back to previous behavior of allowing all
	if len(c.autoDiscoverySpecs) == 0 {
		return true
	}

	for _, spec := range c.autoDiscoverySpecs {
		if allowedByAutoDiscoverySpec(spec, r) {
			return true
		}
	}

	return false
}

// Get an infrastructure machine template given its GVR, name, and namespace.
func (c *machineController) getInfrastructureResource(resource lschema.GroupVersionResource, name string, namespace string) (*unstructured.Unstructured, error) {
	// get an informer for this type, this will create the informer if it does not exist
	informer := c.managementInformerFactory.ForResource(resource)
	// since this may be a new informer, we need to restart the informer factory
	c.managementInformerFactory.Start(c.stopChannel)
	// wait for the informer to sync
	klog.V(4).Infof("waiting for cache sync on infrastructure resource")
	if !lcache.WaitForCacheSync(c.stopChannel, informer.Informer().HasSynced) {
		return nil, fmt.Errorf("syncing cache on infrastructure resource failed")
	}
	// use the informer to get the object we want, this will use the informer cache if possible
	obj, err := informer.Lister().ByNamespace(namespace).Get(name)
	if err != nil {
		klog.V(4).Infof("Unable to read infrastructure reference from informer, error: %v", err)
		return nil, err
	}

	infra, ok := obj.(*unstructured.Unstructured)
	if !ok {
		err := fmt.Errorf("unable to convert infrastructure reference for %s/%s", namespace, name)
		klog.V(4).Infof("%v", err)
		return nil, err
	}
	return infra, err
}

func (c *machineController) nodeGroupForNode(node *corev1.Node) (*nodegroup, error) {
	scalableResource, err := c.findScalableResourceByProviderID(normalizedProviderString(node.Spec.ProviderID))
	if err != nil {
		return nil, err
	}
	if scalableResource == nil {
		return nil, nil
	}

	nodegroup, err := newNodeGroupFromScalableResource(c, scalableResource)
	if err != nil {
		return nil, fmt.Errorf("failed to build nodegroup for node %q: %v", node.Name, err)
	}

	// the nodegroup will be nil if it doesn't match the autodiscovery configuration
	// or if it doesn't meet the scaling requirements
	if nodegroup == nil {
		return nil, nil
	}

	klog.V(4).Infof("node %q is in nodegroup %q", node.Name, nodegroup.Id())
	return nodegroup, nil
}

func (c *machineController) findScalableResourceByProviderID(providerID normalizedProviderID) (*unstructured.Unstructured, error) {
	// Check for a MachinePool first to simplify the logic afterward.
	if c.machinePoolsAvailable {
		machinePool, err := c.findMachinePoolByProviderID(providerID)
		if err != nil {
			return nil, err
		}
		if machinePool != nil {
			return machinePool, nil
		}
	}

	// Check for a MachineSet.
	machine, err := c.findMachineByProviderID(providerID)
	if err != nil {
		return nil, err
	}
	if machine == nil {
		return nil, nil
	}
	machineSet, err := c.findMachineOwner(machine)
	if err != nil {
		return nil, err
	}
	if machineSet == nil {
		return nil, nil
	}

	// Check for a MachineDeployment.
	if c.machineDeploymentsAvailable {
		machineDeployment, err := c.findMachineSetOwner(machineSet)
		if err != nil {
			return nil, err
		}
		if machineDeployment != nil {
			return machineDeployment, nil
		}
	}

	return machineSet, nil
}

// findMachinePoolByProviderID finds machine pools matching providerID. A
// DeepCopy() of the object is returned on success.
func (c *machineController) findMachinePoolByProviderID(providerID normalizedProviderID) (*unstructured.Unstructured, error) {
	objs, err := c.machinePoolInformer.Informer().GetIndexer().ByIndex(machinePoolProviderIDIndex, string(providerID))
	if err != nil {
		return nil, err
	}

	switch n := len(objs); {
	case n > 1:
		return nil, fmt.Errorf("internal error; expected len==1, got %v", n)
	case n == 1:
		u, ok := objs[0].(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("internal error; unexpected type %T", objs[0])
		}
		return u.DeepCopy(), nil
	default:
		return nil, nil
	}
}

// findMachineByProviderID finds machine matching providerID. A
// DeepCopy() of the object is returned on success.
func (c *machineController) findMachineByProviderID(providerID normalizedProviderID) (*unstructured.Unstructured, error) {
	objs, err := c.machineInformer.Informer().GetIndexer().ByIndex(machineProviderIDIndex, string(providerID))
	if err != nil {
		return nil, err
	}

	switch n := len(objs); {
	case n > 1:
		return nil, fmt.Errorf("internal error; expected len==1, got %v", n)
	case n == 1:
		u, ok := objs[0].(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("internal error; unexpected type %T", objs[0])
		}
		return u.DeepCopy(), nil
	}

	if isFailedMachineProviderID(providerID) {
		return c.findMachine(machineKeyFromFailedProviderID(providerID))
	}
	if isPendingMachineProviderID(providerID) {
		return c.findMachine(machineKeyFromPendingMachineProviderID(providerID))
	}

	// If the machine object has no providerID--maybe actuator
	// does not set this value (e.g., OpenStack)--then first
	// lookup the node using ProviderID. If that is successful
	// then the machine can be found using the annotation (should
	// it exist).
	node, err := c.findNodeByProviderID(providerID)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}

	machineID := node.Annotations[machineAnnotationKey]
	return c.findMachine(machineID)
}

// findMachineOwner returns the machine set owner for machine, or nil
// if there is no owner. A DeepCopy() of the object is returned on
// success.
func (c *machineController) findMachineOwner(machine *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	machineOwnerRef := machineOwnerRef(machine)
	if machineOwnerRef == nil {
		return nil, nil
	}

	return c.findMachineSet(fmt.Sprintf("%s/%s", machine.GetNamespace(), machineOwnerRef.Name))
}

// findMachineSetOwner returns the owner for the machineSet, or nil
// if there is no owner. A DeepCopy() of the object is returned on
// success.
func (c *machineController) findMachineSetOwner(machineSet *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	machineSetOwnerRef := machineSetOwnerRef(machineSet)
	if machineSetOwnerRef == nil {
		return nil, nil
	}

	return c.findMachineDeployment(fmt.Sprintf("%s/%s", machineSet.GetNamespace(), machineSetOwnerRef.Name))
}

func (c *machineController) scalableResourceProviderIDs(scalableResource *unstructured.Unstructured) ([]string, error) {
	if scalableResource.GetKind() == machinePoolKind {
		return c.findMachinePoolProviderIDs(scalableResource)
	}
	return c.findScalableResourceProviderIDs(scalableResource)
}

func (c *machineController) findMachinePoolProviderIDs(scalableResource *unstructured.Unstructured) ([]string, error) {
	var providerIDs []string

	providerIDList, found, err := unstructured.NestedStringSlice(scalableResource.UnstructuredContent(), "spec", "providerIDList")
	if err != nil {
		return nil, err
	}
	if found {
		providerIDs = providerIDList
	} else {
		klog.Warningf("Machine Pool %q has no providerIDList", scalableResource.GetName())
	}

	klog.V(4).Infof("nodegroup %s has %d nodes: %v", scalableResource.GetName(), len(providerIDs), providerIDs)
	return providerIDs, nil
}

func isFailedMachineProviderID(providerID normalizedProviderID) bool {
	return strings.HasPrefix(string(providerID), failedMachinePrefix)
}

func machineKeyFromFailedProviderID(providerID normalizedProviderID) string {
	namespaceName := strings.TrimPrefix(string(providerID), failedMachinePrefix)
	return strings.Replace(namespaceName, "_", "/", 1)
}

func (c *machineController) findMachine(id string) (*unstructured.Unstructured, error) {
	return c.findResourceByKey(c.machineInformer.Informer().GetStore(), id)
}

func (c *machineController) findResourceByKey(store lcache.Store, key string) (*unstructured.Unstructured, error) {
	item, exists, err := store.GetByKey(key)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	u, ok := item.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("internal error; unexpected type: %T", item)
	}

	// Verify the resource is allowed by the autodiscovery configuration
	if !c.allowedByAutoDiscoverySpecs(u) {
		return nil, nil
	}

	return u.DeepCopy(), nil
}

func (c *machineController) findMachineDeployment(id string) (*unstructured.Unstructured, error) {
	return c.findResourceByKey(c.machineDeploymentInformer.Informer().GetStore(), id)
}

func (c *machineController) findMachineSet(id string) (*unstructured.Unstructured, error) {
	return c.findResourceByKey(c.machineSetInformer.Informer().GetStore(), id)
}
func isPendingMachineProviderID(providerID normalizedProviderID) bool {
	return strings.HasPrefix(string(providerID), pendingMachinePrefix)
}

func (c *machineController) findScalableResourceProviderIDs(scalableResource *unstructured.Unstructured) ([]string, error) {
	var providerIDs []string

	machines, err := c.listMachinesForScalableResource(scalableResource)
	if err != nil {
		return nil, fmt.Errorf("error listing machines: %v", err)
	}

	for _, machine := range machines {
		providerID, found, err := unstructured.NestedString(machine.UnstructuredContent(), "spec", "providerID")
		if err != nil {
			return nil, err
		}

		if found {
			if providerID != "" {
				providerIDs = append(providerIDs, providerID)
				continue
			}
		}

		klog.Warningf("Machine %q has no providerID", machine.GetName())

		failureMessage, found, err := unstructured.NestedString(machine.UnstructuredContent(), "status", "failureMessage")
		if err != nil {
			return nil, err
		}

		if found {
			klog.V(4).Infof("Status.FailureMessage of machine %q is %q", machine.GetName(), failureMessage)
			// Provide a fake ID to allow the autoscaler to track machines that will never
			// become nodes and mark the nodegroup unhealthy after maxNodeProvisionTime.
			// Fake ID needs to be recognised later and converted into a machine key.
			// Use an underscore as a separator between namespace and name as it is not a
			// valid character within a namespace name.
			providerIDs = append(providerIDs, fmt.Sprintf("%s%s_%s", failedMachinePrefix, machine.GetNamespace(), machine.GetName()))
			continue
		}

		_, found, err = unstructured.NestedFieldCopy(machine.UnstructuredContent(), "status", "nodeRef")
		if err != nil {
			return nil, err
		}

		if !found {
			klog.V(4).Infof("Status.NodeRef of machine %q is currently nil", machine.GetName())
			providerIDs = append(providerIDs, fmt.Sprintf("%s%s_%s", pendingMachinePrefix, machine.GetNamespace(), machine.GetName()))
			continue
		}

		nodeRefKind, found, err := unstructured.NestedString(machine.UnstructuredContent(), "status", "nodeRef", "kind")
		if err != nil {
			return nil, err
		}

		if found && nodeRefKind != "Node" {
			klog.Errorf("Status.NodeRef of machine %q does not reference a node (rather %q)", machine.GetName(), nodeRefKind)
			continue
		}

		nodeRefName, found, err := unstructured.NestedString(machine.UnstructuredContent(), "status", "nodeRef", "name")
		if err != nil {
			return nil, err
		}

		if found {
			node, err := c.findNodeByNodeName(nodeRefName)
			if err != nil {
				return nil, fmt.Errorf("unknown node %q", nodeRefName)
			}

			if node != nil {
				providerIDs = append(providerIDs, node.Spec.ProviderID)
			}
		}
	}

	klog.V(4).Infof("nodegroup %s has %d nodes: %v", scalableResource.GetName(), len(providerIDs), providerIDs)
	return providerIDs, nil
}

func (c *machineController) listMachinesForScalableResource(r *unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	switch r.GetKind() {
	case machineSetKind, machineDeploymentKind:
		unstructuredSelector, found, err := unstructured.NestedMap(r.UnstructuredContent(), "spec", "selector")
		if err != nil {
			return nil, err
		}

		if !found {
			return nil, fmt.Errorf("expected field spec.selector on scalable resource type")
		}

		labelSelector := &metav1.LabelSelector{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredSelector, labelSelector); err != nil {
			return nil, err
		}

		selector, err := metav1.LabelSelectorAsSelector(labelSelector)
		if err != nil {
			return nil, err
		}

		return listResources(c.machineInformer.Lister().ByNamespace(r.GetNamespace()), clusterNameFromResource(r), selector)
	default:
		return nil, fmt.Errorf("unknown scalable resource kind %s", r.GetKind())
	}
}

// findNodeByNodeName finds the Node object keyed by name.. Returns
// nil if it cannot be found. A DeepCopy() of the object is returned
// on success.
func (c *machineController) findNodeByNodeName(name string) (*corev1.Node, error) {
	item, exists, err := c.nodeInformer.GetIndexer().GetByKey(name)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	node, ok := item.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("internal error; unexpected type %T", item)
	}

	return node.DeepCopy(), nil
}
