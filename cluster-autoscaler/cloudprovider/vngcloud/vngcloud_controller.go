package vngcloud

import (
	lfmt "fmt"
	llabels "k8s.io/apimachinery/pkg/labels"
	los "os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	lschema "k8s.io/apimachinery/pkg/runtime/schema"
	lcloudprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	ldiscovery "k8s.io/client-go/discovery"
	ldynamic "k8s.io/client-go/dynamic"
	ldynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	linformers "k8s.io/client-go/informers"
	lkubernetes "k8s.io/client-go/kubernetes"
	lscale "k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	machinePoolProviderIDIndex = "machinePoolProviderIDIndex"
	nodeProviderIDIndex        = "nodeProviderIDIndex"
	defaultCAPIGroup           = "cluster.x-k8s.io"
	// CAPIGroupEnvVar contains the environment variable name which allows overriding defaultCAPIGroup.
	CAPIGroupEnvVar = "CAPI_GROUP"
	// CAPIVersionEnvVar contains the environment variable name which allows overriding the Cluster API group version.
	CAPIVersionEnvVar             = "CAPI_VERSION"
	autoDiscovererTypeVngCloud    = "vngcloud"
	autoDiscovererClusterNameKey  = "clusterName"
	autoDiscovererNamespaceKey    = "namespace"
	resourceNameMachineDeployment = "machinedeployments"
	resourceNameMachineSet        = "machinesets"
	resourceNameMachine           = "machines"
	resourceNameMachinePool       = "machinepools"
	machineProviderIDIndex        = "machineProviderIDIndex"
	failedMachinePrefix           = "failed-machine-"
	pendingMachinePrefix          = "pending-machine-"

	machineDeploymentKind = "MachineDeployment"
	machineSetKind        = "MachineSet"
	machinePoolKind       = "MachinePool"
)

func newMachineController(
	pmanagementClient ldynamic.Interface,
	pworkloadClient lkubernetes.Interface,
	managementDiscoveryClient ldiscovery.DiscoveryInterface,
	managementScaleClient lscale.ScalesGetter,
	pdiscoveryOpts lcloudprovider.NodeGroupDiscoveryOptions,
	stopChannel chan struct{}) (*machineController, error) {

	workloadInformerFactory := linformers.NewSharedInformerFactory(pworkloadClient, 0)
	autoDiscoverySpecs, err := parseAutoDiscovery(pdiscoveryOpts.NodeGroupAutoDiscoverySpecs)
	if err != nil {
		return nil, lfmt.Errorf("failed to parse auto discovery configuration: %v", err)
	}

	managementInformerFactory := ldynamicinformer.NewFilteredDynamicSharedInformerFactory(pmanagementClient, 0, namespaceToWatch(autoDiscoverySpecs), nil)

	CAPIGroup := getCAPIGroup()
	CAPIVersion, err := getAPIGroupPreferredVersion(managementDiscoveryClient, CAPIGroup)
	if err != nil {
		return nil, lfmt.Errorf("could not find preferred version for CAPI group %q: %v", CAPIGroup, err)
	}
	klog.Infof("Using version %q for API group %q", CAPIVersion, CAPIGroup)

	var gvrMachineDeployment lschema.GroupVersionResource
	var machineDeploymentInformer linformers.GenericInformer

	machineDeploymentAvailable, err := groupVersionHasResource(managementDiscoveryClient,
		lfmt.Sprintf("%s/%s", CAPIGroup, CAPIVersion), resourceNameMachineDeployment)
	if err != nil {
		return nil, lfmt.Errorf("failed to validate if resource %q is available for group %q: %v",
			resourceNameMachineDeployment, lfmt.Sprintf("%s/%s", CAPIGroup, CAPIVersion), err)
	}

	if machineDeploymentAvailable {
		gvrMachineDeployment = lschema.GroupVersionResource{
			Group:    CAPIGroup,
			Version:  CAPIVersion,
			Resource: resourceNameMachineDeployment,
		}
		machineDeploymentInformer = managementInformerFactory.ForResource(gvrMachineDeployment)
		if _, err := machineDeploymentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{}); err != nil {
			return nil, lfmt.Errorf("failed to add event handler for resource %q: %v", resourceNameMachineDeployment, err)
		}
	}

	gvrMachineSet := lschema.GroupVersionResource{
		Group:    CAPIGroup,
		Version:  CAPIVersion,
		Resource: resourceNameMachineSet,
	}
	machineSetInformer := managementInformerFactory.ForResource(gvrMachineSet)
	if _, err := machineSetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{}); err != nil {
		return nil, lfmt.Errorf("failed to add event handler for resource %q: %v", resourceNameMachineSet, err)
	}
	var gvrMachinePool lschema.GroupVersionResource
	var machinePoolInformer linformers.GenericInformer

	machinePoolsAvailable, err := groupVersionHasResource(managementDiscoveryClient,
		lfmt.Sprintf("%s/%s", CAPIGroup, CAPIVersion), resourceNameMachinePool)
	if err != nil {
		return nil, lfmt.Errorf("failed to validate if resource %q is available for group %q: %v",
			resourceNameMachinePool, lfmt.Sprintf("%s/%s", CAPIGroup, CAPIVersion), err)
	}

	if machinePoolsAvailable {
		gvrMachinePool = lschema.GroupVersionResource{
			Group:    CAPIGroup,
			Version:  CAPIVersion,
			Resource: resourceNameMachinePool,
		}
		machinePoolInformer = managementInformerFactory.ForResource(gvrMachinePool)
		if _, err := machinePoolInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{}); err != nil {
			return nil, lfmt.Errorf("failed to add event handler for resource %q: %w", resourceNameMachinePool, err)
		}

		if err := machinePoolInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{
			machinePoolProviderIDIndex: indexMachinePoolByProviderID,
		}); err != nil {
			return nil, lfmt.Errorf("cannot add machine pool indexer: %v", err)
		}
	}

	gvrMachine := lschema.GroupVersionResource{
		Group:    CAPIGroup,
		Version:  CAPIVersion,
		Resource: resourceNameMachine,
	}
	machineInformer := managementInformerFactory.ForResource(gvrMachine)
	if _, err := machineInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{}); err != nil {
		return nil, lfmt.Errorf("failed to add event handler for resource %q: %v", resourceNameMachine, err)
	}

	nodeInformer := workloadInformerFactory.Core().V1().Nodes().Informer()
	if _, err := nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{}); err != nil {
		return nil, lfmt.Errorf("failed to add event handler for resource %q: %v", "nodes", err)
	}

	if err := machineInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{
		machineProviderIDIndex: indexMachineByProviderID,
	}); err != nil {
		return nil, lfmt.Errorf("cannot add machine indexer: %v", err)
	}

	if err := nodeInformer.GetIndexer().AddIndexers(cache.Indexers{
		nodeProviderIDIndex: indexNodeByProviderID,
	}); err != nil {
		return nil, lfmt.Errorf("cannot add node indexer: %v", err)
	}

	return &machineController{
		autoDiscoverySpecs:          autoDiscoverySpecs,
		workloadInformerFactory:     workloadInformerFactory,
		managementInformerFactory:   managementInformerFactory,
		machineDeploymentInformer:   machineDeploymentInformer,
		machineInformer:             machineInformer,
		machineSetInformer:          machineSetInformer,
		machinePoolInformer:         machinePoolInformer,
		nodeInformer:                nodeInformer,
		managementClient:            pmanagementClient,
		managementScaleClient:       managementScaleClient,
		machineSetResource:          gvrMachineSet,
		machinePoolResource:         gvrMachinePool,
		machinePoolsAvailable:       machinePoolsAvailable,
		machineResource:             gvrMachine,
		machineDeploymentResource:   gvrMachineDeployment,
		machineDeploymentsAvailable: machineDeploymentAvailable,
		stopChannel:                 stopChannel,
	}, nil
}

// getCAPIGroup returns a string that specifies the group for the API.
// It will return either the value from the
// CAPI_GROUP environment variable, or the default value i.e cluster.x-k8s.io.
func getCAPIGroup() string {
	g := los.Getenv(CAPIGroupEnvVar)
	if g == "" {
		g = defaultCAPIGroup
	}
	klog.V(4).Infof("Using API Group %q", g)
	return g
}

func getAPIGroupPreferredVersion(client ldiscovery.DiscoveryInterface, APIGroup string) (string, error) {
	if version := los.Getenv(CAPIVersionEnvVar); version != "" {
		return version, nil
	}

	groupList, err := client.ServerGroups()
	if err != nil {
		return "", lfmt.Errorf("failed to get ServerGroups: %v", err)
	}

	for _, group := range groupList.Groups {
		if group.Name == APIGroup {
			return group.PreferredVersion.Version, nil
		}
	}

	return "", lfmt.Errorf("failed to find API group %q", APIGroup)
}

func groupVersionHasResource(client ldiscovery.DiscoveryInterface, groupVersion, resourceName string) (bool, error) {
	resourceList, err := client.ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		return false, lfmt.Errorf("failed to get ServerGroups: %v", err)
	}

	for _, r := range resourceList.APIResources {
		klog.Infof("Resource %q available", r.Name)
		if r.Name == resourceName {
			return true, nil
		}
	}
	return false, nil
}

func indexMachinePoolByProviderID(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}

	providerIDList, found, err := unstructured.NestedStringSlice(u.UnstructuredContent(), "spec", "providerIDList")
	if err != nil || !found {
		return nil, nil
	}
	if len(providerIDList) == 0 {
		return nil, nil
	}

	normalizedProviderIDs := make([]string, len(providerIDList))

	for i, s := range providerIDList {
		normalizedProviderIDs[i] = string(normalizedProviderString(s))
	}

	return normalizedProviderIDs, nil
}

func indexMachineByProviderID(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}

	providerID, found, err := unstructured.NestedString(u.UnstructuredContent(), "spec", "providerID")
	if err != nil || !found {
		return nil, nil
	}
	if providerID == "" {
		return nil, nil
	}

	return []string{string(normalizedProviderString(providerID))}, nil
}

func indexNodeByProviderID(obj interface{}) ([]string, error) {
	if node, ok := obj.(*corev1.Node); ok {
		if node.Spec.ProviderID != "" {
			return []string{string(normalizedProviderString(node.Spec.ProviderID))}, nil
		}
		return []string{}, nil
	}
	return []string{}, nil
}

func listResources(lister cache.GenericNamespaceLister, clusterName string, selector llabels.Selector) ([]*unstructured.Unstructured, error) {
	objs, err := lister.List(selector)
	if err != nil {
		return nil, err
	}

	results := make([]*unstructured.Unstructured, 0, len(objs))
	for _, x := range objs {
		u, ok := x.(*unstructured.Unstructured)
		if !ok {
			return nil, lfmt.Errorf("expected unstructured resource from lister, not %T", x)
		}

		// if clusterName is not empty and the clusterName does not match the resource, do not return it as part of the results
		if clusterName != "" && clusterNameFromResource(u) != clusterName {
			continue
		}

		// if we are listing MachineSets, do not return MachineSets that are owned by a MachineDeployment
		if u.GetKind() == machineSetKind && machineSetHasMachineDeploymentOwnerRef(u) {
			continue
		}

		results = append(results, u.DeepCopy())
	}

	return results, nil
}

func machineKeyFromPendingMachineProviderID(providerID normalizedProviderID) string {
	namespaceName := strings.TrimPrefix(string(providerID), pendingMachinePrefix)
	return strings.Replace(namespaceName, "_", "/", 1)
}
