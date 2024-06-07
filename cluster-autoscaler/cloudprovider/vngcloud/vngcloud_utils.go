package vngcloud

import (
	"fmt"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
	"os"
	"strconv"
	lstr "strings"
	"sync"
)

const (
	cpuKey          = "capacity.cluster-autoscaler.kubernetes.io/cpu"
	memoryKey       = "capacity.cluster-autoscaler.kubernetes.io/memory"
	diskCapacityKey = "capacity.cluster-autoscaler.kubernetes.io/ephemeral-disk"
	gpuTypeKey      = "capacity.cluster-autoscaler.kubernetes.io/gpu-type"
	gpuCountKey     = "capacity.cluster-autoscaler.kubernetes.io/gpu-count"
	maxPodsKey      = "capacity.cluster-autoscaler.kubernetes.io/maxPods"
	taintsKey       = "capacity.cluster-autoscaler.kubernetes.io/taints"
	labelsKey       = "capacity.cluster-autoscaler.kubernetes.io/labels"

	UnknownArch SystemArchitecture = ""
	// Amd64 is used if the Architecture is x86_64
	Amd64 SystemArchitecture = "amd64"
	// Arm64 is used if the Architecture is ARM64
	Arm64 SystemArchitecture = "arm64"
	// Ppc64le is used if the Architecture is ppc64le
	Ppc64le SystemArchitecture = "ppc64le"
	// S390x is used if the Architecture is s390x
	S390x SystemArchitecture = "s390x"
	// DefaultArch should be used as a fallback if not passed by the environment via the --scale-up-from-zero-default-arch
	DefaultArch = Amd64
)

// SystemArchitecture represents a CPU architecture (e.g., amd64, arm64, ppc64le, s390x).
// It is used to determine the default architecture to use when building the nodes templates for scaling up from zero
// by some cloud providers. This code is the same as the GCE implementation at
// https://github.com/kubernetes/autoscaler/blob/3852f352d96b8763292a9122163c1152dfedec55/cluster-autoscaler/cloudprovider/gce/templates.go#L611-L657
// which is kept to allow for a smooth transition to this package, once the GCE team is ready to use it.
type SystemArchitecture string

var (
	// clusterNameLabel is the label applied to objects(Machine, MachineSet, MachineDeployment)
	// to identify which cluster they are owned by. Because the label can be
	// affected by the CAPI_GROUP environment variable, it is initialized here.
	clusterNameLabel = getClusterNameLabel()

	// errMissingMinAnnotation is the error returned when a
	// machine set does not have an annotation keyed by
	// nodeGroupMinSizeAnnotationKey.
	errMissingMinAnnotation = errors.New("missing min annotation")

	// errMissingMaxAnnotation is the error returned when a
	// machine set does not have an annotation keyed by
	// nodeGroupMaxSizeAnnotationKey.
	errMissingMaxAnnotation = errors.New("missing max annotation")

	// errInvalidMinAnnotationValue is the error returned when a
	// machine set has a non-integral min annotation value.
	errInvalidMinAnnotation = errors.New("invalid min annotation")

	// errInvalidMaxAnnotationValue is the error returned when a
	// machine set has a non-integral max annotation value.
	errInvalidMaxAnnotation = errors.New("invalid max annotation")

	// nodeGroupMinSizeAnnotationKey and nodeGroupMaxSizeAnnotationKey are the keys
	// used in MachineSet and MachineDeployment annotations to specify the limits
	// for the node group. Because the keys can be affected by the CAPI_GROUP env
	// variable, they are initialized here.
	nodeGroupMinSizeAnnotationKey = getNodeGroupMinSizeAnnotationKey()
	nodeGroupMaxSizeAnnotationKey = getNodeGroupMaxSizeAnnotationKey()
	machineAnnotationKey          = getMachineAnnotationKey()
	zeroQuantity                  = resource.MustParse("0")

	// machineDeleteAnnotationKey is the annotation used by cluster-api to indicate
	// that a machine should be deleted. Because this key can be affected by the
	// CAPI_GROUP env variable, it is initialized here.
	machineDeleteAnnotationKey = getMachineDeleteAnnotationKey()
	once                       sync.Once
	systemArchitecture         *SystemArchitecture
)

const (
	// scaleUpFromZeroDefaultEnvVar is the name of the env var for the default architecture
	scaleUpFromZeroDefaultArchEnvVar = "CAPI_SCALE_ZERO_DEFAULT_ARCH"
)

type normalizedProviderID string

func normalizedProviderString(s string) normalizedProviderID {
	if lstr.HasPrefix(s, "azure://") && lstr.Contains(s, "virtualMachineScaleSets") {
		return normalizedProviderID(s)
	}
	split := lstr.Split(s, "/")
	return normalizedProviderID(split[len(split)-1])
}

func clusterNameFromResource(r *unstructured.Unstructured) string {
	// Use Spec.ClusterName if defined (only available on v1alpha3+ types)
	clusterName, found, err := unstructured.NestedString(r.Object, "spec", "clusterName")
	if err != nil {
		return ""
	}

	if found {
		return clusterName
	}

	// Fallback to value of clusterNameLabel
	if clusterName, ok := r.GetLabels()[clusterNameLabel]; ok {
		return clusterName
	}

	return ""
}

// getClusterNameLabel returns the key that is used by cluster-api for labeling
// which cluster an object belongs to. This function is needed because the user can change
// the default group name by using the CAPI_GROUP environment variable.
func getClusterNameLabel() string {
	key := fmt.Sprintf("%s/cluster-name", getCAPIGroup())
	return key
}

func parseScalingBounds(annotations map[string]string) (int, int, error) {
	minSize, err := minSize(annotations)
	if err != nil && err != errMissingMinAnnotation {
		return 0, 0, err
	}

	if minSize < 0 {
		return 0, 0, errInvalidMinAnnotation
	}

	maxSize, err := maxSize(annotations)
	if err != nil && err != errMissingMaxAnnotation {
		return 0, 0, err
	}

	if maxSize < 0 {
		return 0, 0, errInvalidMaxAnnotation
	}

	if maxSize < minSize {
		return 0, 0, errInvalidMaxAnnotation
	}

	return minSize, maxSize, nil
}

// minSize returns the minimum value encoded in the annotations keyed
// by nodeGroupMinSizeAnnotationKey. Returns errMissingMinAnnotation
// if the annotation doesn't exist or errInvalidMinAnnotation if the
// value is not of type int.
func minSize(annotations map[string]string) (int, error) {
	val, found := annotations[nodeGroupMinSizeAnnotationKey]
	if !found {
		return 0, errMissingMinAnnotation
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, errors.Wrapf(err, "%s", errInvalidMinAnnotation)
	}
	return i, nil
}

// maxSize returns the maximum value encoded in the annotations keyed
// by nodeGroupMaxSizeAnnotationKey. Returns errMissingMaxAnnotation
// if the annotation doesn't exist or errInvalidMaxAnnotation if the
// value is not of type int.
func maxSize(annotations map[string]string) (int, error) {
	val, found := annotations[nodeGroupMaxSizeAnnotationKey]
	if !found {
		return 0, errMissingMaxAnnotation
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, errors.Wrapf(err, "%s", errInvalidMaxAnnotation)
	}
	return i, nil
}

// getNodeGroupMinSizeAnnotationKey returns the key that is used for the
// node group minimum size annotation. This function is needed because the user can
// change the default group name by using the CAPI_GROUP environment variable.
func getNodeGroupMinSizeAnnotationKey() string {
	key := fmt.Sprintf("%s/cluster-api-autoscaler-node-group-min-size", getCAPIGroup())
	return key
}

// getNodeGroupMaxSizeAnnotationKey returns the key that is used for the
// node group maximum size annotation. This function is needed because the user can
// change the default group name by using the CAPI_GROUP environment variable.
func getNodeGroupMaxSizeAnnotationKey() string {
	key := fmt.Sprintf("%s/cluster-api-autoscaler-node-group-max-size", getCAPIGroup())
	return key
}

func parseCPUCapacity(annotations map[string]string) (resource.Quantity, error) {
	return parseKey(annotations, cpuKey)
}

func parseMemoryCapacity(annotations map[string]string) (resource.Quantity, error) {
	return parseKey(annotations, memoryKey)
}

func parseEphemeralDiskCapacity(annotations map[string]string) (resource.Quantity, error) {
	return parseKey(annotations, diskCapacityKey)
}

func parseGPUCount(annotations map[string]string) (resource.Quantity, error) {
	return parseIntKey(annotations, gpuCountKey)
}

func parseKey(annotations map[string]string, key string) (resource.Quantity, error) {
	if val, exists := annotations[key]; exists && val != "" {
		return resource.ParseQuantity(val)
	}
	return zeroQuantity.DeepCopy(), nil
}

func parseMaxPodsCapacity(annotations map[string]string) (resource.Quantity, error) {
	return parseIntKey(annotations, maxPodsKey)
}

func parseIntKey(annotations map[string]string, key string) (resource.Quantity, error) {
	if val, exists := annotations[key]; exists && val != "" {
		valInt, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return zeroQuantity.DeepCopy(), fmt.Errorf("value %q from annotation %q expected to be an integer: %v", val, key, err)
		}
		return *resource.NewQuantity(valInt, resource.DecimalSI), nil
	}
	return zeroQuantity.DeepCopy(), nil
}

// The GPU type is not currently considered by the autoscaler when planning
// expansion, but most likely will be in the future. This method is being added
// in expectation of that arrival.
// see https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/utils/gpu/gpu.go
func parseGPUType(annotations map[string]string) string {
	if val, found := annotations[gpuTypeKey]; found {
		return val
	}
	return ""
}

func machineSetHasMachineDeploymentOwnerRef(machineSet *unstructured.Unstructured) bool {
	return machineSetOwnerRef(machineSet) != nil
}

func machineSetOwnerRef(machineSet *unstructured.Unstructured) *metav1.OwnerReference {
	return getOwnerForKind(machineSet, machineDeploymentKind)
}

func getOwnerForKind(u *unstructured.Unstructured, kind string) *metav1.OwnerReference {
	if u != nil {
		for _, ref := range u.GetOwnerReferences() {
			if ref.Kind == kind && ref.Name != "" {
				return ref.DeepCopy()
			}
		}
	}

	return nil
}

func machineOwnerRef(machine *unstructured.Unstructured) *metav1.OwnerReference {
	return getOwnerForKind(machine, machineSetKind)
}

// getMachineDeleteAnnotationKey returns the key that is used by cluster-api for marking
// machines to be deleted. This function is needed because the user can change the default
// group name by using the CAPI_GROUP environment variable.
func getMachineDeleteAnnotationKey() string {
	key := fmt.Sprintf("%s/delete-machine", getCAPIGroup())
	return key
}

// SystemArchitectureFromString parses a string to SystemArchitecture. Returns UnknownArch if the string doesn't represent a
// valid architecture.
func SystemArchitectureFromString(arch string) SystemArchitecture {
	switch arch {
	case string(Arm64):
		return Arm64
	case string(Amd64):
		return Amd64
	case string(Ppc64le):
		return Ppc64le
	case string(S390x):
		return S390x
	default:
		return UnknownArch
	}
}

// GetDefaultScaleFromZeroArchitecture returns the SystemArchitecture from the environment variable
// CAPI_SCALE_ZERO_DEFAULT_ARCH or DefaultArch if the variable is set to an invalid value.
func GetDefaultScaleFromZeroArchitecture() SystemArchitecture {
	once.Do(func() {
		archStr := os.Getenv(scaleUpFromZeroDefaultArchEnvVar)
		arch := SystemArchitectureFromString(archStr)
		klog.V(5).Infof("the default scale from zero architecture value is set to %s (%s)", archStr, arch.Name())
		if arch == UnknownArch {
			arch = DefaultArch
			klog.Errorf("Unrecognized architecture '%s', falling back to %s",
				scaleUpFromZeroDefaultArchEnvVar, DefaultArch.Name())
		}
		systemArchitecture = &arch
	})
	return *systemArchitecture
}

// Name returns the string value for SystemArchitecture
func (s SystemArchitecture) Name() string {
	return string(s)
}

// getMachineAnnotationKey returns the key that is used by cluster-api for annotating
// nodes with their related machine objects. This function is needed because the user can change
// the default group name by using the CAPI_GROUP environment variable.
func getMachineAnnotationKey() string {
	key := fmt.Sprintf("%s/machine", getCAPIGroup())
	return key
}
