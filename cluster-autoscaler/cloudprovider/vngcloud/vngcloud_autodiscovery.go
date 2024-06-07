package vngcloud

import (
	lfmt "fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	lstr "strings"

	llabels "k8s.io/apimachinery/pkg/labels"
	lselection "k8s.io/apimachinery/pkg/selection"
	lerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

type vngcloudAutoDiscoveryConfig struct {
	clusterName   string
	namespace     string
	labelSelector llabels.Selector
}

func parseAutoDiscovery(specs []string) ([]*vngcloudAutoDiscoveryConfig, error) {
	result := make([]*vngcloudAutoDiscoveryConfig, 0, len(specs))
	for _, spec := range specs {
		autoDiscoverySpec, err := parseAutoDiscoverySpec(spec)
		if err != nil {
			return result, err
		}
		result = append(result, autoDiscoverySpec)
	}

	return result, nil
}

func parseAutoDiscoverySpec(spec string) (*vngcloudAutoDiscoveryConfig, error) {
	cfg := &vngcloudAutoDiscoveryConfig{
		labelSelector: llabels.NewSelector(),
	}

	tokens := lstr.Split(spec, ":")
	if len(tokens) != 2 {
		return cfg, lerrors.NewAutoscalerError(lerrors.ConfigurationError, lfmt.Sprintf("spec \"%s\" should be discoverer:key=value,key=value", spec))
	}
	discoverer := tokens[0]
	if discoverer != autoDiscovererTypeVngCloud {
		return cfg, lerrors.NewAutoscalerError(lerrors.ConfigurationError, lfmt.Sprintf("unsupported discoverer specified: %s", discoverer))
	}

	for _, arg := range lstr.Split(tokens[1], ",") {
		if len(arg) == 0 {
			continue
		}
		kv := lstr.Split(arg, "=")
		if len(kv) != 2 {
			return cfg, lerrors.NewAutoscalerError(lerrors.ConfigurationError, lfmt.Sprintf("invalid key=value pair %s", kv))
		}
		k, v := kv[0], kv[1]

		switch k {
		case autoDiscovererClusterNameKey:
			cfg.clusterName = v
		case autoDiscovererNamespaceKey:
			cfg.namespace = v
		default:
			req, err := llabels.NewRequirement(k, lselection.Equals, []string{v})
			if err != nil {
				return cfg, lerrors.NewAutoscalerError(lerrors.ConfigurationError, lfmt.Sprintf("failed to create label selector; %v", err))
			}
			cfg.labelSelector = cfg.labelSelector.Add(*req)
		}
	}
	return cfg, nil
}

func namespaceToWatch(specs []*vngcloudAutoDiscoveryConfig) string {
	for _, spec := range specs {
		if spec.namespace != "" {
			return spec.namespace
		}
	}
	return metav1.NamespaceAll
}

func allowedByAutoDiscoverySpec(spec *vngcloudAutoDiscoveryConfig, r *unstructured.Unstructured) bool {
	switch {
	case spec.namespace != "" && spec.namespace != r.GetNamespace():
		return false
	case spec.clusterName != "" && spec.clusterName != clusterNameFromResource(r):
		return false
	case !spec.labelSelector.Matches(llabels.Set(r.GetLabels())):
		return false
	default:
		return true
	}
}
