/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"testing"
)

func TestPodSpecSemanticallyEqual(t *testing.T) {
	projectedSAVol := test.BuildServiceTokenProjectedVolumeSource("path")

	tests := []struct {
		name   string
		p1Spec apiv1.PodSpec
		p2Spec apiv1.PodSpec
		result bool
	}{
		{
			name: "two pods with projected volume sources",
			p1Spec: apiv1.PodSpec{
				Volumes: []apiv1.Volume{
					{Name: "projected1", VolumeSource: apiv1.VolumeSource{Projected: projectedSAVol}},
				},
			},
			p2Spec: apiv1.PodSpec{
				Volumes: []apiv1.Volume{
					{Name: "projected2", VolumeSource: apiv1.VolumeSource{Projected: projectedSAVol}},
				},
			},
			result: true,
		},
		{
			name: "two pods with different volumes",
			p1Spec: apiv1.PodSpec{
				Volumes: []apiv1.Volume{
					{Name: "vol1", VolumeSource: apiv1.VolumeSource{EmptyDir: &apiv1.EmptyDirVolumeSource{Medium: ""}}},
				},
			},
			p2Spec: apiv1.PodSpec{
				Volumes: []apiv1.Volume{
					{Name: "vol2", VolumeSource: apiv1.VolumeSource{EmptyDir: &apiv1.EmptyDirVolumeSource{Medium: ""}}},
				},
			},
			result: false,
		},
		{
			name: "two pod different containers",
			p1Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{Image: "foo/bar", Name: "foobar"},
				},
			},
			p2Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{Image: "foo/baz", Name: "foobaz"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PodSpecSemanticallyEqual(tt.p1Spec, tt.p2Spec)
			assert.Equal(t, tt.result, result)
		})
	}
}

func TestSanitizeProjectedVolumesAndMounts(t *testing.T) {
	projectedSAVol := test.BuildServiceTokenProjectedVolumeSource("path")

	tests := []struct {
		name          string
		inputPodSpec  apiv1.PodSpec
		outputPodSpec apiv1.PodSpec
	}{
		{
			name: "pod spec with only projected volumes",
			inputPodSpec: apiv1.PodSpec{
				NodeSelector: map[string]string{"foo": "bar"},
				Volumes: []apiv1.Volume{
					{Name: "projected1", VolumeSource: apiv1.VolumeSource{Projected: projectedSAVol}},
					{Name: "projected2", VolumeSource: apiv1.VolumeSource{Projected: projectedSAVol}},
				},
				Containers: []apiv1.Container{
					{Image: "foo/bar", Name: "foobar", VolumeMounts: []apiv1.VolumeMount{{Name: "projected1"}}},
					{Image: "foo/baz", Name: "foobaz", VolumeMounts: []apiv1.VolumeMount{{Name: "projected2"}}},
				},
			},
			outputPodSpec: apiv1.PodSpec{
				NodeSelector: map[string]string{"foo": "bar"},
				Containers: []apiv1.Container{
					{Image: "foo/bar", Name: "foobar"},
					{Image: "foo/baz", Name: "foobaz"},
				},
			},
		},
		{
			name: "pod spec with only non-projected volumes",
			inputPodSpec: apiv1.PodSpec{
				NodeSelector: map[string]string{"foo": "bar"},
				Volumes: []apiv1.Volume{
					{Name: "volume-nz94a", VolumeSource: apiv1.VolumeSource{FlexVolume: &apiv1.FlexVolumeSource{Driver: "testDriver"}}},
					{Name: "volume-nz94b", VolumeSource: apiv1.VolumeSource{FlexVolume: &apiv1.FlexVolumeSource{Driver: "testDriver"}}},
				},
				Containers: []apiv1.Container{
					{Image: "foo/bar", Name: "foobar", VolumeMounts: []apiv1.VolumeMount{{Name: "volume-nz94a"}}},
					{Image: "foo/baz", Name: "foo/baz", VolumeMounts: []apiv1.VolumeMount{{Name: "volume-nz94b"}}},
				},
			},
			outputPodSpec: apiv1.PodSpec{
				NodeSelector: map[string]string{"foo": "bar"},
				Volumes: []apiv1.Volume{
					{Name: "volume-nz94a", VolumeSource: apiv1.VolumeSource{FlexVolume: &apiv1.FlexVolumeSource{Driver: "testDriver"}}},
					{Name: "volume-nz94b", VolumeSource: apiv1.VolumeSource{FlexVolume: &apiv1.FlexVolumeSource{Driver: "testDriver"}}},
				},
				Containers: []apiv1.Container{
					{Image: "foo/bar", Name: "foobar", VolumeMounts: []apiv1.VolumeMount{{Name: "volume-nz94a"}}},
					{Image: "foo/baz", Name: "foo/baz", VolumeMounts: []apiv1.VolumeMount{{Name: "volume-nz94b"}}},
				},
			},
		},
		{
			name: "pod spec with a mix of volume types",
			inputPodSpec: apiv1.PodSpec{
				NodeSelector: map[string]string{"foo": "bar"},
				Volumes: []apiv1.Volume{
					{Name: "volume-nz94b", VolumeSource: apiv1.VolumeSource{FlexVolume: &apiv1.FlexVolumeSource{Driver: "testDriver"}}},
					{Name: "kube-api-access-nz94a", VolumeSource: apiv1.VolumeSource{Projected: projectedSAVol}},
					{Name: "projected2", VolumeSource: apiv1.VolumeSource{Projected: projectedSAVol}},
					{Name: "empty-dir", VolumeSource: apiv1.VolumeSource{EmptyDir: &apiv1.EmptyDirVolumeSource{Medium: ""}}},
				},
				Containers: []apiv1.Container{
					{Image: "foo/bar", Name: "foobar", VolumeMounts: []apiv1.VolumeMount{{Name: "kube-api-access-nz94a"}}},
					{Image: "foo/baz", Name: "foo/baz", VolumeMounts: []apiv1.VolumeMount{{Name: "volume-nz94b"}, {Name: "kube-api-access-nz94a"}, {Name: "empty-dir"}, {Name: "projected2"}}},
					{Image: "foo/qux", Name: "foo/qux"},
				},
			},
			outputPodSpec: apiv1.PodSpec{
				NodeSelector: map[string]string{"foo": "bar"},
				Volumes: []apiv1.Volume{
					{Name: "volume-nz94b", VolumeSource: apiv1.VolumeSource{FlexVolume: &apiv1.FlexVolumeSource{Driver: "testDriver"}}},
					{Name: "empty-dir", VolumeSource: apiv1.VolumeSource{EmptyDir: &apiv1.EmptyDirVolumeSource{Medium: ""}}},
				},
				Containers: []apiv1.Container{
					{Image: "foo/bar", Name: "foobar"},
					{Image: "foo/baz", Name: "foo/baz", VolumeMounts: []apiv1.VolumeMount{{Name: "volume-nz94b"}, {Name: "empty-dir"}}},
					{Image: "foo/qux", Name: "foo/qux"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeProjectedVolumesAndMounts(tt.inputPodSpec)
			assert.True(t, assert.ObjectsAreEqualValues(tt.outputPodSpec, got), "\ngot: %#v\nwant: %#v", got, tt.outputPodSpec)
		})
	}
}
