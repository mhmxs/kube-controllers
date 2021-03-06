// Copyright (c) 2020 IBM Corporation All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastores

import (
	apiv3 "github.com/projectcalico/libcalico-go/lib/apis/v3"
	corev1 "k8s.io/api/core/v1"
)

const (
	RouteReflectorClusterIDAnnotation = "projectcalico.org/RouteReflectorClusterID"
)

// KddDataStore Kubernetes data store specific functions
type KddDataStore struct {
	nodeInfo nodeInfo
}

// RemoveRRStatus removes RR related labels from node
func (d *KddDataStore) RemoveRRStatus(node *corev1.Node, _ *apiv3.Node) error {
	nodeLabelKey, _ := d.nodeInfo.GetNodeLabel(string(node.GetUID()))
	delete(node.Labels, nodeLabelKey)
	delete(node.Annotations, RouteReflectorClusterIDAnnotation)

	return nil
}

// AddRRStatus adds RR related labels from node
func (d *KddDataStore) AddRRStatus(node *corev1.Node, _ *apiv3.Node) error {
	labelKey, labelValue := d.nodeInfo.GetNodeLabel(string(node.GetUID()))
	node.Labels[labelKey] = labelValue

	// Calculate ClusterID for annotation
	clusterID := d.nodeInfo.GetClusterID(node)
	node.Annotations[RouteReflectorClusterIDAnnotation] = clusterID

	return nil
}

// NewKddDatastore initialise new KDD datastore
func NewKddDatastore(topology nodeInfo) Datastore {
	return &KddDataStore{
		nodeInfo: topology,
	}
}
