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

package routereflector

import (
	apiv3 "github.com/projectcalico/libcalico-go/lib/apis/v3"
	bapi "github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
)

type calicoNodeSyncer struct {
	*ctrl
}

func (c *calicoNodeSyncer) OnStatusUpdated(status bapi.SyncStatus) {
	logrus.Debugf("Route reflector controller Calico node syncer status updated: %s", status)

	switch status {
	case bapi.InSync:
		logrus.Info("Route reflector controller Calico nodes are in sync")
		c.ctrl.syncWaitGroup.Done()
	}
}

func (c *calicoNodeSyncer) OnUpdates(updates []bapi.Update) {
	c.ctrl.updateMutex.Lock()
	defer c.ctrl.updateMutex.Unlock()

	// Update local cache.
	logrus.Debug("RR Calico node syncer received updates: %#v", updates)
	for _, upd := range updates {
		switch upd.UpdateType {
		case bapi.UpdateTypeKVNew:
			logrus.Debug("New Calico node")
			fallthrough
		case bapi.UpdateTypeKVUpdated:
			logrus.Debug("Calico node updated")
			// TODO: For some reason, syncer doesn't give revision on the KVPair.
			// So, we need to set it here.
			n := upd.KVPair.Value.(*apiv3.Node)
			n.ResourceVersion = upd.Revision
			c.calicoNodes[n.GetName()] = n
		case bapi.UpdateTypeKVDeleted, bapi.UpdateTypeKVUnknown:
			if upd.KVPair.Value != nil {
				logrus.Warnf("KVPair value should be nil for Deleted UpdataType")
			}
			logrus.Debug("Calico node deleted")
			name := upd.KVPair.Key.(model.ResourceKey).Name
			delete(c.calicoNodes, name)
		default:
			logrus.Errorf("Calico node unhandled update type %d", upd.UpdateType)
		}
	}
	logrus.Debug("Calico node cache: %#v", c.calicoNodes)
}

type bgpPeerSyncer struct {
	*ctrl
}

func (c *bgpPeerSyncer) OnStatusUpdated(status bapi.SyncStatus) {
	logrus.Debugf("Route reflector controller BGP peer syncer status updated: %s", status)

	switch status {
	case bapi.InSync:
		logrus.Info("Route reflector controller BGP peers are in sync")
		c.ctrl.syncWaitGroup.Done()
	}
}

func (c *bgpPeerSyncer) OnUpdates(updates []bapi.Update) {
	c.ctrl.updateMutex.Lock()
	defer c.ctrl.updateMutex.Unlock()

	// Update local cache.
	logrus.Debug("RR BGP peer syncer received updates: %#v", updates)
	for _, upd := range updates {
		switch upd.UpdateType {
		case bapi.UpdateTypeKVNew:
			logrus.Debug("New BGP peer")
			fallthrough
		case bapi.UpdateTypeKVUpdated:
			logrus.Debug("BGP peer updated")
			// TODO: For some reason, syncer doesn't give revision on the KVPair.
			// So, we need to set it here.
			p := upd.KVPair.Value.(*apiv3.BGPPeer)
			p.ResourceVersion = upd.Revision
			c.bgpPeers[p.GetName()] = p
		case bapi.UpdateTypeKVDeleted, bapi.UpdateTypeKVUnknown:
			if upd.KVPair.Value != nil {
				logrus.Warnf("KVPair value should be nil for Deleted UpdataType")
			}
			logrus.Debug("BGP peer deleted")
			name := upd.KVPair.Key.(model.ResourceKey).Name
			delete(c.bgpPeers, name)
		default:
			logrus.Errorf("BGP peer unhandled update type %d", upd.UpdateType)
		}
	}
	logrus.Debug("BGP peer cache: %#v", c.bgpPeers)
}

func (c *ctrl) OnKubeUpdate(oldObj interface{}, newObj interface{}) {
	c.waitForSyncOnce.Do(c.waitForSync)

	c.updateMutex.Lock()
	defer c.updateMutex.Unlock()

	logrus.Debugf("Kube node updated %v", newObj)
	newKubeNode, ok := newObj.(*corev1.Node)
	if !ok {
		logrus.Errorf("Given resource type can't handle %v", newObj)
		return
	}

	c.kubeNodes[newKubeNode.GetUID()] = newKubeNode

	if err := c.update(newKubeNode); err != nil {
		logrus.Errorf("Unable to update Kube node %s because of %s", newKubeNode.GetName(), err)
	}
}

func (c *ctrl) OnKubeDelete(obj interface{}) {
	c.waitForSyncOnce.Do(c.waitForSync)

	c.updateMutex.Lock()
	defer c.updateMutex.Unlock()

	logrus.Debugf("Kube node updated %v", obj)
	kubeNode, ok := obj.(*corev1.Node)
	if !ok {
		logrus.Errorf("Given resource type can't handle %v", obj)
		return
	}

	if err := c.delete(kubeNode); err != nil {
		logrus.Errorf("Unable to delete Kube node %s because of %s", kubeNode.GetName(), err)
	}

	delete(c.kubeNodes, kubeNode.GetUID())
}

func (c *ctrl) waitForSync() {
	logrus.Info("Waiting for sync to calculate rote reflector topology")
	c.syncWaitGroup.Wait()
	logrus.Info("Sync done, time to calculate rote reflector topology")
}
