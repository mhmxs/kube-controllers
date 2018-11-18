// Copyright (c) 2017 Tigera, Inc. All rights reserved.
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
	"context"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/projectcalico/kube-controllers/pkg/controllers/controller"
	apiv3 "github.com/projectcalico/libcalico-go/lib/apis/v3"
	bapi "github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	"github.com/projectcalico/libcalico-go/lib/backend/watchersyncer"
	client "github.com/projectcalico/libcalico-go/lib/clientv3"
	"github.com/projectcalico/libcalico-go/lib/errors"
	"github.com/projectcalico/libcalico-go/lib/options"
)

type ctrl struct {
	inSync      bool
	cc          client.Interface
	syncer      bapi.Syncer
	nodes       map[string]*apiv3.Node
	clusters    map[string]string
	topologyKey string
}

func NewController(ctx context.Context, c client.Interface) controller.Controller {
	rrc := &ctrl{
		cc:          c,
		nodes:       make(map[string]*apiv3.Node),
		clusters:    make(map[string]string),
		topologyKey: os.Getenv("TOPOLOGY_KEY"),
	}
	rrc.initSyncer()
	return rrc
}

func (c *ctrl) initSyncer() {
	// TODO: Also watch BGP peers so we can correct if necessary?
	resourceTypes := []watchersyncer.ResourceType{
		{
			ListInterface: model.ResourceListOptions{Kind: apiv3.KindNode},
		},
	}
	type accessor interface {
		Backend() bapi.Client
	}
	c.syncer = watchersyncer.New(c.cc.(accessor).Backend(), resourceTypes, c)
}

func (c *ctrl) OnStatusUpdated(status bapi.SyncStatus) {
	log.Infof("RR syncer status updated: %s", status)
	if status == bapi.InSync {
		log.Infof("In sync with data store, perform reconciliation")
		c.inSync = true
		c.initClusterMap()
		c.reconcile()
	}
}

func (c *ctrl) OnUpdates(updates []bapi.Update) {
	// Update local cache.
	log.Debug("RR syncer received updates: %#v", updates)
	for _, upd := range updates {
		switch upd.UpdateType {
		case bapi.UpdateTypeKVNew:
			log.Debug("New node")
			fallthrough
		case bapi.UpdateTypeKVUpdated:
			log.Debug("Node updated")
			// TODO: For some reason, syncer doesn't give revision on the KVPair.
			// So, we need to set it here.
			n := upd.KVPair.Value.(*apiv3.Node)
			n.ResourceVersion = upd.Revision
			c.nodes[c.keyForUpdate(upd)] = n
		case bapi.UpdateTypeKVDeleted:
			log.Debug("Node deleted")
			delete(c.nodes, c.keyForUpdate(upd))
		default:
			log.Errorf("Unhandled update type")
		}
	}
	log.Debug("Node cache: %#v", c.nodes)

	// Then, reconcile RR state if we're in sync.
	if !c.inSync {
		log.Infof("Not yet in sync - wait for reconciliation")
		return
	}
	c.reconcile()
}

func (c *ctrl) keyForUpdate(u bapi.Update) string {
	return u.Key.String()
}

func (c *ctrl) reconcile() {
	// Go through each zone and make sure the desired number of RR nodes
	// are configured.
	log.Info("Performing a reconciliation")
	for _, zone := range c.zones() {
		logc := log.WithField("zone", zone)
		logc.Info("Reconciling topology zone")
		for {
			// Find out how many route reflectors are in the cluster.
			// Compare to desired state. TODO: Get this dynamically.
			rrs, nodes := c.nodeState(zone)
			desired := c.numDesiredRRs(zone)
			logc.Infof("Current RRs: %d, Desired: %d", len(rrs), desired)
			if len(rrs) < desired {
				// We want more RRs. Loop through the number we need.
				chosen := c.randomNode(nodes)
				if chosen == "" {
					logc.Warnf("No more nodes available for RR")
					break
				}
				if err := c.ensureRouteReflector(chosen); err != nil {
					logc.WithError(err).Errorf("Failed to claim RR")
				}
			} else if len(rrs) > desired {
				// We have too many RRs.
				logc.Warnf("More RRs than desired - removing one")
				chosen := c.randomNode(rrs)
				if err := c.releaseRouteReflector(chosen); err != nil {
					logc.WithError(err).Errorf("Failed to relese RR")
				}
			} else {
				// We have just the right amount. Make sure the topology information is right for each,
				// and fix it if we need to.
				for _, n := range rrs {
					cid := c.clusters[zone]
					if cid != c.nodes[n].Spec.BGP.RouteReflectorClusterID {
						logc.WithField("node", n).Warnf("Node has wrong cluster ID, correcting")
						if err := c.ensureRouteReflector(n); err != nil {
							logc.WithField("node", n).WithError(err).Errorf("Failed to correct RR cluster ID")
							break
						}
					}
				}
				logc.Info("Achieved desired RR state for zone")
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		// Make sure BGP peers are configured correctly for the given configuration.
		// For each RR cluster, configure a BGP peer that peers them together, and a
		// BGP peer that peers nodes in that topology section to peer with the cluster.
		log.Infof("Current RR clusters: %#v", c.clusters)
		cid := c.clusters[zone]
		if err := c.setupPeersForCluster(zone, cid); err != nil {
			log.WithError(err).Errorf("Failed to ensure BGP peers for cluster")
		}
	}

	// Disable node-to-node mesh.
	if err := c.disableNodeMesh(); err != nil {
		log.WithError(err).Errorf("Failed to disable node to node mesh")
	}
}

// Returns a list of all the topology zones that are currently active
// within the cluster based on the configured topology key.
func (c *ctrl) zones() []string {
	exists := make(map[string]string)
	zones := []string{}
	for _, n := range c.nodes {
		z, err := c.topologyZoneForNode(n)
		if err != nil {
			continue
		}
		if _, ok := exists[z]; !ok {
			exists[z] = ""
			zones = append(zones, z)
		}
	}
	log.Infof("Active topology zones: %s", zones)
	return zones
}

func (c *ctrl) nodesInZone(zone string) []string {
	matches := []string{}
	for k, n := range c.nodes {
		if nz, err := c.topologyZoneForNode(n); err == nil && nz == zone {
			matches = append(matches, k)
		}
	}
	return matches
}

func (c *ctrl) numDesiredRRs(zone string) int {
	nodeCount := len(c.nodesInZone(zone))

	// We want 2 RR for every 50 nodes, and no RRs for clusters
	// under 20 nodes.
	if nodeCount < 2 {
		return 0
	} else if nodeCount < 3 {
		return 1
	}
	return (nodeCount / 3) * 2
}

func (c *ctrl) disableNodeMesh() error {
	cfg := apiv3.NewBGPConfiguration()
	f := false
	cfg.Name = "default"
	cfg.Spec.NodeToNodeMeshEnabled = &f
	_, err := c.cc.BGPConfigurations().Create(context.TODO(), cfg, options.SetOptions{})
	if _, ok := err.(errors.ErrorResourceAlreadyExists); ok {
		var ex *apiv3.BGPConfiguration
		ex, err = c.cc.BGPConfigurations().Get(context.TODO(), cfg.Name, options.GetOptions{})
		if err != nil {
			return err
		}
		ex.Spec.NodeToNodeMeshEnabled = &f
		_, err = c.cc.BGPConfigurations().Update(context.TODO(), ex, options.SetOptions{})
		log.WithError(err).Debug("Updated default BGP configuration")
	}
	return err
}

func (c *ctrl) setupPeersForCluster(section, cid string) error {
	// Create a peer which meshes the RRs in this section, and also
	// peers any nodes in this section to the RRs.
	rrp := apiv3.NewBGPPeer()
	peerSelector := "has(projectcalico.org/routeReflectorClusterID)"
	nodeSelector := "all()"
	name := "rr-mesh"
	if section != "" {
		name = fmt.Sprintf("rr-mesh-%s", section)
		peerSelector = fmt.Sprintf("projectcalico.org/routeReflectorClusterID == '%s'", cid)
		nodeSelector = fmt.Sprintf("%s == '%s'", c.topologyKey, section)
	}
	rrp.Name = name
	rrp.Spec = apiv3.BGPPeerSpec{
		NodeSelector: nodeSelector,
		PeerSelector: peerSelector,
	}

	// Program it.
	var err error
	rrp, err = c.ensureBGPPeer(rrp)
	if err != nil {
		return err
	}
	return nil
}

func (c *ctrl) ensureBGPPeer(p *apiv3.BGPPeer) (*apiv3.BGPPeer, error) {
	log.Infof("Ensuring BGP peer exists: %s", p.Name)
	new, err := c.cc.BGPPeers().Create(context.TODO(), p, options.SetOptions{})
	if _, ok := err.(errors.ErrorResourceAlreadyExists); ok {
		var ex *apiv3.BGPPeer
		ex, err = c.cc.BGPPeers().Get(context.TODO(), p.Name, options.GetOptions{})
		if err != nil {
			return nil, err
		}
		p.ResourceVersion = ex.ResourceVersion
		new, err = c.cc.BGPPeers().Update(context.TODO(), p, options.SetOptions{})
		log.WithError(err).Debug("Updated existing peer")
	}
	return new, err
}

var clusterIDFormat string = "224.0.0.%d"

// Makes a route reflector cluster and returns its ID. If an ID is already assigned
// to this zone, then use that instead.
func (c *ctrl) clusterIDForZone(zone string) string {
	if _, ok := c.clusters[zone]; !ok {
		// Make a new one. TODO: Support more than 255 zones.
		for i := 0; i < 255; i++ {
			cid := fmt.Sprintf(clusterIDFormat, i)
			if _, ok := c.cidInUse(cid); !ok {
				c.clusters[zone] = cid
				break
			}
		}
	}
	return c.clusters[zone]
}

// Build a mapping of toplogy zones to cluster IDs based on the running state of the cluster.
func (c *ctrl) initClusterMap() {
	for _, node := range c.nodes {
		if cid, ok := node.Labels["projectcalico.org/routeReflectorClusterID"]; ok {
			zone, err := c.topologyZoneForNode(node)
			if err != nil {
				log.WithError(err).Errorf("Failed to get topology section for node")
				continue
			}

			// Check if we already know about this cluster ID. It's possible when changing the
			// topology key that we end up with duplicate CIDs across multiple zones. In this case,
			// we'll need to assign a new one to some zones. We'll assign a new CID as part of
			// the reconcile loop.
			if z, ok := c.cidInUse(cid); !ok {
				c.clusters[zone] = cid
			} else if z != zone {
				log.Warnf("ClusterID '%s' in use across multiple zones. Will fix.", cid)
			}
		}
	}
	log.Infof("Initial RR clusters: %#v", c.clusters)
}

func (c *ctrl) cidInUse(cid string) (string, bool) {
	for z, c := range c.clusters {
		if c == cid {
			return z, true
		}
	}
	return "", false
}

func (c *ctrl) clusterIDForNode(node *apiv3.Node) (cid string, err error) {
	var zone string
	zone, err = c.topologyZoneForNode(node)
	if err != nil {
		return
	}
	cid = c.clusterIDForZone(zone)
	return
}

func (c *ctrl) topologyZoneForNode(node *apiv3.Node) (topo string, err error) {
	if c.topologyKey != "" {
		var ok bool
		if topo, ok = node.Labels[c.topologyKey]; !ok {
			// A topology key is given and this node is not within any topology group.
			err = fmt.Errorf("Node cannot be located within the given topology schema")
			return
		}
		// A topology key is given and this node falls into a particular
		// topology group.
	}
	return
}

func (c *ctrl) ensureRouteReflector(n string) error {
	log.Infof("Ensuring node is a RR: %s", n)
	node, ok := c.nodes[n]
	if !ok {
		return fmt.Errorf("Node %s no longer exists", n)
	}

	cid, err := c.clusterIDForNode(node)
	if err != nil {
		return err
	}

	// Update it to be a RR.
	node.Spec.BGP.RouteReflectorClusterID = cid
	if node.Labels == nil {
		node.Labels = make(map[string]string, 1)
	}
	node.Labels["projectcalico.org/routeReflectorClusterID"] = cid
	new, err := c.cc.Nodes().Update(context.TODO(), node, options.SetOptions{})
	if err != nil {
		return err
	}
	c.nodes[n] = new
	return nil
}

func (c *ctrl) releaseRouteReflector(n string) error {
	log.Infof("Releasing RR node: %s", n)
	node, ok := c.nodes[n]
	if !ok {
		return fmt.Errorf("Node %s no longer exists", n)
	}

	// Update it to no longer be a RR.
	node.Spec.BGP.RouteReflectorClusterID = ""
	delete(node.Labels, "projectcalico.org/routeReflectorClusterID")
	new, err := c.cc.Nodes().Update(context.TODO(), node, options.SetOptions{})
	if err != nil {
		return err
	}
	c.nodes[n] = new
	return nil
}

func (c *ctrl) nodeState(zone string) (rrs []string, nodes []string) {
	for k, n := range c.nodes {
		if nz, err := c.topologyZoneForNode(n); err != nil || nz != zone {
			// Skip any nodes not in the provided zone.
			continue
		}
		if n.Spec.BGP != nil && n.Spec.BGP.RouteReflectorClusterID != "" {
			rrs = append(rrs, k)
		} else if n.Spec.BGP != nil {
			// We only care about nodes running BGP.
			nodes = append(nodes, k)
		}
	}
	return
}

// Returns a random node from the given set of keys.
func (c *ctrl) randomNode(from []string) string {
	if len(from) > 0 {
		return from[0]
	}
	return ""
}

func (c *ctrl) Run(_ int, _ string, stopCh chan struct{}) {
	// Start the syncer.
	go c.syncer.Start()

	<-stopCh
	log.Info("Stopping route reflector controller")
}
