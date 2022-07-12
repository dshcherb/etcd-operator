import json
import socket
import uuid

from ops.framework import Object, StoredState, ObjectEvents, EventBase, EventSource
from urllib.parse import urlparse
from tenacity import retry, retry_if_exception_type


class ClusterChanged(EventBase):
    """An event raised by EtcdCluster when cluster-wide settings change.
    """


class EtcdClusterEvents(ObjectEvents):
    cluster_changed = EventSource(ClusterChanged)


class EtcdCluster(Object):

    on = EtcdClusterEvents()
    _stored = StoredState()

    CLIENT_PORT = 2379
    CLUSTER_PORT = 2380

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)

        self.framework.observe(charm.on[relation_name].relation_created, self._on_created)
        self.framework.observe(charm.on[relation_name].relation_changed, self._on_changed)
        self.framework.observe(charm.on[relation_name].relation_departed, self._on_departed)

        self._cluster_address = None
        self._unit_fqdn = None
        self._unit_cluster_name = None

        self._container = self.model.unit.get_container(charm.ETCD_SERVICE)

        self._stored.set_default(initial_cluster_state='new')

    def _on_created(self, event):
        self._notify_cluster_changed()

    def _on_changed(self, event):
        self._notify_cluster_changed()

    def _on_departed(self, event):
        # The check for planned units has to do with the case of complete removal of all
        # units of an application - in this case we do not need to remove members from
        # the cluster as all of it will be gone.
        if self.model.unit.is_leader() and self.model.app.planned_units() > 0:
            # If you can no longer connect to pebble, it is likely that a full
            # application removal is in progress so we should skip attempting member
            # removal altogether.
            if not self._container.can_connect():
                return
            self.remove_member(event.unit.name.replace('/', '-'))

    def _notify_cluster_changed(self):
        self.on.cluster_changed.emit()

    @property
    def cluster_address(self):
        if self._cluster_address is None:
            self._cluster_address = self.model.get_binding(
                self._relation_name).network.bind_address
        return self._cluster_address

    @property
    @retry(retry=retry_if_exception_type(socket.gaierror))
    def unit_fqdn(self):
        if self._unit_fqdn is None:
            addr = self.model.get_binding(self._relation_name).network.bind_address
            self._unit_fqdn, _ = socket.getnameinfo((str(addr), 0), socket.NI_NAMEREQD)
        return self._unit_fqdn

    @property
    def cluster_token(self):
        return self._relation.data[self.model.app].get('cluster-token')

    @property
    def initial_cluster_state(self):
        return self._stored.initial_cluster_state

    @property
    def is_established(self):
        return self._relation is not None

    def is_ready_to_bootstrap(self):
        return len(self.peer_cluster_urls) >= self.model.app.planned_units()

    def is_cluster_token_exposed(self):
        return self.cluster_token is not None

    def maybe_expose_cluster_token(self) -> bool:
        """Expose a unique cluster token used during the cluster bootstrap.

        This token will be used during bootstrap to identify the cluster and
        the peer app relation data update will serve as an indicator for non-leader
        units that it is a time to start attempting to form a cluster.

        It cannot be changed after the initial bootstrap so it will not be changed
        if it has already been exposed.

        Likewise, if the cluster is not yet ready to bootstrap (the amount of units
        that exposed their peer URLs has not reached the amount of planned units in
        the model at the time of a check), the cluster token will not be exposed.
        """
        if not self.model.unit.is_leader():
            raise RuntimeError('only a leader can set a cluster token')
        if self.is_cluster_token_exposed():
            return False

        if self.is_ready_to_bootstrap():
            token = uuid.uuid4()
            self._relation.data[self.model.app]['cluster-token'] = str(token)
            return True
        else:
            return False

    @property
    def unit_cluster_name(self):
        if self._unit_cluster_name is None:
            self._unit_cluster_name = self.model.unit.name.replace('/', '-')
        return self._unit_cluster_name

    def _get_peer_cluster_urls(self):
        peer_urls = dict()
        for u in self._relation.units:
            url = self._relation.data[u].get('peer-cluster-url')
            if url:
                peer_urls[u.name.replace('/', '-')] = url

        peer_urls[self.model.unit.name.replace('/', '-')] = self._relation.data[
            self.model.unit]['peer-cluster-url']
        return peer_urls

    def _get_peer_listen_urls(self):
        peer_urls = dict()
        for u in self._relation.units:
            url = self._relation.data[u].get('peer-listen-url')
            if url:
                peer_urls[u.name.replace('/', '-')] = url

        peer_urls[self.model.unit.name.replace('/', '-')] = self._relation.data[
            self.model.unit]['peer-listen-url']
        return peer_urls

    def _get_current_members(self):
        # TODO: error handling
        # Use all client endpoints here to avoid the case when the local unit is not running and
        # is not able to provide a member list. For example, this may happen when a pod with
        # associated Juju leadership gets recreated but Juju leadership does not change.
        p = self._container.exec(command=['etcdctl', '--write-out=json', '--endpoints',
                                          ','.join(self._get_peer_listen_urls().values()),
                                          'member', 'list'])
        members = json.loads(p.wait_output()[0])['members']
        member_name_id = dict()
        for m in members:
            # TODO: check why this is happening:
            # member_names.add(m['name'])
            # When new members are added as unstarted and their name is empty as opposed to
            # being set to the name they were added with. But we can extract a name from the
            # FQDN.
            member_name = urlparse(m['peerURLs'][0]).hostname.split('.')[0]
            member_name_id[member_name] = m['ID']
        return member_name_id

    def update_peer_urls(self):
        """Update a list of peer URLs to be used for cluster bootstrap.
        A leader unit would do it based on what was discovered.
        """
        if not self.framework.model.unit.is_leader():
            raise RuntimeError('cannot update peer URLs from a non-leader unit')

        # TODO: get a set of peer URLs that are present on the peer relation
        # find the ones that are actually members of the etcd cluster - if they
        # are not, add them first (maybe as learners) via etcdctl and then expose
        # them via the peer relation so that the other
        # TODO: maybe maintain a list of initial peer URLs and current peer URLs
        # that need to be used for addition to the cluster.
        app_data = self._relation.data[self.model.app]
        if not self.is_cluster_token_exposed():
            # Static initial bootstrap - need to expose all peer URLs that are ready.
            app_data['peer-cluster-urls'] = json.dumps(self._get_peer_cluster_urls())
        else:
            current_members = set(self._get_current_members())
            # Get the peer URLs that we see on the peer relation at this point.
            peer_urls = self._get_peer_cluster_urls()
            visible_peers = set(peer_urls)
            # Find out which member names are not part of the cluster just yet.
            new_members = visible_peers.difference(current_members)
            # Add new members via the member API - this is mandatory in case of an existing
            # cluster compared to the initial bootstrap.
            for name in new_members:
                self.add_member(name, peer_urls[name])

            app_data = self._relation.data[self.model.app]
            app_data['peer-cluster-urls'] = json.dumps(self._get_peer_cluster_urls())
            # TODO: need to avoid unnecessary restarts of etcd members that are part of the
            # cluster and are not themselves being added - there is no reason for those units
            # to restart without a meaningful config change (non initial-cluster prefixed).
            # You also need a way to reload config without restarting.
            # Effectively, if initial_cluster_state is set to "existing" and the etcd service
            # is inactive, you can start it. Otherwise don't restart it if only initial-cluster
            # config changes happened.

            # TODO: need to notify each member somehow that they are now considered to be a part
            # of the cluster. You can possibly do it via peer-urls which should unblock them from
            # starting up. However, they need to somehow understand that they need to use
            # initial-cluster-state="existing" instead of "new". Potentially

    def add_member(self, name, peer_cluster_url):
        # Multiple endpoints are supplied to avoid the case when a leader becomes a unit that's
        # not local to an existing cluster member and so it won't be able to change the list of
        # members. Instead you can let etcdctl figure out which endpoint to join. That way a
        # leader unit may not be the one with a cluster member but things will still work as
        # expected. This is relevant for the scaledown case as well.
        p = self._container.exec(command=['etcdctl', 'member', 'add',
                                          name, f'--peer-urls={peer_cluster_url}',
                                          '--endpoints',
                                          ','.join(self._get_peer_listen_urls().values())])
        p.wait()

    def remove_member(self, name):
        member_id = self._get_current_members().get(name)
        if member_id is None:
            return
        # The member ID integer must be formatted as a hex string without a 0x prefix.
        p = self._container.exec(command=['etcdctl', 'member', 'remove',
                                          f'{member_id:x}', '--endpoints',
                                          ','.join(self._get_peer_listen_urls().values())])
        p.wait()

    @property
    def peer_cluster_url(self):
        # Use FQDNs in peer URLs to account for the possibility of pod IPs changing
        # when individual pods of a StatefulSet are recreated (FQDNs are stable but not IPs).
        return f'https://{self.unit_fqdn}:{self.CLUSTER_PORT}'

    @property
    def peer_cluster_listen_url(self):
        return f'https://{str(self.cluster_address)}:{self.CLUSTER_PORT}'

    @property
    def peer_listen_url(self):
        return f'http://{self.unit_fqdn}:{self.CLIENT_PORT}'

    def expose_peer_url(self):
        unit_data = self._relation.data[self.model.unit]
        if unit_data.get('peer-cluster-url') is None and self.is_cluster_token_exposed():
            # TODO: is this the right place to modify this state?
            # The cluster token is already present and the peer URL has never been exposed by
            # this unit which means we were not a part of the initial cluster bootstrap
            # and we need to treat the initial cluster state as "existing".
            self._stored.initial_cluster_state = 'existing'

        self._relation.data[self.model.unit]['peer-cluster-url'] = self.peer_cluster_url
        self._relation.data[self.model.unit]['peer-listen-url'] = self.peer_listen_url

    @property
    def peer_cluster_urls(self):
        """Retrieve peer URLs as seen and exposed via peer app relation data by the leader"""
        if not self.is_established:
            raise RuntimeError('unable to retrieve peer URLs - peer relation does not exist')

        app_data = self._relation.data[self.model.app]
        peer_urls_json = app_data.get('peer-cluster-urls')
        peer_urls = json.loads(peer_urls_json) if peer_urls_json else {}
        return peer_urls

    def is_etcd_ready_to_start(self):
        """Determine whether it is possible to start an etcd daemon from the cluster perspective.

        An etcd daemon should not be started if its peer URL has not been acknowledged by the
        leader unit since the Juju-level leader unit is responsible for adding a new member to
        the cluster.
        """
        # The leader must either ack our unit for initial bootstrap or add it as a member and
        # then ack it post initial bootstrap.
        member_acked = self.peer_cluster_urls.get(self.unit_cluster_name) is not None
        return self.is_cluster_token_exposed() and member_acked
