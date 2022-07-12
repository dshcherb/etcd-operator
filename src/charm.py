#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

    https://discourse.charmhub.io/t/4208
"""

import logging

from jinja2 import Environment, FileSystemLoader

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, WaitingStatus, ModelError

from cluster import EtcdCluster

logger = logging.getLogger(__name__)


class EtcdOperatorCharm(CharmBase):
    """Charm the service."""

    _stored = StoredState()

    ETCD_CONF_PATH = '/etc/etcd/etcd.conf.yml'
    ETCD_SERVICE = 'etcd'

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.etcd_pebble_ready,
                               self._on_etcd_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.start, self._on_start)

        self._template_env = None

        self.cluster = EtcdCluster(self, 'cluster')
        self.framework.observe(self.cluster.on.cluster_changed, self._on_cluster_changed)

        self._container = self.unit.get_container(
            self.ETCD_SERVICE)

    def _push_template(self, container, template_name, target_path, context={}):
        if self._template_env is None:
            self._template_env = Environment(loader=FileSystemLoader(
                f'{self.charm_dir}/templates'))
        container.push(
            target_path,
            self._template_env.get_template(template_name).render(**context),
            make_dirs=True
        )

    def _apply_config_change(self):
        peer_urls = self.cluster.peer_cluster_urls
        listen_client_urls = [
            # 'unix:///var/run/etcd.sock',
            # f'http://{self.cluster.cluster_address}:2379',
            # 'http://localhost:2379',
            'http://0.0.0.0:2379',
        ]
        self._push_template(self._container, 'etcd.conf.yml.j2', self.ETCD_CONF_PATH, {
            'name': self.unit.name.replace('/', '-'),
            'peer_urls': self.cluster.peer_cluster_url,
            # TODO: this field might change but if a unit is already running you
            # do not need to check. This may only become relevant if you need to
            # add another listen URL. Note that this URL will only change if
            # a pod is recreated at which point you will get an on_start event.
            'listen_peer_url': self.cluster.peer_cluster_listen_url,
            'initial_cluster': ','.join([f'{k}={v}' for k, v in peer_urls.items()]),
            'initial_token': self.cluster.cluster_token,
            'listen_client_urls': ','.join(listen_client_urls),
            # TODO: this field might change but should be ignored if a unit is
            # already running
            'initial_cluster_state': self.cluster.initial_cluster_state,
        })

    def _on_etcd_pebble_ready(self, event):
        """Define and start a workload using the Pebble API.
        """
        container = event.workload
        pebble_layer = {
            'summary': 'etcd layer',
            'description': 'pebble config layer for etcd',
            'services': {
                self.ETCD_SERVICE: {
                    'override': 'replace',
                    'summary': 'etcd',
                    'command': f'etcd --config-file {self.ETCD_CONF_PATH}',
                    'startup': 'disabled',
                }
            },
        }
        container.add_layer(self.ETCD_SERVICE, pebble_layer, combine=True)

        # If it happens that pebble-ready fires after the start event but there
        # are no events that are supposed happen immediately afterwards, we need
        # to reassess clustering again. This may happen if a pod of a StatefulSet
        # is removed by hand and is then automatically recreated.
        self._process_clustering()

    def is_service_configured(self):
        """Return whether the etcd service has been configured or not.

        It may happen that a pod gets recreated and handling certain events should not be
        done until pebble is ready. This method allows checking this dynamically rather than
        relying on stored state.
        """
        is_configured = True
        if self._container.can_connect():
            try:
                self._container.get_service(self.ETCD_SERVICE)
            except ModelError:
                is_configured = False
        else:
            is_configured = False
        return is_configured

    def _on_config_changed(self, event):
        self._process_clustering()

    def _on_cluster_changed(self, event):
        self._process_clustering()

    def _on_start(self, event):
        # If a pod gets recreated, the storage used for rending a config file will be
        # gone so we need to re-render the config to change listen URLs.
        if self.is_service_configured():
            self._apply_config_change()
        self._process_clustering()

    def _etcd_needs_start(self):
        return not self._container.get_service(self.ETCD_SERVICE).is_running()

    def _process_clustering(self):
        if not self.is_service_configured():
            return
        self.configure_clustering()
        if self._etcd_needs_start():
            self._maybe_restart_etcd()

    def configure_clustering(self):
        self.cluster.expose_peer_url()
        if self.unit.is_leader():
            self.cluster.update_peer_urls()
            self.cluster.maybe_expose_cluster_token()

    def _maybe_restart_etcd(self):
        if self.cluster.is_etcd_ready_to_start():
            self._restart_etcd()
            self.unit.status = ActiveStatus()
        else:
            self.unit.status = WaitingStatus("Waiting for enough units of the cluster to come up")

    def _restart_etcd(self):
        self._apply_config_change()
        logger.info('Restarting etcd')
        if self._container.get_service(self.ETCD_SERVICE).is_running():
            # Only restart the service if it is actually running since restarting a service that
            # has never been started results in a Pebble error.
            self._container.restart(self.ETCD_SERVICE)
        else:
            self._container.start(self.ETCD_SERVICE)


if __name__ == "__main__":
    main(EtcdOperatorCharm, use_juju_for_storage=True)
