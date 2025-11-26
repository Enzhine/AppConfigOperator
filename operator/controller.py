#!/usr/bin/env python3
import os
import time
import json
import logging
import datetime
from kubernetes import client, config, watch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s")

GROUP, VERSION, PLURAL = "ac.io", "v1", "appconfigs"


def owner_ref(ca):
    api_version = f"{GROUP}/{VERSION}"
    name = ca["metadata"]["name"]
    uid = ca["metadata"]["uid"]

    return [{
        "apiVersion": api_version,
        "kind": "AppConfig",
        "name": name,
        "uid": uid,
        "controller": True,
        "blockOwnerDeletion": True
    }]


def desired_cm(ca):
    name = ca['spec']['appName']
    namespace = ca['spec']['namespace']
    data = ca['spec']['data']

    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{name}-config",
            "namespace": namespace,
            "ownerReferences": owner_ref(ca)
        },
        "data": data
    }


def main():
    config.load_incluster_config()
    crd_api = client.CustomObjectsApi()
    core_api = client.CoreV1Api()
    w = watch.Watch()

    logging.info("Watching...")
    for event in w.stream(crd_api.list_cluster_custom_object,
                          GROUP, VERSION, PLURAL):
        obj = event["object"]
        name = obj['metadata']['name']
        namespace = obj['metadata']['namespace']
        event_type = event["type"]

        match event_type:
            case "DELETED":
                logging.info("configMap deleted: %s in %s", name, namespace)
                continue
            case "ADDED" | "UPDATED":
                cm_manifest = desired_cm(obj)
                try:
                    core_api.create_namespaced_config_map(body=cm_manifest)
                    core_api.patch_namespaced_custom_object(
                        group="ac.io",
                        version="v1",
                        namespace=namespace,
                        plural=PLURAL,
                        name=name,
                        body={
                            "status": {
                                "lastUpdated": datetime.now(datetime.timezone.utc).isoformat()
                            }
                        }
                    )
                    logging.info("Created configMap: %s", json.dumps(cm_manifest))
                except client.rest.ApiException as e:
                    if e.status == 409:
                        logging.info("Pod %s-pod already exists", name)
                    else:
                        logging.exception("Unexpected error")


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception:
            logging.exception("Controller crashed, restarting in 5s")
            time.sleep(5)
