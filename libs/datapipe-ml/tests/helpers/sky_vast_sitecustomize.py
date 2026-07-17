from __future__ import annotations

import os


def _patch_sky_vast_launch() -> None:
    try:
        from sky.provision.vast import utils as vast_utils
    except Exception:
        return
    if getattr(vast_utils, "_datapipe_live_vast_patch", False):
        return

    original_launch = vast_utils.launch

    def patched_launch(
        name,
        instance_type,
        region,
        disk_size,
        image_name,
        ports,
        preemptible,
        secure_only,
        private_docker_registry=None,
        login=None,
        create_instance_kwargs=None,
        ssh_public_key=None,
    ):
        country = str(region or "").strip().split(",")[-1].strip().upper()

        original_vast_factory = vast_utils.vast.vast

        class LiveVastClient:
            def __init__(self, client):
                self._client = client
                api_key = (
                    getattr(client, "api_key_access", None)
                    or getattr(client, "api_key", None)
                    or getattr(getattr(client, "client", None), "api_key", None)
                )
                if api_key is not None:
                    self.api_key_access = api_key
                    self.client = type("VastClientShim", (), {"api_key": api_key})()

            def __getattr__(self, name):
                return getattr(self._client, name)

            def search_offers(self, query=None, *args, **kwargs):
                cpu_ram = float(instance_type.split("-")[-1]) / 1024
                gpu_name = instance_type.split("-")[1].replace("_", " ")
                num_gpus = int(instance_type.split("-")[0].replace("x", ""))
                live_query = [
                    "verified=true",
                    "rentable=true",
                    "rented=false",
                    f"disk_space>={disk_size}",
                    f"num_gpus={num_gpus}",
                    f'gpu_name="{gpu_name}"',
                    f'cpu_ram>="{cpu_ram}"',
                ]
                if secure_only or os.getenv("DATAPIPE_ML_SKY_VAST_SECURE_ONLY", "1") == "1":
                    live_query.append("datacenter=true")
                kwargs.setdefault("order", "dph_total+")
                offers = self._client.search_offers(query=" ".join(live_query), *args, **kwargs)
                if isinstance(offers, int) or not country:
                    return offers
                return [
                    offer
                    for offer in offers
                    if str(offer.get("geolocation") or "").strip().upper().endswith(f", {country}")
                ]

        def live_vast_factory():
            return LiveVastClient(original_vast_factory())

        vast_utils.vast.vast = live_vast_factory
        try:
            return original_launch(
                name=name,
                instance_type=instance_type,
                region=region,
                disk_size=disk_size,
                image_name=image_name,
                ports=ports,
                preemptible=preemptible,
                secure_only=secure_only,
                private_docker_registry=private_docker_registry,
                login=login,
                create_instance_kwargs=create_instance_kwargs,
                ssh_public_key=ssh_public_key,
            )
        finally:
            vast_utils.vast.vast = original_vast_factory

    vast_utils.launch = patched_launch
    vast_utils._datapipe_live_vast_patch = True


_patch_sky_vast_launch()
