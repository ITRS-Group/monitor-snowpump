#!/usr/bin/env python3

import sys

import toml

from tqdm import tqdm

from snowpump import monitor, servicenow


BANNER = "\n".join([
    "\n"
    "##########################################",
    "## ServiceNow CMDB pump for OP5 Monitor ##",
    "##########################################",
    "\n"
])


class Summary:
    total = 0
    created = 0
    skipped = 0

    @property
    def __str__(self):
        return f"Done. Objects total: {self.total}, created: {self.created}, skipped: {self.skipped}\n"


def write_log(message, marker="> ", pad_top=0, pad_bottom=1):
    pad_top = "\n" * pad_top
    pad_bottom = "\n" * pad_bottom
    sys.stdout.write(f"{pad_top}{marker}{message}{pad_bottom}")


def main():
    sys.stdout.write(BANNER)

    write_log("Getting ready...", marker="")

    config = toml.load("config.toml")
    mappings = config["mappings"]

    mon = monitor.init(config["monitor"])
    snow = servicenow.init(config["servicenow"])
    summary = Summary()

    for mapping_type, mapping in mappings.items():
        hostgroup = mapping["hostgroup"]
        table = mapping["table"]

        mon_hosts = mon.get_many(hostgroup)
        count = snow.get_count(table)

        write_log(f"Processing {hostgroup}", pad_top=1)
        bar = tqdm(total=count, ncols=100)

        for host in snow.get_many(table, stream=True):
            name = host["host_name"]
            bar.update()
            bar.set_description(f"Processing {name}")
            summary.total += 1

            if name in [h["host_name"] for h in mon_hosts]:
                summary.skipped += 1
                continue

            host["hostgroups"].append(mapping["hostgroup"])
            mon.create(host)

            summary.created += 1

        bar.set_description()
        bar.close()

    write_log(summary.__str__, pad_top=1, marker="")


if __name__ == "__main__":
    main()
